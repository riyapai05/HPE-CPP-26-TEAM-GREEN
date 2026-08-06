# shipment_service.py

import uuid
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
import os

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import (
    create_engine, Column, String, Float, DateTime,
    Enum as SAEnum, ForeignKey, text
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)


# Database Setup (PostgreSQL)
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://postgres:password@localhost:5432/tempsafe"
)

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    """FastAPI dependency — yields a DB session, closes it after the request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Enums — 3-state lifecycle
# CREATED → IN_TRANSIT → DELIVERED
class ShipmentStatus(str, Enum):
    CREATED    = "CREATED"
    IN_TRANSIT = "IN_TRANSIT"
    DELIVERED  = "DELIVERED"


# Enforces forward-only transitions — no skipping, no reversing.
VALID_TRANSITIONS = {
    ShipmentStatus.CREATED:    [ShipmentStatus.IN_TRANSIT],
    ShipmentStatus.IN_TRANSIT: [ShipmentStatus.DELIVERED],
    ShipmentStatus.DELIVERED:  [],  # Terminal state
}


# SQLAlchemy ORM Models

class ShipmentModel(Base):
    __tablename__ = "shipments"

    shipment_id    = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    origin         = Column(String, nullable=False)
    destination    = Column(String, nullable=False)
    status         = Column(SAEnum(ShipmentStatus), nullable=False, default=ShipmentStatus.CREATED)
    min_temp_limit = Column(Float, nullable=False)   # e.g. 2.0°C
    max_temp_limit = Column(Float, nullable=False)   # e.g. 8.0°C
    created_at     = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    sensors = relationship("SensorModel", back_populates="shipment")


class SensorModel(Base):
    __tablename__ = "sensors"

    sensor_id        = Column(String, primary_key=True)
    shipment_id      = Column(String, ForeignKey("shipments.shipment_id"), nullable=False)
    calibration_date = Column(DateTime(timezone=True), nullable=True)
    registered_at    = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    shipment = relationship("ShipmentModel", back_populates="sensors")


# Create tables if they don't exist
Base.metadata.create_all(bind=engine)

# Pydantic Schemas


# --- Shipment Schemas ---
class ShipmentCreate(BaseModel):
    """Body for POST /shipments — used by Logistics Manager."""
    origin:         str   = Field(..., example="Mangaluru Warehouse")
    destination:    str   = Field(..., example="Bengaluru Hub")
    min_temp_limit: float = Field(..., example=2.0, description="Min safe temp in °C")
    max_temp_limit: float = Field(..., example=8.0, description="Max safe temp in °C")

    @field_validator("max_temp_limit")
    @classmethod
    def max_must_exceed_min(cls, v, info):
        if "min_temp_limit" in info.data and v <= info.data["min_temp_limit"]:
            raise ValueError("max_temp_limit must be greater than min_temp_limit")
        return v


class ShipmentStatusUpdate(BaseModel):
    # Body for PATCH /shipments/{id}/status — used by Warehouse Receiver.
    new_status: ShipmentStatus


class ShipmentResponse(BaseModel):
    """What the API returns when a client reads a shipment."""
    shipment_id:    str
    origin:         str
    destination:    str
    status:         ShipmentStatus
    min_temp_limit: float
    max_temp_limit: float
    created_at:     datetime

    model_config = {"from_attributes": True}


# --- Sensor Schemas ---
class SensorRegister(BaseModel):
    """Body for POST /shipments/{id}/sensors — Admin registers a physical device."""
    sensor_id:        str               = Field(..., example="SEN_001")
    calibration_date: Optional[datetime] = Field(None, example="2026-04-01T00:00:00Z")


class SensorResponse(BaseModel):
    sensor_id:        str
    shipment_id:      str
    calibration_date: Optional[datetime]
    registered_at:    datetime

    model_config = {"from_attributes": True}


# --- Simulator Contract Schema ---
class ActiveSensorInfo(BaseModel):
    # What the simulator receives when it calls GET /sensors/active.
    sensor_id:      str
    shipment_id:    str
    min_temp_limit: float
    max_temp_limit: float
    origin:         str
    destination:    str


# FastAPI App
app = FastAPI(
    title="TempSafe — Shipment Microservice",
    description=(
        "Manages shipment lifecycle, IoT sensor registration, and temperature "
        "compliance rules. Part of the Cloud Native Temperature Excursion & "
        "Compliance Platform."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Tighten this when connecting the React dashboard
    allow_methods=["*"],
    allow_headers=["*"],
)


# Health Check
@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "ok", "service": "shipment-microservice"}


# Shipment Endpoints — Logistics Manager

@app.post(
    "/shipments",
    response_model=ShipmentResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Shipments"],
    summary="Register a new shipment (Logistics Manager)"
)
def create_shipment(payload: ShipmentCreate, db: Session = Depends(get_db)):
    """
    Logistics Manager creates a shipment and sets temperature compliance rules.
    """
    shipment = ShipmentModel(
        shipment_id=str(uuid.uuid4()),
        origin=payload.origin,
        destination=payload.destination,
        min_temp_limit=payload.min_temp_limit,
        max_temp_limit=payload.max_temp_limit,
        status=ShipmentStatus.CREATED,
    )
    db.add(shipment)
    db.commit()
    db.refresh(shipment)
    logger.info(f"New shipment registered: {shipment.shipment_id} | {shipment.origin} → {shipment.destination}")
    return shipment


@app.get(
    "/shipments",
    response_model=list[ShipmentResponse],
    tags=["Shipments"],
    summary="List all shipments"
)
def list_shipments(
    status_filter: Optional[ShipmentStatus] = None,
    db: Session = Depends(get_db)
):
    """Returns all shipments. Optionally filter by status, e.g. ?status_filter=IN_TRANSIT."""
    query = db.query(ShipmentModel)
    if status_filter:
        query = query.filter(ShipmentModel.status == status_filter)
    return query.all()


@app.get(
    "/shipments/{shipment_id}",
    response_model=ShipmentResponse,
    tags=["Shipments"],
    summary="Get a single shipment by ID"
)
def get_shipment(shipment_id: str, db: Session = Depends(get_db)):
    shipment = db.query(ShipmentModel).filter(ShipmentModel.shipment_id == shipment_id).first()
    if not shipment:
        raise HTTPException(status_code=404, detail=f"Shipment {shipment_id} not found")
    return shipment


@app.patch(
    "/shipments/{shipment_id}/status",
    response_model=ShipmentResponse,
    tags=["Shipments"],
    summary="Advance shipment status (Warehouse Receiver)"
)
def update_shipment_status(
    shipment_id: str,
    payload: ShipmentStatusUpdate,
    db: Session = Depends(get_db)
):
    """
    Warehouse Receiver constraint: can only touch the STATUS of the trip.
    Backwards transitions and invalid jumps are rejected with a 400.
    """
    shipment = db.query(ShipmentModel).filter(ShipmentModel.shipment_id == shipment_id).first()
    if not shipment:
        raise HTTPException(status_code=404, detail=f"Shipment {shipment_id} not found")

    allowed_next_states = VALID_TRANSITIONS.get(shipment.status, [])
    if payload.new_status not in allowed_next_states:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid transition: {shipment.status} → {payload.new_status}. "
                f"Allowed next states: {[s.value for s in allowed_next_states]}"
            )
        )

    old_status = shipment.status
    shipment.status = payload.new_status
    db.commit()
    db.refresh(shipment)
    logger.info(f"Shipment {shipment_id} status: {old_status} → {payload.new_status}")
    return shipment


# Sensor Endpoints — Admin

@app.post(
    "/shipments/{shipment_id}/sensors",
    response_model=SensorResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Sensors"],
    summary="Register an IoT sensor to a shipment (Admin)"
)
def register_sensor(
    shipment_id: str,
    payload: SensorRegister,
    db: Session = Depends(get_db)
):
    """
    Admin binds a physical sensor_id to a logical shipment_id.
    Returns 409 if sensor is already registered to any shipment.
    """
    shipment = db.query(ShipmentModel).filter(ShipmentModel.shipment_id == shipment_id).first()
    if not shipment:
        raise HTTPException(status_code=404, detail=f"Shipment {shipment_id} not found")

    existing = db.query(SensorModel).filter(SensorModel.sensor_id == payload.sensor_id).first()
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"Sensor {payload.sensor_id} is already registered to shipment {existing.shipment_id}"
        )

    sensor = SensorModel(
        sensor_id=payload.sensor_id,
        shipment_id=shipment_id,
        calibration_date=payload.calibration_date,
    )
    db.add(sensor)
    db.commit()
    db.refresh(sensor)
    logger.info(f"Sensor {sensor.sensor_id} registered to shipment {shipment_id}")
    return sensor


@app.get(
    "/shipments/{shipment_id}/sensors",
    response_model=list[SensorResponse],
    tags=["Sensors"],
    summary="List all sensors for a shipment"
)
def list_sensors_for_shipment(shipment_id: str, db: Session = Depends(get_db)):
    shipment = db.query(ShipmentModel).filter(ShipmentModel.shipment_id == shipment_id).first()
    if not shipment:
        raise HTTPException(status_code=404, detail=f"Shipment {shipment_id} not found")
    return shipment.sensors


# Simulator Contract Endpoint

@app.get(
    "/sensors/active",
    response_model=list[ActiveSensorInfo],
    tags=["Simulator Contract"],
    summary="Get all active sensors for IN_TRANSIT shipments (Simulator)"
)
def get_active_sensors(db: Session = Depends(get_db)):
    """
    Only returns sensors whose shipment is currently IN_TRANSIT.
    """
    results = (
        db.query(SensorModel, ShipmentModel)
        .join(ShipmentModel, SensorModel.shipment_id == ShipmentModel.shipment_id)
        .filter(ShipmentModel.status == ShipmentStatus.IN_TRANSIT)
        .all()
    )

    if not results:
        logger.info("No IN_TRANSIT sensors found.")
        return []

    active = [
        ActiveSensorInfo(
            sensor_id=sensor.sensor_id,
            shipment_id=sensor.shipment_id,
            min_temp_limit=shipment.min_temp_limit,
            max_temp_limit=shipment.max_temp_limit,
            origin=shipment.origin,
            destination=shipment.destination,
        )
        for sensor, shipment in results
    ]
    logger.info(f"Returning {len(active)} active sensors to simulator.")
    return active