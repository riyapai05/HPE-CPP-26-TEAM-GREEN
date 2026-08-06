import logging
import os
import re
import time
from datetime import datetime
from typing import Optional

import asyncpg
from fastapi import FastAPI, Query, HTTPException

# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)

logger = logging.getLogger(__name__)

# --------------------------------------------------
# Database Configuration
# --------------------------------------------------

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:pass123@localhost:5432/tempsafe"
)

# --------------------------------------------------
# FastAPI App
# --------------------------------------------------

app = FastAPI(
    title="Temperature Reporting Service",
    description="Generates temperature analytics and compliance reports.",
    version="1.0.0"
)

# --------------------------------------------------
# Database Pool
# --------------------------------------------------

db_pool = None


@app.on_event("startup")
async def startup():

    global db_pool

    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=10
    )

    logger.info("Reporting service connected to database.")


@app.on_event("shutdown")
async def shutdown():

    await db_pool.close()

    logger.info("Reporting service shutdown complete.")


# --------------------------------------------------
# Input Sanitization — SQL Injection Protection
# --------------------------------------------------

def sanitize_id(value: str) -> str:
    """
    Only allows alphanumeric characters, hyphens, and underscores.
    Rejects anything suspicious before it touches the query.
    """
    if value and not re.match(r'^[\w\-]+$', value):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid characters in parameter: '{value}'"
        )
    return value


# --------------------------------------------------
# 1. Temperature History Report
# --------------------------------------------------

@app.get("/reports/temperature-history")
async def get_temperature_history(
    shipment_id: Optional[str] = Query(None, description="Filter by shipment ID"),
    sensor_id:   Optional[str] = Query(None, description="Filter by sensor ID"),
    start_time:  Optional[str] = Query(None, description="Start time (YYYY-MM-DDTHH:MM:SS)"),
    end_time:    Optional[str] = Query(None, description="End time (YYYY-MM-DDTHH:MM:SS)"),
    page:        int           = Query(1,    ge=1,   description="Page number"),
    page_size:   int           = Query(50,   ge=1,   le=500, description="Records per page")
):

    start = time.time()

    # ----------------------------------------------
    # Sanitize Inputs
    # ----------------------------------------------

    if shipment_id:
        shipment_id = sanitize_id(shipment_id)

    if sensor_id:
        sensor_id = sanitize_id(sensor_id)

    # ----------------------------------------------
    # Base Query
    # ----------------------------------------------

    query = """
    SELECT
        event_id,
        sensor_id,
        shipment_id,
        temperature,
        recorded_at,
        latitude,
        longitude,
        is_excursion,
        is_buffered
    FROM telemetry_readings
    WHERE 1=1
    """

    params = []
    idx = 1

    # ----------------------------------------------
    # Dynamic Filters
    # ----------------------------------------------

    if shipment_id:
        query += f" AND shipment_id = ${idx}"
        params.append(shipment_id)
        idx += 1

    if sensor_id:
        query += f" AND sensor_id = ${idx}"
        params.append(sensor_id)
        idx += 1

    if start_time:
        query += f" AND recorded_at >= ${idx}"
        params.append(start_time)
        idx += 1

    if end_time:
        query += f" AND recorded_at <= ${idx}"
        params.append(end_time)
        idx += 1

    # ----------------------------------------------
    # Pagination
    # ----------------------------------------------

    query += " ORDER BY recorded_at DESC"

    offset = (page - 1) * page_size
    query += f" LIMIT ${idx} OFFSET ${idx + 1}"
    params.append(page_size)
    params.append(offset)

    # ----------------------------------------------
    # Execute
    # ----------------------------------------------

    async with db_pool.acquire() as conn:

        rows = await conn.fetch(query, *params)

    result = []

    for row in rows:

        result.append({
            "event_id":    str(row["event_id"]),
            "sensor_id":   row["sensor_id"],
            "shipment_id": row["shipment_id"],
            "temperature": row["temperature"],
            "recorded_at": str(row["recorded_at"]),
            "latitude":    row["latitude"],
            "longitude":   row["longitude"],
            "is_excursion": row["is_excursion"],
            "is_buffered":  row["is_buffered"]
        })

    # ----------------------------------------------
    # Structured Log
    # ----------------------------------------------

    duration = round((time.time() - start) * 1000, 2)

    logger.info(
        f"[temperature-history] "
        f"filters={{ shipment_id={shipment_id}, sensor_id={sensor_id}, "
        f"start={start_time}, end={end_time} }} "
        f"page={page} page_size={page_size} "
        f"records={len(result)} "
        f"duration={duration}ms"
    )

    return {
        "report_type":   "temperature_history",
        "generated_at":  str(datetime.utcnow()),
        "page":          page,
        "page_size":     page_size,
        "total_records": len(result),
        "data":          result
    }


# --------------------------------------------------
# 2. Excursion Report
# --------------------------------------------------

@app.get("/reports/excursions")
async def get_excursion_report(
    shipment_id: Optional[str] = Query(None, description="Filter by shipment ID"),
    sensor_id:   Optional[str] = Query(None, description="Filter by sensor ID"),
    page:        int           = Query(1,    ge=1,   description="Page number"),
    page_size:   int           = Query(50,   ge=1,   le=500, description="Records per page")
):

    start = time.time()

    # ----------------------------------------------
    # Sanitize Inputs
    # ----------------------------------------------

    if shipment_id:
        shipment_id = sanitize_id(shipment_id)

    if sensor_id:
        sensor_id = sanitize_id(sensor_id)

    # ----------------------------------------------
    # Base Query
    # ----------------------------------------------

    query = """
    SELECT
        alert_id,
        shipment_id,
        sensor_id,
        temperature,
        min_temp_limit,
        max_temp_limit,
        origin,
        destination,
        recorded_at,
        is_buffered
    FROM alerts
    WHERE 1=1
    """

    params = []
    idx = 1

    # ----------------------------------------------
    # Dynamic Filters
    # ----------------------------------------------

    if shipment_id:
        query += f" AND shipment_id = ${idx}"
        params.append(shipment_id)
        idx += 1

    if sensor_id:
        query += f" AND sensor_id = ${idx}"
        params.append(sensor_id)
        idx += 1

    # ----------------------------------------------
    # Pagination
    # ----------------------------------------------

    query += " ORDER BY recorded_at DESC"

    offset = (page - 1) * page_size
    query += f" LIMIT ${idx} OFFSET ${idx + 1}"
    params.append(page_size)
    params.append(offset)

    # ----------------------------------------------
    # Execute
    # ----------------------------------------------

    async with db_pool.acquire() as conn:

        rows = await conn.fetch(query, *params)

    result = []

    for row in rows:

        deviation = 0

        if row["temperature"] < row["min_temp_limit"]:

            deviation = row["min_temp_limit"] - row["temperature"]

        elif row["temperature"] > row["max_temp_limit"]:

            deviation = row["temperature"] - row["max_temp_limit"]

        # ------------------------------------------
        # Severity Calculation
        # ------------------------------------------

        if deviation < 5:
            severity = "MINOR"

        elif deviation < 10:
            severity = "MAJOR"

        else:
            severity = "CRITICAL"

        result.append({
            "alert_id":    str(row["alert_id"]),
            "shipment_id": row["shipment_id"],
            "sensor_id":   row["sensor_id"],
            "temperature": row["temperature"],
            "allowed_range": {
                "min": row["min_temp_limit"],
                "max": row["max_temp_limit"]
            },
            "deviation":   round(deviation, 2),
            "severity":    severity,
            "origin":      row["origin"],
            "destination": row["destination"],
            "recorded_at": str(row["recorded_at"]),
            "is_buffered": row["is_buffered"]
        })

    # ----------------------------------------------
    # Structured Log
    # ----------------------------------------------

    duration = round((time.time() - start) * 1000, 2)

    logger.info(
        f"[excursion-report] "
        f"filters={{ shipment_id={shipment_id}, sensor_id={sensor_id} }} "
        f"page={page} page_size={page_size} "
        f"records={len(result)} "
        f"duration={duration}ms"
    )

    return {
        "report_type":      "excursion_report",
        "generated_at":     str(datetime.utcnow()),
        "page":             page,
        "page_size":        page_size,
        "total_excursions": len(result),
        "data":             result
    }


# --------------------------------------------------
# 3. Compliance Summary Report
# --------------------------------------------------

@app.get("/reports/compliance-summary")
async def get_compliance_summary():

    start = time.time()

    async with db_pool.acquire() as conn:

        total_readings = await conn.fetchval("""
            SELECT COUNT(*)
            FROM telemetry_readings
        """)

        total_excursions = await conn.fetchval("""
            SELECT COUNT(*)
            FROM telemetry_readings
            WHERE is_excursion = TRUE
        """)

        buffered_events = await conn.fetchval("""
            SELECT COUNT(*)
            FROM telemetry_readings
            WHERE is_buffered = TRUE
        """)

        active_shipments = await conn.fetchval("""
            SELECT COUNT(DISTINCT shipment_id)
            FROM telemetry_readings
        """)

    # ----------------------------------------------
    # Compliance Percentage
    # ----------------------------------------------

    if total_readings == 0:

        compliance_percentage = 100

    else:

        safe_readings = total_readings - total_excursions

        compliance_percentage = (
            safe_readings / total_readings
        ) * 100

    # ----------------------------------------------
    # Structured Log
    # ----------------------------------------------

    duration = round((time.time() - start) * 1000, 2)

    logger.info(
        f"[compliance-summary] "
        f"total_readings={total_readings} "
        f"total_excursions={total_excursions} "
        f"compliance={round(compliance_percentage, 2)}% "
        f"duration={duration}ms"
    )

    return {
        "report_type":  "compliance_summary",
        "generated_at": str(datetime.utcnow()),

        "statistics": {

            "total_readings":   total_readings,

            "total_excursions": total_excursions,

            "buffered_events":  buffered_events,

            "active_shipments": active_shipments,

            "compliance_percentage":
                round(compliance_percentage, 2)
        }
    }