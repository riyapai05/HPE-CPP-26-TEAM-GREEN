import asyncio
import json
import random
import logging
import uuid
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer
import aiohttp
import os

# Configure professional logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "telemetry-events"

# --- CloudEvents Type Constants ---
CE_TYPE_READING       = "com.logistics.telemetry.reading"
CE_TYPE_ALERT_BREACH  = "com.logistics.telemetry.alert.temperature_breach"
CE_TYPE_BUFFERED      = "com.logistics.telemetry.reading.buffered"

SHIPMENT_API_URL = os.getenv("SHIPMENT_API_URL", "http://localhost:8000/sensors/active")

async def fetch_in_transit_sensors():
    """Calls the Shipment API to get dynamically active sensors."""
    logger.info(f"Fetching active 'in-transit' sensors from {SHIPMENT_API_URL}...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(SHIPMENT_API_URL) as response:
                if response.status == 200:
                    data = await response.json()
                    # Extract just the sensor_ids from the ActiveSensorInfo payloads
                    sensors = [item["sensor_id"] for item in data]
                    logger.info(f"Found {len(sensors)} active sensors: {sensors}")
                    return sensors
                else:
                    logger.error(f"Failed to fetch sensors. HTTP {response.status}")
                    return []
    except Exception as e:
        logger.error(f"Error connecting to Shipment API: {e}")
        return []

def build_cloud_event(event_type: str, sensor_id: str, data: dict) -> dict:
    """
    Constructs a CloudEvents v1.0 compliant envelope.
    Separating this into its own function keeps sensor_worker clean
    """
    return {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "source": f"urn:sensor:{sensor_id}",
        "type": event_type,
        "datacontenttype": "application/json",
        "time": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }

async def publish(producer: AIOKafkaProducer, event_type: str, sensor_id: str, data: dict):
    """
    Builds a CloudEvent and sends it to Kafka.
    Single responsibility: wrapping + encoding + sending in one call.
    """
    cloud_event = build_cloud_event(event_type, sensor_id, data)
    await producer.send_and_wait(KAFKA_TOPIC, json.dumps(cloud_event).encode("utf-8"))

async def sensor_worker(sensor_id: str, producer: AIOKafkaProducer):
    # The infinite worker loop running the Chaos Engine for a single sensor.
    logger.info(f"[{sensor_id}] Worker started.")

    current_temp = 4.0
    lat, lon = 12.9141, 74.8560
    # Buffer now stores (event_type, data) tuples to preserve
    # the original event type when flushing after reconnection
    local_buffer = []
    is_offline = False

    try:
        while True:
            # --- STEP 1: Decide the State ---
            state = "NORMAL"
            roll = random.random()
            if roll < 0.02:
                state = "FRIDGE_BROKE"
            elif roll < 0.07:
                state = "NO_SIGNAL"

            # --- STEP 2: Generate Physics ---
            if state == "FRIDGE_BROKE":
                current_temp += 2.0
                logger.warning(f"🚨 [{sensor_id}] FRIDGE BROKE! Temp spiking to {current_temp}°C")
            else:
                current_temp += random.uniform(-0.1, 0.1)

            lat += random.uniform(-0.0005, 0.0005)
            lon += random.uniform(-0.0005, 0.0005)

            # --- STEP 3: Build the Business Data ---
            sensor_data = {
                "temperature": round(current_temp, 2),
                "gps": {
                    "latitude": round(lat, 4),
                    "longitude": round(lon, 4)
                }
            }

            # --- STEP 4: Assign CloudEvents Type Based on State ---
            if state == "FRIDGE_BROKE":
                ce_type = CE_TYPE_ALERT_BREACH
            else:
                # Both NORMAL and NO_SIGNAL produce readings;
                # NO_SIGNAL ones get upgraded to CE_TYPE_BUFFERED at flush time
                ce_type = CE_TYPE_READING

            # --- STEP 5: Network Routing & Buffering ---
            if state == "NO_SIGNAL":
                if not is_offline:
                    logger.info(f"📡 [{sensor_id}] Entered dead zone. Buffering data locally.")
                is_offline = True
                # Store the raw data — type will become CE_TYPE_BUFFERED on flush
                local_buffer.append(sensor_data)

            else:
                if is_offline:
                    logger.info(f"🚀 [{sensor_id}] Signal restored! Flushing {len(local_buffer)} buffered messages to Kafka.")
                    for buffered_data in local_buffer:
                        # Buffered messages get CE_TYPE_BUFFERED so consumers
                        # know these arrived late and may be out of order
                        await publish(producer, CE_TYPE_BUFFERED, sensor_id, buffered_data)
                    local_buffer.clear()
                    is_offline = False

                # Send the current live event with its correctly assigned type
                await publish(producer, ce_type, sensor_id, sensor_data)

            # --- STEP 6: Wait ---
            await asyncio.sleep(10)

    except asyncio.CancelledError:
        logger.info(f"[{sensor_id}] Worker received shutdown signal.")
        if local_buffer:
            logger.info(f"[{sensor_id}] Emergency flush of {len(local_buffer)} messages before dying.")
            for buffered_data in local_buffer:
                await publish(producer, CE_TYPE_BUFFERED, sensor_id, buffered_data)
        raise

async def main():
    """The orchestrator that starts the service."""
    logger.info("Initializing Simulation Microservice (CloudEvents v1.0)...")
    logger.info(f"  Event types in use:")
    logger.info(f"    Normal  → {CE_TYPE_READING}")
    logger.info(f"    Alert   → {CE_TYPE_ALERT_BREACH}")
    logger.info(f"    Buffered→ {CE_TYPE_BUFFERED}")

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await producer.start()
    logger.info("Connected to Kafka Broker.")

    active_sensors = await fetch_in_transit_sensors()
    tasks = []

    for sensor_id in active_sensors:
        task = asyncio.create_task(sensor_worker(sensor_id, producer))
        tasks.append(task)

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Manual shutdown initiated...")
    finally:
        logger.info("Cancelling all background workers...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await producer.stop()
        logger.info("Service shut down successfully.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass