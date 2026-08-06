import asyncio
import json
import logging
import os
from datetime import datetime

import asyncpg
import httpx
from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL     = os.getenv("DATABASE_URL",     "postgresql://postgres:password@localhost:5432/tempsafe")
KAFKA_BROKER     = os.getenv("KAFKA_BROKER",     "localhost:9092")
SHIPMENT_API_URL = os.getenv("SHIPMENT_API_URL", "http://localhost:8000")

TELEMETRY_TOPIC = "telemetry-events"
CONSUMER_GROUP  = "telemetry-db-writer-group"

CE_TYPE_ALERT_BREACH = "com.logistics.telemetry.alert.temperature_breach"
CE_TYPE_BUFFERED     = "com.logistics.telemetry.reading.buffered"

_sensor_cache: dict[str, dict] = {}


async def refresh_cache():
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{SHIPMENT_API_URL}/sensors/active", timeout=5.0)
            resp.raise_for_status()
            _sensor_cache.clear()
            for s in resp.json():
                _sensor_cache[s["sensor_id"]] = s
            logger.info(f"Sensor cache refreshed — {len(_sensor_cache)} active sensors.")
        except Exception as exc:
            logger.error(f"Cache refresh failed: {exc}")


async def setup_db(pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS telemetry_readings (
                event_id     UUID         NOT NULL,
                sensor_id    VARCHAR(50)  NOT NULL,
                shipment_id  VARCHAR(100) NOT NULL,
                event_type   VARCHAR(100) NOT NULL,
                recorded_at  TIMESTAMPTZ  NOT NULL,
                temperature  FLOAT        NOT NULL,
                latitude     FLOAT,
                longitude    FLOAT,
                is_excursion BOOLEAN      NOT NULL DEFAULT FALSE,
                is_buffered  BOOLEAN      NOT NULL DEFAULT FALSE,
                ingested_at  TIMESTAMPTZ  DEFAULT NOW(),
                PRIMARY KEY (event_id, ingested_at)
            );
        """)
        await conn.execute("""
            SELECT create_hypertable('telemetry_readings', 'ingested_at', if_not_exists => TRUE);
        """)
    logger.info("DB schema ready.")


async def process_batch(batch, pool: asyncpg.Pool, consumer: AIOKafkaConsumer):
    records = []

    for msg in batch:
        try:
            ce        = json.loads(msg.value.decode("utf-8"))
            sensor_id = ce["source"].removeprefix("urn:sensor:")
            data      = ce["data"]
            temp      = data["temperature"]
            gps       = data.get("gps", {})
        except Exception as exc:
            logger.error(f"Bad message offset {msg.offset}: {exc}")
            continue

        info = _sensor_cache.get(sensor_id)
        if info is None:
            await refresh_cache()
            info = _sensor_cache.get(sensor_id)
        if info is None:
            logger.warning(f"Unknown sensor {sensor_id} — skipping.")
            continue

        is_excursion = (
            ce["type"] == CE_TYPE_ALERT_BREACH
            or temp < info["min_temp_limit"]
            or temp > info["max_temp_limit"]
        )
        is_buffered = ce["type"] == CE_TYPE_BUFFERED

        records.append((
            ce["id"], sensor_id, info["shipment_id"], ce["type"],
            datetime.fromisoformat(ce["time"]),
            temp, gps.get("latitude"), gps.get("longitude"),
            is_excursion, is_buffered,
        ))

    if not records:
        return

    try:
        async with pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO telemetry_readings
                    (event_id, sensor_id, shipment_id, event_type,
                     recorded_at, temperature, latitude, longitude,
                     is_excursion, is_buffered)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (event_id, ingested_at) DO NOTHING;
            """, records)
        await consumer.commit()
        logger.info(f"✅ Stored {len(records)} records.")
    except Exception as exc:
        logger.error(f"DB write failed — offset not committed, Kafka will re-deliver. {exc}")


async def main():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    await setup_db(pool)
    await refresh_cache()

    async def cache_loop():
        while True:
            await asyncio.sleep(60)
            await refresh_cache()

    asyncio.create_task(cache_loop())

    consumer = AIOKafkaConsumer(
        TELEMETRY_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info(f"Listening on '{TELEMETRY_TOPIC}'...")

    try:
        while True:
            raw = await consumer.getmany(timeout_ms=1000, max_records=500)
            for _tp, messages in raw.items():
                if messages:
                    await process_batch(messages, pool, consumer)
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()
        await pool.close()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass