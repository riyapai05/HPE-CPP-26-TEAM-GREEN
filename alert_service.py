# alert_service.py — TempSafe Alert Microservice

import asyncio
import json
import logging
import os
from datetime import datetime

import asyncpg
from aiokafka import AIOKafkaConsumer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"#creates structured logs for alerts,failures
)

logger = logging.getLogger(__name__)



DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:password@postgres:5432/tempsafe"
)

KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "localhost:9092"
)

EXCURSION_TOPIC = "excursion-events"#continuously listens to excursion events topic to detect any temperature excursion

CONSUMER_GROUP = "alert-service-group"



async def setup_db(pool: asyncpg.Pool):

    async with pool.acquire() as conn:

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            alert_id UUID PRIMARY KEY,
            reading_event_id UUID NOT NULL,
            sensor_id VARCHAR(50) NOT NULL,
            shipment_id VARCHAR(100) NOT NULL,
            temperature FLOAT NOT NULL,
            min_temp_limit FLOAT NOT NULL,
            max_temp_limit FLOAT NOT NULL,
            origin VARCHAR(100),
            destination VARCHAR(100),
            recorded_at TIMESTAMPTZ NOT NULL,
            is_buffered BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """)

        logger.info("Alert database schema ready.")



async def process_messages(messages, pool, consumer):

    records = []

    for msg in messages:

        try:

           
            event = json.loads(
                msg.value.decode("utf-8")
            )

            data = event["data"]

            
            alert_record = (
                event["id"],
                data["reading_event_id"],
                data["sensor_id"],
                data["shipment_id"],
                data["temperature"],
                data["min_temp_limit"],
                data["max_temp_limit"],
                data["origin"],
                data["destination"],
                datetime.fromisoformat(
                    data["recorded_at"].replace("Z", "+00:00")
                ),
                data["is_buffered"]
            )

            records.append(alert_record)

           

            logger.warning(
                f"""
🚨 TEMPERATURE EXCURSION ALERT

Shipment ID : {data['shipment_id']}
Sensor ID   : {data['sensor_id']}

Route:
{data['origin']} → {data['destination']}

Temperature : {data['temperature']}°C

Allowed Range:
{data['min_temp_limit']}°C
to
{data['max_temp_limit']}°C

Buffered Reading : {data['is_buffered']}

Recorded At:
{data['recorded_at']}
"""
            )

        except Exception as exc:

            logger.error(
                f"Failed processing message "
                f"offset={msg.offset} "
                f"error={exc}"
            )



    if not records:
        return

   
    try:

        async with pool.acquire() as conn:

            await conn.executemany("""
            INSERT INTO alerts (
                alert_id,
                reading_event_id,
                sensor_id,
                shipment_id,
                temperature,
                min_temp_limit,
                max_temp_limit,
                origin,
                destination,
                recorded_at,
                is_buffered
            )
            VALUES (
                $1,$2,$3,$4,$5,
                $6,$7,$8,$9,$10,$11
            )
            ON CONFLICT (alert_id) DO NOTHING;
            """, records)

       

        await consumer.commit()

        logger.info(
            f"✅ Stored {len(records)} alerts successfully."
        )

    except Exception as exc:

        logger.error(
            f"Database write failed. "
            f"Kafka will re-deliver messages. "
            f"Error: {exc}"
        )

# --------------------------------------------------
# Main Service Runner
# --------------------------------------------------


async def main():

    # ----------------------------------------------
    # Create Database Connection Pool
    # ----------------------------------------------

    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=10
    )

    await setup_db(pool)

    # ----------------------------------------------
    # Kafka Consumer Setup
    # ----------------------------------------------

    consumer = AIOKafkaConsumer(
        EXCURSION_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest"
    )

    await consumer.start()

    logger.info(
        f"Listening for excursion alerts on "
        f"topic '{EXCURSION_TOPIC}'..."
    )

    try:

        while True:

            # --------------------------------------
            # Batch Message Consumption
            # --------------------------------------

            raw = await consumer.getmany(
                timeout_ms=1000,
                max_records=500
            )

            for _tp, messages in raw.items():

                if messages:

                    await process_messages(
                        messages,
                        pool,
                        consumer
                    )

    except asyncio.CancelledError:
        pass

    finally:

        await consumer.stop()

        await pool.close()

        logger.info(
            "Alert service shutdown complete."
        )

# --------------------------------------------------
# Application Entry Point
# --------------------------------------------------

if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        pass
