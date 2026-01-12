"""
Snowflake sink — consumes CDC and marketing events from Kafka
and loads them into Snowflake raw tables for dbt transformation.
"""

import json
from datetime import datetime, timezone

import structlog
import snowflake.connector
from confluent_kafka import Consumer

from config.settings import settings

logger = structlog.get_logger()


def get_snowflake_connection():
    sf = settings.snowflake
    return snowflake.connector.connect(
        account=sf.account,
        user=sf.user,
        password=sf.password,
        database=sf.database,
        schema=sf.schema_name,
        warehouse=sf.warehouse,
    )


TOPIC_TABLE_MAP = {
    "crm.public.customers": "raw_crm_customers",
    "crm.public.contacts": "raw_crm_contacts",
    "crm.public.campaigns": "raw_crm_campaigns",
    "crm.public.campaign_responses": "raw_crm_campaign_responses",
    "ecommerce.ecommerce_source.orders": "raw_ecom_orders",
    "ecommerce.ecommerce_source.order_items": "raw_ecom_order_items",
    "ecommerce.ecommerce_source.products": "raw_ecom_products",
    "ecommerce.ecommerce_source.page_views": "raw_ecom_page_views",
    "marketing.ga_sessions": "raw_ga_sessions",
    "marketing.google_ads": "raw_google_ads",
    "marketing.fb_ads": "raw_fb_ads",
}


def setup_raw_tables(conn):
    """Create raw landing tables in Snowflake."""
    cur = conn.cursor()
    for table in TOPIC_TABLE_MAP.values():
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                record_key VARCHAR,
                record_value VARIANT,
                kafka_topic VARCHAR,
                kafka_partition INT,
                kafka_offset BIGINT,
                loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
    conn.commit()
    cur.close()
    logger.info("raw_tables_created", tables=list(TOPIC_TABLE_MAP.values()))


def run_snowflake_sink(batch_size: int = 100):
    """Consume from all topics and load into Snowflake."""
    topics = list(TOPIC_TABLE_MAP.keys())

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "group.id": "snowflake-sink",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(topics)

    conn = get_snowflake_connection()
    setup_raw_tables(conn)

    batch: dict[str, list] = {table: [] for table in TOPIC_TABLE_MAP.values()}
    total = 0

    logger.info("snowflake_sink_started", topics=topics)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Flush any pending batches
                _flush_all(conn, batch)
                consumer.commit()
                continue

            if msg.error():
                logger.error("consumer_error", error=msg.error())
                continue

            topic = msg.topic()
            table = TOPIC_TABLE_MAP.get(topic)
            if not table:
                continue

            key = msg.key().decode("utf-8") if msg.key() else ""
            value = msg.value()
            if isinstance(value, bytes):
                value = value.decode("utf-8")

            batch[table].append(
                (
                    key,
                    value,
                    topic,
                    msg.partition(),
                    msg.offset(),
                )
            )
            total += 1

            # Flush when batch is full
            if sum(len(b) for b in batch.values()) >= batch_size:
                _flush_all(conn, batch)
                consumer.commit()

                if total % 1000 == 0:
                    logger.info("sink_progress", total=total)

    except KeyboardInterrupt:
        _flush_all(conn, batch)
        consumer.commit()
        logger.info("snowflake_sink_stopped", total=total)
    finally:
        consumer.close()
        conn.close()


def _flush_all(conn, batch: dict[str, list]):
    """Flush all batches to Snowflake."""
    cur = conn.cursor()
    for table, rows in batch.items():
        if not rows:
            continue

        cur.executemany(
            f"""
            INSERT INTO {table} (record_key, record_value, kafka_topic, kafka_partition, kafka_offset)
            VALUES (%s, PARSE_JSON(%s), %s, %s, %s)
            """,
            rows,
        )
        rows.clear()

    conn.commit()
    cur.close()


if __name__ == "__main__":
    run_snowflake_sink()
