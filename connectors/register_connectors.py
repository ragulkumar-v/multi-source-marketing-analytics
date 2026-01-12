"""
Register Debezium CDC connectors with Kafka Connect.

Creates source connectors for:
- PostgreSQL (CRM): customers, contacts, campaigns, campaign_responses
- MySQL (E-Commerce): orders, order_items, products, page_views
"""

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_fixed

from config.settings import settings

logger = structlog.get_logger()

POSTGRES_CONNECTOR = {
    "name": "crm-postgres-source",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": settings.postgres.host,
        "database.port": str(settings.postgres.port),
        "database.user": settings.postgres.user,
        "database.password": settings.postgres.password,
        "database.dbname": settings.postgres.db,
        "database.server.name": "crm",
        "topic.prefix": "crm",
        "schema.include.list": "public",
        "table.include.list": "public.customers,public.contacts,public.campaigns,public.campaign_responses",
        "plugin.name": "pgoutput",
        "slot.name": "crm_debezium",
        "publication.name": "crm_publication",
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": settings.kafka.schema_registry_url,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": settings.kafka.schema_registry_url,
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "true",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "snapshot.mode": "initial",
        "heartbeat.interval.ms": "10000",
    },
}

MYSQL_CONNECTOR = {
    "name": "ecommerce-mysql-source",
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.hostname": settings.mysql.host,
        "database.port": str(settings.mysql.port),
        "database.user": settings.mysql.user,
        "database.password": settings.mysql.password,
        "database.server.id": "1001",
        "database.server.name": "ecommerce",
        "topic.prefix": "ecommerce",
        "database.include.list": settings.mysql.db,
        "table.include.list": f"{settings.mysql.db}.orders,{settings.mysql.db}.order_items,{settings.mysql.db}.products,{settings.mysql.db}.page_views",
        "schema.history.internal.kafka.bootstrap.servers": settings.kafka.bootstrap_servers,
        "schema.history.internal.kafka.topic": "schema-changes.ecommerce",
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": settings.kafka.schema_registry_url,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": settings.kafka.schema_registry_url,
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "true",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "snapshot.mode": "initial",
        "include.schema.changes": "true",
    },
}


@retry(stop=stop_after_attempt(5), wait=wait_fixed(5))
def register_connector(connector_config: dict):
    """Register a connector with Kafka Connect REST API."""
    name = connector_config["name"]
    url = f"{settings.connect_url}/connectors"

    # Check if already exists
    response = httpx.get(f"{url}/{name}", timeout=10)
    if response.status_code == 200:
        logger.info("connector_exists_updating", name=name)
        response = httpx.put(
            f"{url}/{name}/config",
            json=connector_config["config"],
            timeout=10,
        )
    else:
        logger.info("creating_connector", name=name)
        response = httpx.post(url, json=connector_config, timeout=10)

    response.raise_for_status()
    logger.info("connector_registered", name=name, status=response.status_code)


def check_connector_status(name: str) -> dict:
    """Check connector status."""
    response = httpx.get(f"{settings.connect_url}/connectors/{name}/status", timeout=10)
    response.raise_for_status()
    return response.json()


def list_connectors() -> list[str]:
    """List all registered connectors."""
    response = httpx.get(f"{settings.connect_url}/connectors", timeout=10)
    response.raise_for_status()
    return response.json()


def register_all():
    """Register all Debezium connectors."""
    logger.info("registering_all_connectors")

    for connector in [POSTGRES_CONNECTOR, MYSQL_CONNECTOR]:
        try:
            register_connector(connector)
        except Exception as e:
            logger.error(
                "connector_registration_failed",
                name=connector["name"],
                error=str(e),
            )

    # Verify status
    for name in ["crm-postgres-source", "ecommerce-mysql-source"]:
        try:
            status = check_connector_status(name)
            logger.info(
                "connector_status",
                name=name,
                state=status["connector"]["state"],
            )
        except Exception:
            logger.warning("status_check_failed", name=name)


if __name__ == "__main__":
    register_all()
