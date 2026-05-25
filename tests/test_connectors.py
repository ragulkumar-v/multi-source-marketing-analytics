"""Tests for Debezium connector configuration and marketing logic."""

import pytest

# Inline connector configs for testing (mirrors register_connectors.py)
POSTGRES_CONNECTOR = {
    "name": "crm-postgres-source",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "table.include.list": "public.customers,public.contacts,public.campaigns,public.campaign_responses",
        "plugin.name": "pgoutput",
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    },
}

MYSQL_CONNECTOR = {
    "name": "ecommerce-mysql-source",
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "table.include.list": "ecommerce_source.orders,ecommerce_source.order_items,ecommerce_source.products,ecommerce_source.page_views",
        "schema.history.internal.kafka.topic": "schema-changes.ecommerce",
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    },
}


class TestPostgresConnector:
    def test_connector_name(self):
        assert POSTGRES_CONNECTOR["name"] == "crm-postgres-source"

    def test_connector_class(self):
        cfg = POSTGRES_CONNECTOR["config"]
        assert cfg["connector.class"] == "io.debezium.connector.postgresql.PostgresConnector"

    def test_avro_converter(self):
        cfg = POSTGRES_CONNECTOR["config"]
        assert cfg["key.converter"] == "io.confluent.connect.avro.AvroConverter"
        assert cfg["value.converter"] == "io.confluent.connect.avro.AvroConverter"

    def test_unwrap_transform(self):
        cfg = POSTGRES_CONNECTOR["config"]
        assert cfg["transforms"] == "unwrap"
        assert cfg["transforms.unwrap.type"] == "io.debezium.transforms.ExtractNewRecordState"

    def test_tables_included(self):
        cfg = POSTGRES_CONNECTOR["config"]
        tables = cfg["table.include.list"]
        assert "public.customers" in tables
        assert "public.campaigns" in tables
        assert "public.campaign_responses" in tables

    def test_logical_replication(self):
        cfg = POSTGRES_CONNECTOR["config"]
        assert cfg["plugin.name"] == "pgoutput"


class TestMySQLConnector:
    def test_connector_name(self):
        assert MYSQL_CONNECTOR["name"] == "ecommerce-mysql-source"

    def test_connector_class(self):
        cfg = MYSQL_CONNECTOR["config"]
        assert cfg["connector.class"] == "io.debezium.connector.mysql.MySqlConnector"

    def test_tables_included(self):
        cfg = MYSQL_CONNECTOR["config"]
        tables = cfg["table.include.list"]
        assert "orders" in tables
        assert "order_items" in tables
        assert "products" in tables

    def test_schema_history_topic(self):
        cfg = MYSQL_CONNECTOR["config"]
        assert "schema.history.internal.kafka.topic" in cfg


class TestCustomer360Logic:
    def test_value_tier_classification(self):
        def classify(revenue):
            if revenue >= 5000:
                return "platinum"
            elif revenue >= 1000:
                return "gold"
            elif revenue >= 250:
                return "silver"
            elif revenue > 0:
                return "bronze"
            return "prospect"

        assert classify(10000) == "platinum"
        assert classify(2500) == "gold"
        assert classify(500) == "silver"
        assert classify(50) == "bronze"
        assert classify(0) == "prospect"

    def test_health_score_bounds(self):
        def health_score(orders, revenue, campaigns, recent_order):
            raw = orders * 5 + min(revenue / 100, 30) + campaigns * 3 + (20 if recent_order else 0)
            return min(100, int(raw))

        assert health_score(0, 0, 0, False) == 0
        assert health_score(20, 10000, 10, True) == 100
        assert 0 <= health_score(5, 500, 2, True) <= 100

    def test_roas_calculation(self):
        cost, value = 1000, 5000
        roas = round(value / cost, 2) if cost > 0 else 0
        assert roas == 5.0

    def test_ctr_calculation(self):
        impressions, clicks = 10000, 250
        ctr = round(clicks / impressions * 100, 4) if impressions > 0 else 0
        assert ctr == 2.5
