"""
Marketing data producers — fetches from Google Analytics, Google Ads,
and Facebook Ads APIs and publishes to Kafka topics with Avro serialization.
"""

import json
import time
from datetime import date, timedelta, datetime, timezone

import httpx
import structlog
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings

logger = structlog.get_logger()

# --- Avro Schemas ---

GA_SESSION_SCHEMA = """{
    "type": "record",
    "name": "GASession",
    "namespace": "marketing.ga",
    "fields": [
        {"name": "date", "type": "string"},
        {"name": "source", "type": "string"},
        {"name": "medium", "type": "string"},
        {"name": "campaign", "type": ["null", "string"], "default": null},
        {"name": "sessions", "type": "int"},
        {"name": "users", "type": "int"},
        {"name": "new_users", "type": "int"},
        {"name": "pageviews", "type": "int"},
        {"name": "bounce_rate", "type": "double"},
        {"name": "avg_session_duration", "type": "double"},
        {"name": "goal_completions", "type": "int"},
        {"name": "extracted_at", "type": "string"}
    ]
}"""

GOOGLE_ADS_SCHEMA = """{
    "type": "record",
    "name": "GoogleAdMetric",
    "namespace": "marketing.gads",
    "fields": [
        {"name": "date", "type": "string"},
        {"name": "campaign_id", "type": "string"},
        {"name": "campaign_name", "type": "string"},
        {"name": "ad_group_id", "type": "string"},
        {"name": "ad_group_name", "type": "string"},
        {"name": "impressions", "type": "long"},
        {"name": "clicks", "type": "long"},
        {"name": "cost_micros", "type": "long"},
        {"name": "conversions", "type": "double"},
        {"name": "conversion_value", "type": "double"},
        {"name": "extracted_at", "type": "string"}
    ]
}"""

FB_ADS_SCHEMA = """{
    "type": "record",
    "name": "FBAdsMetric",
    "namespace": "marketing.fb",
    "fields": [
        {"name": "date", "type": "string"},
        {"name": "campaign_id", "type": "string"},
        {"name": "campaign_name", "type": "string"},
        {"name": "adset_id", "type": "string"},
        {"name": "adset_name", "type": "string"},
        {"name": "impressions", "type": "long"},
        {"name": "clicks", "type": "long"},
        {"name": "spend", "type": "double"},
        {"name": "actions", "type": "long"},
        {"name": "action_values", "type": "double"},
        {"name": "reach", "type": "long"},
        {"name": "frequency", "type": "double"},
        {"name": "extracted_at", "type": "string"}
    ]
}"""


def create_avro_producer(schema_str: str, topic: str) -> SerializingProducer:
    """Create a Kafka producer with Avro serialization."""
    schema_registry = SchemaRegistryClient(
        {"url": settings.kafka.schema_registry_url}
    )

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry,
        schema_str=schema_str,
    )

    return SerializingProducer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "value.serializer": avro_serializer,
            "client.id": f"marketing-{topic}",
        }
    )


# --- Google Analytics ---

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
def fetch_ga_data(start_date: date, end_date: date) -> list[dict]:
    """Fetch session data from Google Analytics Data API."""
    # GA4 Data API endpoint
    url = f"https://analyticsdata.googleapis.com/v1beta/properties/{settings.ga_property_id}:runReport"

    payload = {
        "dateRanges": [
            {"startDate": start_date.isoformat(), "endDate": end_date.isoformat()}
        ],
        "dimensions": [
            {"name": "date"},
            {"name": "sessionSource"},
            {"name": "sessionMedium"},
            {"name": "sessionCampaignName"},
        ],
        "metrics": [
            {"name": "sessions"},
            {"name": "totalUsers"},
            {"name": "newUsers"},
            {"name": "screenPageViews"},
            {"name": "bounceRate"},
            {"name": "averageSessionDuration"},
            {"name": "conversions"},
        ],
    }

    # In production, use google-auth for OAuth
    response = httpx.post(url, json=payload, timeout=30)
    response.raise_for_status()

    data = response.json()
    records = []
    now = datetime.now(timezone.utc).isoformat()

    for row in data.get("rows", []):
        dims = row["dimensionValues"]
        mets = row["metricValues"]
        records.append(
            {
                "date": dims[0]["value"],
                "source": dims[1]["value"],
                "medium": dims[2]["value"],
                "campaign": dims[3]["value"] if dims[3]["value"] != "(not set)" else None,
                "sessions": int(mets[0]["value"]),
                "users": int(mets[1]["value"]),
                "new_users": int(mets[2]["value"]),
                "pageviews": int(mets[3]["value"]),
                "bounce_rate": float(mets[4]["value"]),
                "avg_session_duration": float(mets[5]["value"]),
                "goal_completions": int(mets[6]["value"]),
                "extracted_at": now,
            }
        )

    return records


# --- Google Ads ---

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
def fetch_google_ads_data(start_date: date, end_date: date) -> list[dict]:
    """Fetch campaign metrics from Google Ads API."""
    url = f"https://googleads.googleapis.com/v16/customers/{settings.google_ads.customer_id}/googleAds:searchStream"

    query = f"""
        SELECT
            segments.date,
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value
        FROM ad_group
        WHERE segments.date BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'
    """

    headers = {
        "Authorization": f"Bearer {settings.google_ads.refresh_token}",
        "developer-token": settings.google_ads.developer_token,
    }

    response = httpx.post(url, json={"query": query}, headers=headers, timeout=60)
    response.raise_for_status()

    records = []
    now = datetime.now(timezone.utc).isoformat()

    for result in response.json():
        for row in result.get("results", []):
            records.append(
                {
                    "date": row["segments"]["date"],
                    "campaign_id": str(row["campaign"]["id"]),
                    "campaign_name": row["campaign"]["name"],
                    "ad_group_id": str(row["adGroup"]["id"]),
                    "ad_group_name": row["adGroup"]["name"],
                    "impressions": row["metrics"]["impressions"],
                    "clicks": row["metrics"]["clicks"],
                    "cost_micros": row["metrics"]["costMicros"],
                    "conversions": row["metrics"]["conversions"],
                    "conversion_value": row["metrics"]["conversionsValue"],
                    "extracted_at": now,
                }
            )

    return records


# --- Facebook Ads ---

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
def fetch_fb_ads_data(start_date: date, end_date: date) -> list[dict]:
    """Fetch campaign insights from Facebook Marketing API."""
    url = f"https://graph.facebook.com/v19.0/{settings.facebook_ads.ad_account_id}/insights"

    params = {
        "access_token": settings.facebook_ads.access_token,
        "time_range": json.dumps(
            {"since": start_date.isoformat(), "until": end_date.isoformat()}
        ),
        "level": "adset",
        "fields": "campaign_id,campaign_name,adset_id,adset_name,impressions,clicks,spend,actions,action_values,reach,frequency",
        "time_increment": 1,
        "limit": 500,
    }

    records = []
    now = datetime.now(timezone.utc).isoformat()

    while url:
        response = httpx.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        for row in data.get("data", []):
            actions_count = sum(
                int(a.get("value", 0))
                for a in row.get("actions", [])
                if a["action_type"] == "offsite_conversion"
            )
            action_values_sum = sum(
                float(a.get("value", 0))
                for a in row.get("action_values", [])
                if a["action_type"] == "offsite_conversion"
            )

            records.append(
                {
                    "date": row["date_start"],
                    "campaign_id": row["campaign_id"],
                    "campaign_name": row["campaign_name"],
                    "adset_id": row["adset_id"],
                    "adset_name": row["adset_name"],
                    "impressions": int(row["impressions"]),
                    "clicks": int(row["clicks"]),
                    "spend": float(row["spend"]),
                    "actions": actions_count,
                    "action_values": action_values_sum,
                    "reach": int(row.get("reach", 0)),
                    "frequency": float(row.get("frequency", 0)),
                    "extracted_at": now,
                }
            )

        # Pagination
        paging = data.get("paging", {})
        url = paging.get("next")
        params = {}  # next URL has params embedded

    return records


def publish_records(producer: SerializingProducer, topic: str, records: list[dict]):
    """Publish records to Kafka."""
    for record in records:
        producer.produce(topic=topic, value=record)
    producer.flush()
    logger.info("records_published", topic=topic, count=len(records))


def run_marketing_extraction(days_back: int = 7):
    """Run full marketing data extraction and publish to Kafka."""
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back)

    logger.info("marketing_extraction_started", start=str(start_date), end=str(end_date))

    # Google Analytics
    try:
        ga_producer = create_avro_producer(GA_SESSION_SCHEMA, "marketing.ga_sessions")
        ga_data = fetch_ga_data(start_date, end_date)
        publish_records(ga_producer, "marketing.ga_sessions", ga_data)
    except Exception as e:
        logger.error("ga_extraction_failed", error=str(e))

    # Google Ads
    try:
        gads_producer = create_avro_producer(GOOGLE_ADS_SCHEMA, "marketing.google_ads")
        gads_data = fetch_google_ads_data(start_date, end_date)
        publish_records(gads_producer, "marketing.google_ads", gads_data)
    except Exception as e:
        logger.error("google_ads_extraction_failed", error=str(e))

    # Facebook Ads
    try:
        fb_producer = create_avro_producer(FB_ADS_SCHEMA, "marketing.fb_ads")
        fb_data = fetch_fb_ads_data(start_date, end_date)
        publish_records(fb_producer, "marketing.fb_ads", fb_data)
    except Exception as e:
        logger.error("fb_ads_extraction_failed", error=str(e))

    logger.info("marketing_extraction_complete")


if __name__ == "__main__":
    run_marketing_extraction()
