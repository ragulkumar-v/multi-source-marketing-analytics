from pydantic_settings import BaseSettings


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"

    model_config = {"env_prefix": "KAFKA_"}


class PostgresSourceSettings(BaseSettings):
    host: str = "localhost"
    port: int = 5432
    db: str = "crm_source"
    user: str = "debezium"
    password: str = ""

    model_config = {"env_prefix": "PG_"}


class MySQLSourceSettings(BaseSettings):
    host: str = "localhost"
    port: int = 3306
    db: str = "ecommerce_source"
    user: str = "debezium"
    password: str = ""

    model_config = {"env_prefix": "MYSQL_"}


class GoogleAdsSettings(BaseSettings):
    developer_token: str = ""
    client_id: str = ""
    client_secret: str = ""
    refresh_token: str = ""
    customer_id: str = ""

    model_config = {"env_prefix": "GOOGLE_ADS_"}


class FacebookAdsSettings(BaseSettings):
    access_token: str = ""
    ad_account_id: str = ""

    model_config = {"env_prefix": "FB_"}


class SnowflakeSettings(BaseSettings):
    account: str = ""
    user: str = ""
    password: str = ""
    database: str = "MARKETING_ANALYTICS"
    schema_name: str = "RAW"
    warehouse: str = "COMPUTE_WH"

    model_config = {"env_prefix": "SNOWFLAKE_"}


class Settings(BaseSettings):
    kafka: KafkaSettings = KafkaSettings()
    postgres: PostgresSourceSettings = PostgresSourceSettings()
    mysql: MySQLSourceSettings = MySQLSourceSettings()
    google_ads: GoogleAdsSettings = GoogleAdsSettings()
    facebook_ads: FacebookAdsSettings = FacebookAdsSettings()
    snowflake: SnowflakeSettings = SnowflakeSettings()
    connect_url: str = "http://localhost:8083"
    ga_property_id: str = ""
    ga_credentials_path: str = "credentials/ga-key.json"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
