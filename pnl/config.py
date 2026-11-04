from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PNL_", env_file=".env", extra="ignore")

    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_user: str = "pnl"
    mysql_password: str = "pnl"
    mysql_db: str = "pnl"

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_max_connections: int = 1000
    redis_pool_timeout_seconds: float = 5.0

    kafka_bootstrap_servers: str = "localhost:9092"

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123

    fanout_max_concurrency: int = 500
    fanout_client_timeout_seconds: float = 5.0
    coalescer_max_drain_wait_seconds: float = 1.0
    watermark_lateness_seconds: float = 2.0


settings = Settings()
