# config.py
import os
from typing import Dict, Any

class Config:
    """Configuration management for the clickstream analytics application."""
    
    @staticmethod
    def get_kafka_config() -> Dict[str, Any]:
        """Get Kafka configuration from environment variables."""
        return {
            "bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "topic": os.environ.get("KAFKA_TOPIC", "clickstream"),
            "client_id": os.environ.get("KAFKA_CLIENT_ID", "clickstream-client"),
            "group_id": os.environ.get("KAFKA_GROUP_ID", "clickstream-group"),
            "auto_offset_reset": os.environ.get("KAFKA_AUTO_OFFSET_RESET", "latest"),
            "enable_auto_commit": os.environ.get("KAFKA_ENABLE_AUTO_COMMIT", "true").lower() == "true"
        }
    
    @staticmethod
    def get_postgres_config() -> Dict[str, Any]:
        """Get PostgreSQL configuration from environment variables."""
        return {
            "host": os.environ.get("POSTGRES_HOST", "postgres"),
            "port": int(os.environ.get("POSTGRES_PORT", "5432")),
            "database": os.environ.get("POSTGRES_DB", "clickstream_analytics"),
            "user": os.environ.get("POSTGRES_USER", "postgres"),
            "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
            "min_connections": int(os.environ.get("POSTGRES_MIN_CONNECTIONS", "1")),
            "max_connections": int(os.environ.get("POSTGRES_MAX_CONNECTIONS", "10"))
        }
    
    @staticmethod
    def get_redis_config() -> Dict[str, Any]:
        """Get Redis configuration from environment variables."""
        return {
            "host": os.environ.get("REDIS_HOST", "redis"),
            "port": int(os.environ.get("REDIS_PORT", "6379")),
            "db": int(os.environ.get("REDIS_DB", "0")),
            "password": os.environ.get("REDIS_PASSWORD", None),
            "decode_responses": True
        }
    
    @staticmethod
    def get_spark_config() -> Dict[str, Any]:
        """Get Spark configuration from environment variables."""
        return {
            "app_name": os.environ.get("SPARK_APP_NAME", "ClickstreamProcessor"),
            "master": os.environ.get("SPARK_MASTER", "local[*]"),
            "checkpoint_dir": os.environ.get("CHECKPOINT_DIR", "/tmp/clickstream_checkpoints"),
            "batch_duration": int(os.environ.get("SPARK_BATCH_DURATION", "5")),
            "shuffle_partitions": int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "10")),
            "max_offsets_per_trigger": int(os.environ.get("SPARK_MAX_OFFSETS_PER_TRIGGER", "10000"))
        }
    
    @staticmethod
    def get_generator_config() -> Dict[str, Any]:
        """Get data generator configuration from environment variables."""
        return {
            "events_per_second": int(os.environ.get("EVENTS_PER_SECOND", "20")),
            "duration_seconds": int(os.environ.get("DURATION_SECONDS", "0")) or None,
            "num_users": int(os.environ.get("NUM_USERS", "1000"))
        }
    
    @staticmethod
    def get_dashboard_config() -> Dict[str, Any]:
        """Get dashboard configuration from environment variables."""
        return {
            "host": os.environ.get("DASHBOARD_HOST", "0.0.0.0"),
            "port": int(os.environ.get("DASHBOARD_PORT", "8050")),
            "debug": os.environ.get("DASHBOARD_DEBUG", "false").lower() == "true",
            "refresh_interval": int(os.environ.get("DASHBOARD_REFRESH_INTERVAL", "5"))
        }