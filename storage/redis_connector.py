import os
import json
from typing import Dict, Any, Optional, Union
import redis
import logging

class RedisConnector:
    def __init__(self, host=None, port=None, db=None, password=None, decode_responses=True):
        # Allow parameters to override environment variables
        self.host = host or os.environ.get("REDIS_HOST", "localhost")
        self.port = int(port or os.environ.get("REDIS_PORT", "6379"))
        self.db = int(db or os.environ.get("REDIS_DB", "0"))
        self.password = password or os.environ.get("REDIS_PASSWORD", None)
        self.decode_responses = decode_responses
        self.client = None

    def connect(self) -> None:
        """Establish connection to Redis"""
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=self.decode_responses
            )
            self.client.ping()  # Test connection
            logging.info("Successfully connected to Redis")
        except Exception as e:
            logging.error(f"Error connecting to Redis: {e}")
            raise

    def disconnect(self) -> None:
        """Close Redis connection"""
        if self.client:
            self.client.close()
            logging.info("Redis connection closed")

    def set_value(self, key: str, value: Union[str, Dict, list], 
                  expiry: Optional[int] = None) -> None:
        """Set a key-value pair in Redis"""
        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            self.client.set(key, value, ex=expiry)
        except Exception as e:
            logging.error(f"Error setting value in Redis: {e}")
            raise

    def get_value(self, key: str, deserialize: bool = False) -> Any:
        """Get value for a key from Redis"""
        try:
            value = self.client.get(key)
            if value and deserialize:
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return value
        except Exception as e:
            logging.error(f"Error getting value from Redis: {e}")
            raise

    def increment(self, key: str, amount: int = 1) -> int:
        """Increment a key's value"""
        try:
            return self.client.incrby(key, amount)
        except Exception as e:
            logging.error(f"Error incrementing value in Redis: {e}")
            raise

    def expire(self, key: str, seconds: int) -> None:
        """Set expiration time for a key"""
        try:
            self.client.expire(key, seconds)
        except Exception as e:
            logging.error(f"Error setting expiry in Redis: {e}")
            raise

    def delete_key(self, key: str) -> None:
        """Delete a key from Redis"""
        try:
            self.client.delete(key)
        except Exception as e:
            logging.error(f"Error deleting key from Redis: {e}")
            raise

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
