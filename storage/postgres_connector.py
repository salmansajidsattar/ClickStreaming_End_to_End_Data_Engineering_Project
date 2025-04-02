import os
from typing import Dict, Any, List
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

class PostgresConnector:
    def __init__(self, host=None, port=None, database=None, user=None, password=None, min_connections=None, max_connections=None):
        # Allow parameters to override environment variables
        self.host = host or os.environ.get("POSTGRES_HOST", "localhost")
        self.port = port or os.environ.get("POSTGRES_PORT", "5432")
        self.database = database or os.environ.get("POSTGRES_DB", "clickstream_analytics")
        self.user = user or os.environ.get("POSTGRES_USER", "postgres")
        self.password = password or os.environ.get("POSTGRES_PASSWORD", "postgres")
        self.min_connections = min_connections or int(os.environ.get("POSTGRES_MIN_CONNECTIONS", "1"))
        self.max_connections = max_connections or int(os.environ.get("POSTGRES_MAX_CONNECTIONS", "10"))
        self.conn = None
        self.cursor = None

    def connect(self) -> None:
        """Establish connection to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logging.info("Successfully connected to PostgreSQL database")
        except Exception as e:
            logging.error(f"Error connecting to PostgreSQL database: {e}")
            raise

    def disconnect(self) -> None:
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logging.info("PostgreSQL connection closed")

    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        try:
            self.cursor.execute(query, params)
            if query.strip().upper().startswith(('SELECT', 'WITH')):
                return self.cursor.fetchall()
            self.conn.commit()
            return []
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error executing query: {e}")
            raise

    def batch_insert(self, table: str, data: List[Dict[str, Any]]) -> None:
        """Insert multiple rows into a table"""
        if not data:
            return

        columns = data[0].keys()
        values = [tuple(row[column] for column in columns) for row in data]
        
        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(['%s' for _ in columns])})
        """
        
        try:
            self.cursor.executemany(query, values)
            self.conn.commit()
            logging.info(f"Successfully inserted {len(data)} rows into {table}")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error in batch insert: {e}")
            raise

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
