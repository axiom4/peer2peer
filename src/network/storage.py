import sqlite3
import json
import time
import os
import logging
from typing import Any, List, Optional, Union

logger = logging.getLogger(__name__)

class DHTStorage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        # check_same_thread=False allows using connection across threads
        # We must ensure we don't write concurrently if not safe, but SQLite handles locking.
        # Ideally we use a lock for application-level consistency if needed.
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        try:
            with self.conn:
                self.conn.execute("""
                    CREATE TABLE IF NOT EXISTS dht (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        timestamp REAL
                    )
                """)
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ts ON dht(timestamp)")
        except Exception as e:
            logger.error(f"Failed to initialize DHT database: {e}")

    def get(self, key: str, default: Any = None) -> Any:
        try:
            cursor = self.conn.execute("SELECT value FROM dht WHERE key = ?", (key,))
            row = cursor.fetchone()
            if row:
                return json.loads(row['value'])
            return default
        except Exception as e:
            logger.error(f"DHT Read Error: {e}")
            return default

    def set(self, key: str, value: Any):
        try:
            val_json = json.dumps(value)
            now = time.time()
            with self.conn:
                self.conn.execute(
                    "INSERT OR REPLACE INTO dht (key, value, timestamp) VALUES (?, ?, ?)",
                    (key, val_json, now)
                )
        except Exception as e:
            logger.error(f"DHT Write Error: {e}")

    def delete(self, key: str):
        try:
            with self.conn:
                self.conn.execute("DELETE FROM dht WHERE key = ?", (key,))
        except Exception as e:
            logger.error(f"DHT Delete Error: {e}")

    def contains(self, key: str) -> bool:
        try:
            cursor = self.conn.execute("SELECT 1 FROM dht WHERE key = ?", (key,))
            return cursor.fetchone() is not None
        except Exception:
            return False

    def items(self):
        """Yields (key, value) pairs. Warning: May be slow for large datasets."""
        try:
            cursor = self.conn.execute("SELECT key, value FROM dht")
            while True:
                rows = cursor.fetchmany(100)
                if not rows:
                    break
                for row in rows:
                    yield row['key'], json.loads(row['value'])
        except Exception as e:
            logger.error(f"DHT Iteration Error: {e}")

    def cleanup_expired(self, ttl: int, exclude_prefix: str = None):
        """Removes entries older than TTL seconds."""
        try:
            limit_ts = time.time() - ttl
            with self.conn:
                if exclude_prefix:
                    self.conn.execute(
                        "DELETE FROM dht WHERE timestamp < ? AND key NOT LIKE ?",
                        (limit_ts, f"{exclude_prefix}%")
                    )
                else:
                    self.conn.execute("DELETE FROM dht WHERE timestamp < ?", (limit_ts,))
        except Exception as e:
            logger.error(f"DHT Cleanup Error: {e}")

    def close(self):
        self.conn.close()
