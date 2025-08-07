import pyodbc
import logging
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from .config import DatabaseConfig, Settings


logger = logging.getLogger(__name__)


class DatabaseConnection:
    def __init__(self, config: DatabaseConfig, settings: Settings):
        self.config = config
        self.settings = settings
        self._connection_string = self._build_connection_string()

    def _build_connection_string(self) -> str:
        if self.config.auth_type == "windows":
            return (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.config.server};"
                f"DATABASE={self.config.database};"
                f"Trusted_Connection=yes;"
                f"Connection Timeout={self.settings.connection_timeout};"
            )
        else:
            return (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.config.server};"
                f"DATABASE={self.config.database};"
                f"UID={self.config.user};"
                f"PWD={self.config.password};"
                f"Connection Timeout={self.settings.connection_timeout};"
            )

    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = pyodbc.connect(self._connection_string)
            conn.timeout = self.settings.command_timeout
            yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

    def test_connection(self) -> bool:
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


class DatabaseHandler:
    def __init__(self, connection: DatabaseConnection):
        self.connection = connection

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        with self.connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())

            columns = [column[0] for column in cursor.description] if cursor.description else []
            rows = cursor.fetchall()

            return [dict(zip(columns, row)) for row in rows]

    def execute_non_query(self, query: str, params: Optional[tuple] = None) -> int:
        with self.connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            conn.commit()
            return cursor.rowcount

    def get_max_value(self, table_name: str, column_name: str) -> Optional[Any]:
        query = f"SELECT MAX({column_name}) as max_value FROM {table_name}"
        try:
            result = self.execute_query(query)
            return result[0]["max_value"] if result and result[0]["max_value"] is not None else None
        except Exception as e:
            logger.warning(f"Could not get max value for {table_name}.{column_name}: {e}")
            return None

    def get_table_count(self, table_name: str, where_clause: str = None) -> int:
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        result = self.execute_query(query)
        return result[0]["count"] if result else 0

    def truncate_table(self, table_name: str) -> None:
        query = f"TRUNCATE TABLE {table_name}"
        self.execute_non_query(query)
        logger.info(f"Truncated table {table_name}")

    def bulk_insert(self, table_name: str, data: List[Dict[str, Any]], batch_size: int = 1000) -> int:
        if not data:
            return 0

        columns = list(data[0].keys())
        placeholders = ", ".join(["?" for _ in columns])
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        total_inserted = 0

        with self.connection.get_connection() as conn:
            cursor = conn.cursor()

            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                values = [tuple(row[col] for col in columns) for row in batch]

                cursor.executemany(query, values)
                conn.commit()
                total_inserted += len(batch)

                logger.debug(f"Inserted batch of {len(batch)} rows into {table_name}")

        return total_inserted
