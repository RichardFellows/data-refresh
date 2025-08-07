from abc import ABC, abstractmethod
from typing import List, Dict, Any
import logging
from datetime import datetime, timedelta
from .config import TableConfig
from .database import DatabaseHandler

logger = logging.getLogger(__name__)


class RefreshStrategy(ABC):
    def __init__(self, source_handler: DatabaseHandler, target_handler: DatabaseHandler, table_config: TableConfig):
        self.source_handler = source_handler
        self.target_handler = target_handler
        self.table_config = table_config

    @abstractmethod
    def refresh_table(self) -> Dict[str, Any]:
        pass


class SimpleCopyStrategy(RefreshStrategy):
    def refresh_table(self) -> Dict[str, Any]:
        logger.info(f"Starting simple copy refresh for table {self.table_config.name}")

        if self.table_config.sync_mode == "full_replace":
            return self._full_refresh()
        elif self.table_config.sync_mode == "incremental":
            return self._incremental_refresh()
        elif self.table_config.sync_mode == "smart_sync":
            return self._smart_sync()
        else:
            raise ValueError(f"Unsupported sync mode: {self.table_config.sync_mode}")

    def _full_refresh(self) -> Dict[str, Any]:
        start_time = datetime.now()

        if self.table_config.truncate_target:
            self.target_handler.truncate_table(self.table_config.name)

        query = f"SELECT * FROM {self.table_config.name}"
        if self.table_config.row_limit:
            query += f" ORDER BY 1 OFFSET 0 ROWS FETCH NEXT {self.table_config.row_limit} ROWS ONLY"

        source_data = self.source_handler.execute_query(query)

        if source_data:
            rows_inserted = self.target_handler.bulk_insert(
                self.table_config.name, source_data, self.table_config.batch_size or 5000
            )
        else:
            rows_inserted = 0

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        return {
            "table_name": self.table_config.name,
            "strategy": "simple_copy",
            "sync_mode": "full_replace",
            "rows_processed": rows_inserted,
            "duration_seconds": duration,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "status": "success",
        }

    def _incremental_refresh(self) -> Dict[str, Any]:
        start_time = datetime.now()

        max_value = self.target_handler.get_max_value(self.table_config.name, self.table_config.incremental_column)

        if max_value is None:
            logger.info(f"No existing data found, performing full refresh for {self.table_config.name}")
            return self._full_refresh()

        where_clause = self._build_incremental_where_clause(max_value)
        query = f"SELECT * FROM {self.table_config.name} WHERE {where_clause}"

        source_data = self.source_handler.execute_query(query)

        if source_data:
            rows_inserted = self.target_handler.bulk_insert(
                self.table_config.name, source_data, self.table_config.batch_size or 5000
            )
        else:
            rows_inserted = 0

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        return {
            "table_name": self.table_config.name,
            "strategy": "simple_copy",
            "sync_mode": "incremental",
            "rows_processed": rows_inserted,
            "duration_seconds": duration,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "status": "success",
            "incremental_from": str(max_value),
        }

    def _smart_sync(self) -> Dict[str, Any]:
        target_count = self.target_handler.get_table_count(self.table_config.name)

        if target_count == 0:
            logger.info(f"Target table {self.table_config.name} is empty, performing full refresh")
            result = self._full_refresh()
            result["sync_mode"] = "smart_sync_full"
            return result
        else:
            logger.info(
                f"Target table {self.table_config.name} has {target_count} rows, performing incremental refresh"
            )
            result = self._incremental_refresh()
            result["sync_mode"] = "smart_sync_incremental"
            return result

    def _build_incremental_where_clause(self, max_value: Any) -> str:
        column = self.table_config.incremental_column

        if self.table_config.incremental_type == "identity":
            return f"{column} > {max_value}"
        elif self.table_config.incremental_type == "date":
            if self.table_config.date_buffer_days > 0:
                buffer_date = max_value - timedelta(days=self.table_config.date_buffer_days)
                return f"{column} >= '{buffer_date}'"
            else:
                return f"{column} > '{max_value}'"
        elif self.table_config.incremental_type == "datetime":
            if self.table_config.date_buffer_days > 0:
                buffer_datetime = max_value - timedelta(days=self.table_config.date_buffer_days)
                return f"{column} >= '{buffer_datetime}'"
            else:
                return f"{column} > '{max_value}'"
        else:
            return f"{column} > '{max_value}'"


class StagingPartitionSwitchStrategy(RefreshStrategy):
    def refresh_table(self) -> Dict[str, Any]:
        logger.info(f"Starting staging partition switch refresh for table {self.table_config.name}")

        start_time = datetime.now()
        staging_table = f"{self.table_config.name}_staging"
        partitions_created = []

        try:
            if self.table_config.sync_mode == "incremental":
                data = self._get_incremental_data()
            else:
                data = self._get_full_data()

            if data:
                required_partitions = self._get_required_partitions(data)
                partitions_created = self._ensure_partitions_exist(required_partitions)

                self._create_staging_table(staging_table)

                rows_inserted = self.target_handler.bulk_insert(
                    staging_table, data, self.table_config.batch_size or 10000
                )
            else:
                rows_inserted = 0

            if rows_inserted > 0:
                self._apply_indexes_and_constraints(staging_table)
                self._switch_partitions(staging_table, required_partitions)

            self._cleanup_staging(staging_table)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            return {
                "table_name": self.table_config.name,
                "strategy": "staging_partition_switch",
                "sync_mode": self.table_config.sync_mode,
                "rows_processed": rows_inserted,
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "status": "success",
                "partitions_created": partitions_created,
            }

        except Exception as e:
            self._cleanup_staging(staging_table)
            logger.error(f"Staging partition switch failed for {self.table_config.name}: {e}")
            raise

    def _create_staging_table(self, staging_table: str) -> None:
        create_query = f"""
        SELECT TOP 0 * 
        INTO {staging_table} 
        FROM {self.table_config.name}
        """
        self.target_handler.execute_non_query(create_query)
        logger.debug(f"Created staging table {staging_table}")

    def _get_incremental_data(self) -> List[Dict[str, Any]]:
        max_value = self.target_handler.get_max_value(self.table_config.name, self.table_config.incremental_column)

        if max_value is None:
            return self._get_full_data()

        simple_strategy = SimpleCopyStrategy(self.source_handler, self.target_handler, self.table_config)
        where_clause = simple_strategy._build_incremental_where_clause(max_value)
        query = f"SELECT * FROM {self.table_config.name} WHERE {where_clause}"

        return self.source_handler.execute_query(query)

    def _get_full_data(self) -> List[Dict[str, Any]]:
        query = f"SELECT * FROM {self.table_config.name}"
        return self.source_handler.execute_query(query)

    def _get_required_partitions(self, data: List[Dict[str, Any]]) -> List[int]:
        if not data or not self.table_config.incremental_column:
            return []

        partition_column = self.table_config.incremental_column
        partition_dates = set()

        for row in data:
            date_value = row.get(partition_column)
            if date_value:
                if isinstance(date_value, datetime):
                    partition_date = int(date_value.strftime("%Y%m%d"))
                elif isinstance(date_value, int):
                    partition_date = date_value
                else:
                    try:
                        parsed_date = datetime.strptime(str(date_value)[:8], "%Y%m%d")
                        partition_date = int(parsed_date.strftime("%Y%m%d"))
                    except ValueError:
                        logger.warning(f"Could not parse partition date from: {date_value}")
                        continue

                partition_dates.add(partition_date)

        return sorted(list(partition_dates))

    def _get_existing_partitions(self) -> List[int]:
        query = f"""
        SELECT DISTINCT 
            CAST(prv.value AS INT) as partition_value
        FROM sys.partition_schemes ps
        INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
        INNER JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id
        INNER JOIN sys.indexes i ON ps.data_space_id = i.data_space_id
        INNER JOIN sys.objects o ON i.object_id = o.object_id
        WHERE o.name = '{self.table_config.name}'
        AND pf.type = 'R'
        ORDER BY partition_value
        """

        try:
            results = self.target_handler.execute_query(query)
            return [row["partition_value"] for row in results]
        except Exception as e:
            logger.warning(f"Could not retrieve existing partitions for {self.table_config.name}: {e}")
            return []

    def _ensure_partitions_exist(self, required_partitions: List[int]) -> List[int]:
        if not required_partitions:
            return []

        existing_partitions = self._get_existing_partitions()
        missing_partitions = [p for p in required_partitions if p not in existing_partitions]

        partitions_created = []

        for partition_date in missing_partitions:
            try:
                self._create_partition(partition_date)
                partitions_created.append(partition_date)
                logger.info(f"Created partition for date {partition_date} on table {self.table_config.name}")
            except Exception as e:
                logger.error(f"Failed to create partition for date {partition_date}: {e}")
                raise

        return partitions_created

    def _create_partition(self, partition_date: int) -> None:
        partition_function_name = self.table_config.partition_function or f"pf_{self.table_config.name}"

        split_query = f"""
        ALTER PARTITION FUNCTION {partition_function_name}()
        SPLIT RANGE ({partition_date})
        """

        self.target_handler.execute_non_query(split_query)
        logger.debug(f"Created partition boundary at {partition_date}")

    def _apply_indexes_and_constraints(self, staging_table: str) -> None:
        indexes_query = f"""
        SELECT 
            i.name as index_name,
            i.type_desc,
            i.is_unique,
            STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) as columns
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        INNER JOIN sys.objects o ON i.object_id = o.object_id
        WHERE o.name = '{self.table_config.name}'
        AND i.type > 0
        GROUP BY i.name, i.type_desc, i.is_unique, i.index_id
        ORDER BY i.index_id
        """

        try:
            indexes = self.target_handler.execute_query(indexes_query)

            for index in indexes:
                index_type = "UNIQUE" if index["is_unique"] else ""
                index_name = f"{index['index_name']}_staging"

                create_index_query = f"""
                CREATE {index_type} INDEX {index_name} 
                ON {staging_table} ({index['columns']})
                """

                self.target_handler.execute_non_query(create_index_query)
                logger.debug(f"Created index {index_name} on staging table")

        except Exception as e:
            logger.warning(f"Failed to apply some indexes to staging table: {e}")

    def _switch_partitions(self, staging_table: str, required_partitions: List[int]) -> None:
        if not required_partitions:
            logger.warning("No partitions to switch")
            return

        for partition_date in required_partitions:
            try:
                partition_number = self._get_partition_number(partition_date)

                temp_table = f"{self.table_config.name}_temp_{partition_date}"

                switch_out_query = f"""
                ALTER TABLE {self.table_config.name} 
                SWITCH PARTITION {partition_number} TO {temp_table}
                """

                switch_in_query = f"""
                ALTER TABLE {staging_table} 
                SWITCH PARTITION {partition_number} TO {self.table_config.name} PARTITION {partition_number}
                """

                drop_temp_query = f"DROP TABLE IF EXISTS {temp_table}"

                self.target_handler.execute_non_query(f"SELECT TOP 0 * INTO {temp_table} FROM {self.table_config.name}")
                self.target_handler.execute_non_query(switch_out_query)
                self.target_handler.execute_non_query(switch_in_query)
                self.target_handler.execute_non_query(drop_temp_query)

                logger.info(f"Successfully switched partition {partition_number} for date {partition_date}")

            except Exception as e:
                logger.error(f"Failed to switch partition for date {partition_date}: {e}")
                raise

    def _get_partition_number(self, partition_date: int) -> int:
        partition_function_name = self.table_config.partition_function or f"pf_{self.table_config.name}"

        query = f"""
        SELECT 
            $PARTITION.{partition_function_name}({partition_date}) as partition_number
        """

        result = self.target_handler.execute_query(query)
        if result:
            return result[0]["partition_number"]
        else:
            raise ValueError(f"Could not determine partition number for date {partition_date}")

    def _cleanup_staging(self, staging_table: str) -> None:
        try:
            drop_query = f"DROP TABLE IF EXISTS {staging_table}"
            self.target_handler.execute_non_query(drop_query)
            logger.debug(f"Cleaned up staging table {staging_table}")
        except Exception as e:
            logger.warning(f"Failed to cleanup staging table {staging_table}: {e}")


def get_strategy(
    source_handler: DatabaseHandler, target_handler: DatabaseHandler, table_config: TableConfig
) -> RefreshStrategy:
    if table_config.strategy == "simple_copy":
        return SimpleCopyStrategy(source_handler, target_handler, table_config)
    elif table_config.strategy == "staging_partition_switch":
        return StagingPartitionSwitchStrategy(source_handler, target_handler, table_config)
    else:
        raise ValueError(f"Unknown strategy: {table_config.strategy}")
