import logging
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional
from .config import ConfigManager
from .database import DatabaseConnection, DatabaseHandler
from .refresh_strategies import get_strategy


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DataRefreshService:
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_manager = ConfigManager(config_path)
        self.settings = self.config_manager.get_settings()

        source_config = self.config_manager.get_source_db_config()
        target_config = self.config_manager.get_target_db_config()

        self.source_connection = DatabaseConnection(source_config, self.settings)
        self.target_connection = DatabaseConnection(target_config, self.settings)

        self.source_handler = DatabaseHandler(self.source_connection)
        self.target_handler = DatabaseHandler(self.target_connection)

    def test_connections(self) -> Dict[str, bool]:
        results = {
            "source": self.source_connection.test_connection(),
            "target": self.target_connection.test_connection(),
        }

        logger.info(f"Connection test results: {results}")
        return results

    def refresh_table(self, table_name: str) -> Dict[str, Any]:
        try:
            table_config = self.config_manager.get_table_config(table_name)
            strategy = get_strategy(self.source_handler, self.target_handler, table_config)

            if self.settings.dry_run:
                logger.info(f"DRY RUN: Would refresh table {table_name} using {table_config.strategy}")
                return {
                    "table_name": table_name,
                    "status": "dry_run",
                    "strategy": table_config.strategy,
                    "sync_mode": table_config.sync_mode,
                }

            return strategy.refresh_table()

        except Exception as e:
            logger.error(f"Failed to refresh table {table_name}: {e}")
            return {
                "table_name": table_name,
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }

    def refresh_all_tables(self) -> List[Dict[str, Any]]:
        table_configs = self.config_manager.get_table_configs()
        results = []

        logger.info(f"Starting refresh for {len(table_configs)} tables")

        for table_config in table_configs:
            result = self.refresh_table(table_config.name)
            results.append(result)

        logger.info(
            f"Completed refresh for all tables. Success: {sum(1 for r in results if r.get('status') == 'success')}, Errors: {sum(1 for r in results if r.get('status') == 'error')}"
        )

        return results

    def get_table_status(self, table_name: Optional[str] = None) -> List[Dict[str, Any]]:
        table_configs = self.config_manager.get_table_configs()

        if table_name:
            table_configs = [config for config in table_configs if config.name == table_name]

        status_list = []

        for table_config in table_configs:
            try:
                source_count = self.source_handler.get_table_count(table_config.name)
                target_count = self.target_handler.get_table_count(table_config.name)

                status = {
                    "table_name": table_config.name,
                    "source_count": source_count,
                    "target_count": target_count,
                    "sync_mode": table_config.sync_mode,
                    "strategy": table_config.strategy,
                    "last_checked": datetime.now().isoformat(),
                }

                if table_config.incremental_column:
                    source_max = self.source_handler.get_max_value(table_config.name, table_config.incremental_column)
                    target_max = self.target_handler.get_max_value(table_config.name, table_config.incremental_column)
                    status.update(
                        {
                            "source_max_value": str(source_max) if source_max else None,
                            "target_max_value": str(target_max) if target_max else None,
                        }
                    )

                status_list.append(status)

            except Exception as e:
                logger.error(f"Failed to get status for table {table_config.name}: {e}")
                status_list.append(
                    {"table_name": table_config.name, "error": str(e), "last_checked": datetime.now().isoformat()}
                )

        return status_list


def main():
    parser = argparse.ArgumentParser(description="Data Refresh Service")
    parser.add_argument("--config", default="config/config.yaml", help="Configuration file path")
    parser.add_argument("--table", help="Specific table to refresh")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without executing")
    parser.add_argument("--test-connections", action="store_true", help="Test database connections")
    parser.add_argument("--status", action="store_true", help="Show table status")
    parser.add_argument("--force", action="store_true", help="Skip confirmations")

    args = parser.parse_args()

    service = DataRefreshService(args.config)

    if args.test_connections:
        results = service.test_connections()
        print(f"Connection test results: {results}")
        return

    if args.status:
        status = service.get_table_status(args.table)
        for table_status in status:
            print(f"Table: {table_status['table_name']}")
            for key, value in table_status.items():
                if key != "table_name":
                    print(f"  {key}: {value}")
            print()
        return

    if args.table:
        if not args.force:
            confirmation = input(f"Refresh table '{args.table}'? (y/N): ")
            if confirmation.lower() != "y":
                print("Cancelled.")
                return

        result = service.refresh_table(args.table)
        print(f"Refresh result: {result}")
    else:
        if not args.force:
            confirmation = input("Refresh all tables? (y/N): ")
            if confirmation.lower() != "y":
                print("Cancelled.")
                return

        results = service.refresh_all_tables()
        for result in results:
            print(f"Table {result['table_name']}: {result.get('status', 'unknown')}")


if __name__ == "__main__":
    main()
