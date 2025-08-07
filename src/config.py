import yaml
import os
from typing import Dict, Any, List
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class DatabaseConfig:
    server: str
    database: str
    auth_type: str
    user: str = None
    password: str = None


@dataclass
class TableConfig:
    name: str
    strategy: str
    sync_mode: str
    incremental_column: str = None
    incremental_type: str = None
    truncate_target: bool = False
    date_buffer_days: int = 0
    batch_size: int = None
    fallback_to_full: bool = False
    row_limit: int = None
    partition_function: str = None
    partition_scheme: str = None


@dataclass
class Settings:
    default_batch_size: int
    connection_timeout: int
    command_timeout: int
    max_retries: int
    dry_run: bool = False
    verbose_logging: bool = False


class ConfigManager:
    def __init__(self, config_path: str = "config/config.yaml"):
        load_dotenv()
        self.config_path = config_path
        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        with open(self.config_path, "r") as file:
            return yaml.safe_load(file)

    def get_source_db_config(self) -> DatabaseConfig:
        db_config = self._config["databases"]["source"]
        return DatabaseConfig(
            server=db_config["server"],
            database=db_config["database"],
            auth_type=db_config["auth_type"],
            user=os.getenv("SOURCE_DB_USER"),
            password=os.getenv("SOURCE_DB_PASSWORD"),
        )

    def get_target_db_config(self) -> DatabaseConfig:
        db_config = self._config["databases"]["target"]
        return DatabaseConfig(
            server=db_config["server"],
            database=db_config["database"],
            auth_type=db_config["auth_type"],
            user=os.getenv("TARGET_DB_USER"),
            password=os.getenv("TARGET_DB_PASSWORD"),
        )

    def get_table_configs(self) -> List[TableConfig]:
        tables = []
        for table_config in self._config["tables"]:
            tables.append(TableConfig(**table_config))
        return tables

    def get_table_config(self, table_name: str) -> TableConfig:
        for table_config in self._config["tables"]:
            if table_config["name"] == table_name:
                return TableConfig(**table_config)
        raise ValueError(f"Table '{table_name}' not found in configuration")

    def get_settings(self) -> Settings:
        settings_config = self._config["settings"]
        return Settings(**settings_config)
