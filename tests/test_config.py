import pytest
import os
import tempfile
import yaml
from src.config import ConfigManager, DatabaseConfig, TableConfig, Settings


@pytest.fixture
def sample_config():
    return {
        "databases": {
            "source": {
                "server": "source-server",
                "database": "SourceDB",
                "auth_type": "windows"
            },
            "target": {
                "server": "target-server",
                "database": "TargetDB",
                "auth_type": "sql"
            }
        },
        "tables": [
            {
                "name": "TestTable",
                "strategy": "simple_copy",
                "sync_mode": "full_replace",
                "truncate_target": True
            },
            {
                "name": "PartitionedTable",
                "strategy": "staging_partition_switch",
                "sync_mode": "incremental",
                "incremental_column": "report_date",
                "incremental_type": "date",
                "partition_function": "pf_PartitionedTable",
                "partition_scheme": "ps_PartitionedTable"
            }
        ],
        "settings": {
            "default_batch_size": 1000,
            "connection_timeout": 30,
            "command_timeout": 300,
            "max_retries": 3
        }
    }


@pytest.fixture
def config_file(sample_config):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(sample_config, f)
        yield f.name
    os.unlink(f.name)


def test_config_manager_initialization(config_file):
    config_manager = ConfigManager(config_file)
    assert config_manager.config_path == config_file


def test_get_source_db_config(config_file):
    os.environ['SOURCE_DB_USER'] = 'test_user'
    os.environ['SOURCE_DB_PASSWORD'] = 'test_pass'
    
    config_manager = ConfigManager(config_file)
    source_config = config_manager.get_source_db_config()
    
    assert isinstance(source_config, DatabaseConfig)
    assert source_config.server == "source-server"
    assert source_config.database == "SourceDB"
    assert source_config.auth_type == "windows"
    assert source_config.user == "test_user"
    assert source_config.password == "test_pass"


def test_get_target_db_config(config_file):
    os.environ['TARGET_DB_USER'] = 'target_user'
    os.environ['TARGET_DB_PASSWORD'] = 'target_pass'
    
    config_manager = ConfigManager(config_file)
    target_config = config_manager.get_target_db_config()
    
    assert isinstance(target_config, DatabaseConfig)
    assert target_config.server == "target-server"
    assert target_config.database == "TargetDB"
    assert target_config.auth_type == "sql"
    assert target_config.user == "target_user"
    assert target_config.password == "target_pass"


def test_get_table_configs(config_file):
    config_manager = ConfigManager(config_file)
    table_configs = config_manager.get_table_configs()
    
    assert len(table_configs) == 2
    
    # First table
    assert isinstance(table_configs[0], TableConfig)
    assert table_configs[0].name == "TestTable"
    assert table_configs[0].strategy == "simple_copy"
    assert table_configs[0].sync_mode == "full_replace"
    assert table_configs[0].truncate_target is True
    
    # Second table with partition config
    assert isinstance(table_configs[1], TableConfig)
    assert table_configs[1].name == "PartitionedTable"
    assert table_configs[1].strategy == "staging_partition_switch"
    assert table_configs[1].sync_mode == "incremental"
    assert table_configs[1].incremental_column == "report_date"
    assert table_configs[1].incremental_type == "date"
    assert table_configs[1].partition_function == "pf_PartitionedTable"
    assert table_configs[1].partition_scheme == "ps_PartitionedTable"


def test_get_table_config(config_file):
    config_manager = ConfigManager(config_file)
    table_config = config_manager.get_table_config("TestTable")
    
    assert isinstance(table_config, TableConfig)
    assert table_config.name == "TestTable"


def test_get_table_config_not_found(config_file):
    config_manager = ConfigManager(config_file)
    
    with pytest.raises(ValueError, match="Table 'NonExistent' not found"):
        config_manager.get_table_config("NonExistent")


def test_get_settings(config_file):
    config_manager = ConfigManager(config_file)
    settings = config_manager.get_settings()
    
    assert isinstance(settings, Settings)
    assert settings.default_batch_size == 1000
    assert settings.connection_timeout == 30
    assert settings.command_timeout == 300
    assert settings.max_retries == 3


def test_get_partitioned_table_config(config_file):
    config_manager = ConfigManager(config_file)
    table_config = config_manager.get_table_config("PartitionedTable")
    
    assert isinstance(table_config, TableConfig)
    assert table_config.name == "PartitionedTable"
    assert table_config.strategy == "staging_partition_switch"
    assert table_config.partition_function == "pf_PartitionedTable"
    assert table_config.partition_scheme == "ps_PartitionedTable"