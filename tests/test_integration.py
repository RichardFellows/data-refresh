import pytest
import sys
from unittest.mock import Mock, patch, MagicMock


@pytest.fixture(autouse=True)
def mock_pyodbc():
    # Mock pyodbc module at the system level
    mock_pyodbc = MagicMock()
    sys.modules['pyodbc'] = mock_pyodbc
    yield mock_pyodbc
    # Clean up
    if 'pyodbc' in sys.modules:
        del sys.modules['pyodbc']


def test_database_connection_creation(mock_pyodbc):
    from src.database import DatabaseConnection
    from src.config import DatabaseConfig, Settings
    
    config = DatabaseConfig(
        server="test-server",
        database="TestDB", 
        auth_type="windows"
    )
    settings = Settings(
        default_batch_size=1000,
        connection_timeout=30,
        command_timeout=300,
        max_retries=3
    )
    
    connection = DatabaseConnection(config, settings)
    
    expected = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=test-server;"
        "DATABASE=TestDB;"
        "Trusted_Connection=yes;"
        "Connection Timeout=30;"
    )
    
    assert connection._connection_string == expected


def test_simple_copy_strategy_full_replace(mock_pyodbc):
    from src.refresh_strategies import SimpleCopyStrategy
    from src.config import TableConfig
    from unittest.mock import Mock
    
    config = TableConfig(
        name="TestTable",
        strategy="simple_copy",
        sync_mode="full_replace",
        truncate_target=True
    )
    
    source_handler = Mock()
    target_handler = Mock()
    
    test_data = [{'id': 1, 'name': 'Test1'}]
    source_handler.execute_query.return_value = test_data
    target_handler.bulk_insert.return_value = 1
    
    strategy = SimpleCopyStrategy(source_handler, target_handler, config)
    result = strategy.refresh_table()
    
    assert result['table_name'] == "TestTable"
    assert result['strategy'] == "simple_copy"
    assert result['sync_mode'] == "full_replace"
    assert result['rows_processed'] == 1
    target_handler.truncate_table.assert_called_once_with("TestTable")


def test_partition_strategy_get_required_partitions(mock_pyodbc):
    from src.refresh_strategies import StagingPartitionSwitchStrategy
    from src.config import TableConfig
    from datetime import datetime
    from unittest.mock import Mock
    
    config = TableConfig(
        name="DailyReports",
        strategy="staging_partition_switch",
        sync_mode="incremental",
        incremental_column="report_date"
    )
    
    strategy = StagingPartitionSwitchStrategy(Mock(), Mock(), config)
    
    data = [
        {'report_date': datetime(2025, 2, 7)},
        {'report_date': datetime(2025, 2, 8)},
        {'report_date': 20250209}  # Also test integer format
    ]
    
    partitions = strategy._get_required_partitions(data)
    assert sorted(partitions) == [20250207, 20250208, 20250209]


def test_partition_strategy_ensure_partitions_exist(mock_pyodbc):
    from src.refresh_strategies import StagingPartitionSwitchStrategy
    from src.config import TableConfig
    from unittest.mock import Mock, patch
    
    config = TableConfig(
        name="DailyReports",
        strategy="staging_partition_switch",
        sync_mode="incremental",
        incremental_column="report_date",
        partition_function="pf_DailyReports"
    )
    
    source_handler = Mock()
    target_handler = Mock()
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, config)
    
    with patch.object(strategy, '_get_existing_partitions', return_value=[20250206]):
        with patch.object(strategy, '_create_partition') as mock_create:
            created = strategy._ensure_partitions_exist([20250206, 20250207, 20250208])
            
            assert created == [20250207, 20250208]
            assert mock_create.call_count == 2
            mock_create.assert_any_call(20250207)
            mock_create.assert_any_call(20250208)


def test_config_table_with_partitions(mock_pyodbc):
    from src.config import ConfigManager, TableConfig
    import tempfile
    import yaml
    import os
    
    config_data = {
        "databases": {
            "source": {"server": "src", "database": "db", "auth_type": "windows"},
            "target": {"server": "tgt", "database": "db", "auth_type": "sql"}
        },
        "tables": [{
            "name": "PartitionedTable",
            "strategy": "staging_partition_switch",
            "sync_mode": "incremental",
            "incremental_column": "report_date",
            "partition_function": "pf_PartitionedTable",
            "partition_scheme": "ps_PartitionedTable"
        }],
        "settings": {
            "default_batch_size": 1000,
            "connection_timeout": 30,
            "command_timeout": 300,
            "max_retries": 3
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_data, f)
        config_file = f.name
    
    try:
        config_manager = ConfigManager(config_file)
        table_config = config_manager.get_table_config("PartitionedTable")
        
        assert isinstance(table_config, TableConfig)
        assert table_config.partition_function == "pf_PartitionedTable"
        assert table_config.partition_scheme == "ps_PartitionedTable"
    finally:
        os.unlink(config_file)


def test_data_refresh_service_creation(mock_pyodbc):
    from src.data_refresh import DataRefreshService
    import tempfile
    import yaml
    import os
    
    config_data = {
        "databases": {
            "source": {"server": "src", "database": "db", "auth_type": "windows"},
            "target": {"server": "tgt", "database": "db", "auth_type": "sql"}
        },
        "tables": [{
            "name": "TestTable",
            "strategy": "simple_copy",
            "sync_mode": "full_replace"
        }],
        "settings": {
            "default_batch_size": 1000,
            "connection_timeout": 30,
            "command_timeout": 300,
            "max_retries": 3
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_data, f)
        config_file = f.name
    
    try:
        service = DataRefreshService(config_file)
        assert service.config_manager is not None
        assert service.source_connection is not None
        assert service.target_connection is not None
    finally:
        os.unlink(config_file)