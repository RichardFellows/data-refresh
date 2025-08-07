import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from src.config import TableConfig


@pytest.fixture
def mock_handlers():
    from unittest.mock import Mock
    source_handler = Mock()
    target_handler = Mock()
    return source_handler, target_handler


@pytest.fixture
def full_replace_config():
    return TableConfig(
        name="TestTable",
        strategy="simple_copy",
        sync_mode="full_replace",
        truncate_target=True
    )


@pytest.fixture
def incremental_config():
    return TableConfig(
        name="TestTable",
        strategy="simple_copy",
        sync_mode="incremental",
        incremental_column="id",
        incremental_type="identity"
    )


@pytest.fixture
def smart_sync_config():
    return TableConfig(
        name="TestTable",
        strategy="simple_copy",
        sync_mode="smart_sync",
        incremental_column="updated_at",
        incremental_type="datetime"
    )


@pytest.fixture
def partition_switch_config():
    return TableConfig(
        name="DailyReports",
        strategy="staging_partition_switch",
        sync_mode="incremental",
        incremental_column="report_date",
        incremental_type="date",
        date_buffer_days=7,
        batch_size=10000,
        partition_function="pf_DailyReports",
        partition_scheme="ps_DailyReports"
    )


@patch('src.refresh_strategies.pyodbc')
def test_full_replace_strategy(mock_pyodbc, mock_handlers, full_replace_config):
    from src.refresh_strategies import SimpleCopyStrategy
    
    source_handler, target_handler = mock_handlers
    
    test_data = [
        {'id': 1, 'name': 'Test1'},
        {'id': 2, 'name': 'Test2'}
    ]
    
    source_handler.execute_query.return_value = test_data
    target_handler.bulk_insert.return_value = 2
    
    strategy = SimpleCopyStrategy(source_handler, target_handler, full_replace_config)
    result = strategy.refresh_table()
    
    target_handler.truncate_table.assert_called_once_with("TestTable")
    source_handler.execute_query.assert_called_once_with("SELECT * FROM TestTable")
    target_handler.bulk_insert.assert_called_once_with("TestTable", test_data, 5000)
    
    assert result['table_name'] == "TestTable"
    assert result['strategy'] == "simple_copy"
    assert result['sync_mode'] == "full_replace"
    assert result['rows_processed'] == 2
    assert result['status'] == "success"


@patch('src.refresh_strategies.pyodbc')
def test_incremental_strategy_with_existing_data(mock_pyodbc, mock_handlers, incremental_config):
    from src.refresh_strategies import SimpleCopyStrategy
    
    source_handler, target_handler = mock_handlers
    
    target_handler.get_max_value.return_value = 5
    
    test_data = [
        {'id': 6, 'name': 'Test6'},
        {'id': 7, 'name': 'Test7'}
    ]
    
    source_handler.execute_query.return_value = test_data
    target_handler.bulk_insert.return_value = 2
    
    strategy = SimpleCopyStrategy(source_handler, target_handler, incremental_config)
    result = strategy.refresh_table()
    
    target_handler.get_max_value.assert_called_once_with("TestTable", "id")
    source_handler.execute_query.assert_called_once_with("SELECT * FROM TestTable WHERE id > 5")
    target_handler.bulk_insert.assert_called_once_with("TestTable", test_data, 5000)
    
    assert result['sync_mode'] == "incremental"
    assert result['rows_processed'] == 2
    assert result['incremental_from'] == "5"


@patch('src.refresh_strategies.pyodbc')
def test_partition_switch_strategy_basic(mock_pyodbc, mock_handlers, partition_switch_config):
    from src.refresh_strategies import StagingPartitionSwitchStrategy
    
    source_handler, target_handler = mock_handlers
    
    test_data = [
        {'report_date': 20250207, 'amount': 100.0},
        {'report_date': 20250208, 'amount': 150.0}
    ]
    
    source_handler.execute_query.return_value = test_data
    target_handler.get_max_value.return_value = 20250206
    target_handler.execute_query.return_value = []  # No existing partitions
    target_handler.bulk_insert.return_value = 2
    
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, partition_switch_config)
    
    with patch.object(strategy, '_ensure_partitions_exist', return_value=[20250207, 20250208]):
        with patch.object(strategy, '_apply_indexes_and_constraints'):
            with patch.object(strategy, '_switch_partitions'):
                result = strategy.refresh_table()
    
    assert result['table_name'] == "DailyReports"
    assert result['strategy'] == "staging_partition_switch"
    assert result['rows_processed'] == 2
    assert result['partitions_created'] == [20250207, 20250208]


@patch('src.refresh_strategies.pyodbc')
def test_get_required_partitions_from_datetime(mock_pyodbc):
    from src.refresh_strategies import StagingPartitionSwitchStrategy
    
    source_handler = Mock()
    target_handler = Mock()
    config = TableConfig(
        name="TestTable",
        strategy="staging_partition_switch",
        sync_mode="incremental",
        incremental_column="report_date"
    )
    
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, config)
    
    data = [
        {'report_date': datetime(2025, 2, 7, 10, 30)},
        {'report_date': datetime(2025, 2, 8, 15, 45)},
        {'report_date': datetime(2025, 2, 7, 20, 15)}  # Duplicate date
    ]
    
    partitions = strategy._get_required_partitions(data)
    
    assert partitions == [20250207, 20250208]


@patch('src.refresh_strategies.pyodbc')
def test_get_required_partitions_from_int(mock_pyodbc):
    from src.refresh_strategies import StagingPartitionSwitchStrategy
    
    source_handler = Mock()
    target_handler = Mock()
    config = TableConfig(
        name="TestTable",
        strategy="staging_partition_switch",
        sync_mode="incremental",
        incremental_column="report_date"
    )
    
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, config)
    
    data = [
        {'report_date': 20250207},
        {'report_date': 20250208},
        {'report_date': 20250207}  # Duplicate
    ]
    
    partitions = strategy._get_required_partitions(data)
    
    assert partitions == [20250207, 20250208]


@patch('src.refresh_strategies.pyodbc')
def test_get_existing_partitions(mock_pyodbc, mock_handlers, partition_switch_config):
    from src.refresh_strategies import StagingPartitionSwitchStrategy
    
    source_handler, target_handler = mock_handlers
    
    target_handler.execute_query.return_value = [
        {'partition_value': 20250205},
        {'partition_value': 20250206}
    ]
    
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, partition_switch_config)
    existing_partitions = strategy._get_existing_partitions()
    
    assert existing_partitions == [20250205, 20250206]
    target_handler.execute_query.assert_called_once()


@patch('src.refresh_strategies.pyodbc')
def test_ensure_partitions_exist(mock_pyodbc, mock_handlers, partition_switch_config):
    from src.refresh_strategies import StagingPartitionSwitchStrategy
    
    source_handler, target_handler = mock_handlers
    
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, partition_switch_config)
    
    with patch.object(strategy, '_get_existing_partitions', return_value=[20250206]):
        with patch.object(strategy, '_create_partition') as mock_create:
            created = strategy._ensure_partitions_exist([20250206, 20250207, 20250208])
            
            assert created == [20250207, 20250208]
            assert mock_create.call_count == 2
            mock_create.assert_any_call(20250207)
            mock_create.assert_any_call(20250208)


@patch('src.refresh_strategies.pyodbc')
def test_create_partition(mock_pyodbc, mock_handlers, partition_switch_config):
    from src.refresh_strategies import StagingPartitionSwitchStrategy
    
    source_handler, target_handler = mock_handlers
    
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, partition_switch_config)
    strategy._create_partition(20250207)
    
    target_handler.execute_non_query.assert_called_once()
    actual_query = target_handler.execute_non_query.call_args[0][0]
    assert "ALTER PARTITION FUNCTION pf_DailyReports()" in actual_query
    assert "SPLIT RANGE (20250207)" in actual_query


@patch('src.refresh_strategies.pyodbc')
def test_get_partition_number(mock_pyodbc, mock_handlers, partition_switch_config):
    from src.refresh_strategies import StagingPartitionSwitchStrategy
    
    source_handler, target_handler = mock_handlers
    
    target_handler.execute_query.return_value = [{'partition_number': 5}]
    
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, partition_switch_config)
    partition_num = strategy._get_partition_number(20250207)
    
    assert partition_num == 5
    target_handler.execute_query.assert_called_once()
    actual_query = target_handler.execute_query.call_args[0][0]
    assert "$PARTITION.pf_DailyReports(20250207)" in actual_query


@patch('src.refresh_strategies.pyodbc')
def test_partition_function_defaults(mock_pyodbc):
    from src.refresh_strategies import StagingPartitionSwitchStrategy
    
    config = TableConfig(
        name="TestTable",
        strategy="staging_partition_switch",
        sync_mode="incremental",
        incremental_column="report_date"
        # No partition_function specified
    )
    
    source_handler = Mock()
    target_handler = Mock()
    target_handler.execute_query.return_value = [{'partition_number': 3}]
    
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, config)
    partition_num = strategy._get_partition_number(20250207)
    
    actual_query = target_handler.execute_query.call_args[0][0]
    assert "$PARTITION.pf_TestTable(20250207)" in actual_query


@patch('src.refresh_strategies.pyodbc')
def test_get_strategy_simple_copy(mock_pyodbc, mock_handlers):
    from src.refresh_strategies import get_strategy, SimpleCopyStrategy
    
    source_handler, target_handler = mock_handlers
    config = TableConfig(name="Test", strategy="simple_copy", sync_mode="full_replace")
    
    strategy = get_strategy(source_handler, target_handler, config)
    
    assert isinstance(strategy, SimpleCopyStrategy)


@patch('src.refresh_strategies.pyodbc')
def test_get_strategy_staging_partition_switch(mock_pyodbc, mock_handlers):
    from src.refresh_strategies import get_strategy, StagingPartitionSwitchStrategy
    
    source_handler, target_handler = mock_handlers
    config = TableConfig(name="Test", strategy="staging_partition_switch", sync_mode="incremental")
    
    strategy = get_strategy(source_handler, target_handler, config)
    
    assert isinstance(strategy, StagingPartitionSwitchStrategy)


@patch('src.refresh_strategies.pyodbc')
def test_get_strategy_unknown(mock_pyodbc):
    from src.refresh_strategies import get_strategy
    from unittest.mock import Mock
    
    source_handler = Mock()
    target_handler = Mock()
    config = TableConfig(name="Test", strategy="unknown_strategy", sync_mode="full_replace")
    
    with pytest.raises(ValueError, match="Unknown strategy: unknown_strategy"):
        get_strategy(source_handler, target_handler, config)