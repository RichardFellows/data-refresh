import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from src.refresh_strategies import SimpleCopyStrategy, StagingPartitionSwitchStrategy, get_strategy
from src.config import TableConfig
from src.database import DatabaseHandler


@pytest.fixture
def mock_handlers():
    source_handler = Mock(spec=DatabaseHandler)
    target_handler = Mock(spec=DatabaseHandler)
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


def test_full_replace_strategy(mock_handlers, full_replace_config):
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


def test_incremental_strategy_with_existing_data(mock_handlers, incremental_config):
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


def test_incremental_strategy_no_existing_data(mock_handlers, incremental_config):
    source_handler, target_handler = mock_handlers
    
    target_handler.get_max_value.return_value = None
    
    test_data = [
        {'id': 1, 'name': 'Test1'},
        {'id': 2, 'name': 'Test2'}
    ]
    
    source_handler.execute_query.return_value = test_data
    target_handler.bulk_insert.return_value = 2
    
    strategy = SimpleCopyStrategy(source_handler, target_handler, incremental_config)
    result = strategy.refresh_table()
    
    source_handler.execute_query.assert_called_once_with("SELECT * FROM TestTable")
    assert result['sync_mode'] == "full_replace"


def test_smart_sync_empty_target(mock_handlers, smart_sync_config):
    source_handler, target_handler = mock_handlers
    
    target_handler.get_table_count.return_value = 0
    
    test_data = [{'id': 1, 'name': 'Test1'}]
    source_handler.execute_query.return_value = test_data
    target_handler.bulk_insert.return_value = 1
    
    strategy = SimpleCopyStrategy(source_handler, target_handler, smart_sync_config)
    result = strategy.refresh_table()
    
    target_handler.get_table_count.assert_called_once_with("TestTable")
    assert result['sync_mode'] == "smart_sync_full"


def test_smart_sync_existing_target(mock_handlers, smart_sync_config):
    source_handler, target_handler = mock_handlers
    
    target_handler.get_table_count.return_value = 100
    target_handler.get_max_value.return_value = datetime.now()
    
    test_data = []
    source_handler.execute_query.return_value = test_data
    target_handler.bulk_insert.return_value = 0
    
    strategy = SimpleCopyStrategy(source_handler, target_handler, smart_sync_config)
    result = strategy.refresh_table()
    
    assert result['sync_mode'] == "smart_sync_incremental"


def test_date_buffer_where_clause():
    source_handler = Mock(spec=DatabaseHandler)
    target_handler = Mock(spec=DatabaseHandler)
    
    config = TableConfig(
        name="TestTable",
        strategy="simple_copy",
        sync_mode="incremental",
        incremental_column="report_date",
        incremental_type="date",
        date_buffer_days=7
    )
    
    strategy = SimpleCopyStrategy(source_handler, target_handler, config)
    max_date = datetime(2023, 1, 15).date()
    
    where_clause = strategy._build_incremental_where_clause(max_date)
    expected_date = max_date - timedelta(days=7)
    
    assert where_clause == f"report_date >= '{expected_date}'"


def test_get_strategy_simple_copy(mock_handlers):
    source_handler, target_handler = mock_handlers
    config = TableConfig(name="Test", strategy="simple_copy", sync_mode="full_replace")
    
    strategy = get_strategy(source_handler, target_handler, config)
    
    assert isinstance(strategy, SimpleCopyStrategy)


def test_get_strategy_staging_partition_switch(mock_handlers):
    source_handler, target_handler = mock_handlers
    config = TableConfig(name="Test", strategy="staging_partition_switch", sync_mode="incremental")
    
    strategy = get_strategy(source_handler, target_handler, config)
    
    assert isinstance(strategy, StagingPartitionSwitchStrategy)


def test_get_strategy_unknown():
    source_handler = Mock(spec=DatabaseHandler)
    target_handler = Mock(spec=DatabaseHandler)
    config = TableConfig(name="Test", strategy="unknown_strategy", sync_mode="full_replace")
    
    with pytest.raises(ValueError, match="Unknown strategy: unknown_strategy"):
        get_strategy(source_handler, target_handler, config)


def test_partition_switch_strategy_basic(mock_handlers, partition_switch_config):
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


def test_get_required_partitions_from_datetime():
    source_handler = Mock(spec=DatabaseHandler)
    target_handler = Mock(spec=DatabaseHandler)
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


def test_get_required_partitions_from_int():
    source_handler = Mock(spec=DatabaseHandler)
    target_handler = Mock(spec=DatabaseHandler)
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


def test_get_required_partitions_empty_data():
    source_handler = Mock(spec=DatabaseHandler)
    target_handler = Mock(spec=DatabaseHandler)
    config = TableConfig(
        name="TestTable",
        strategy="staging_partition_switch",
        sync_mode="incremental",
        incremental_column="report_date"
    )
    
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, config)
    
    partitions = strategy._get_required_partitions([])
    assert partitions == []


def test_get_existing_partitions(mock_handlers, partition_switch_config):
    source_handler, target_handler = mock_handlers
    
    target_handler.execute_query.return_value = [
        {'partition_value': 20250205},
        {'partition_value': 20250206}
    ]
    
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, partition_switch_config)
    existing_partitions = strategy._get_existing_partitions()
    
    assert existing_partitions == [20250205, 20250206]
    target_handler.execute_query.assert_called_once()


def test_ensure_partitions_exist(mock_handlers, partition_switch_config):
    source_handler, target_handler = mock_handlers
    
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, partition_switch_config)
    
    with patch.object(strategy, '_get_existing_partitions', return_value=[20250206]):
        with patch.object(strategy, '_create_partition') as mock_create:
            created = strategy._ensure_partitions_exist([20250206, 20250207, 20250208])
            
            assert created == [20250207, 20250208]
            assert mock_create.call_count == 2
            mock_create.assert_any_call(20250207)
            mock_create.assert_any_call(20250208)


def test_create_partition(mock_handlers, partition_switch_config):
    source_handler, target_handler = mock_handlers
    
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, partition_switch_config)
    strategy._create_partition(20250207)
    
    expected_query = """
        ALTER PARTITION FUNCTION pf_DailyReports()
        SPLIT RANGE (20250207)
        """
    
    target_handler.execute_non_query.assert_called_once()
    actual_query = target_handler.execute_non_query.call_args[0][0]
    assert "ALTER PARTITION FUNCTION pf_DailyReports()" in actual_query
    assert "SPLIT RANGE (20250207)" in actual_query


def test_get_partition_number(mock_handlers, partition_switch_config):
    source_handler, target_handler = mock_handlers
    
    target_handler.execute_query.return_value = [{'partition_number': 5}]
    
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, partition_switch_config)
    partition_num = strategy._get_partition_number(20250207)
    
    assert partition_num == 5
    expected_query = """
        SELECT 
            $PARTITION.pf_DailyReports(20250207) as partition_number
        """
    target_handler.execute_query.assert_called_once()
    actual_query = target_handler.execute_query.call_args[0][0]
    assert "$PARTITION.pf_DailyReports(20250207)" in actual_query


def test_partition_function_defaults(mock_handlers):
    config = TableConfig(
        name="TestTable",
        strategy="staging_partition_switch",
        sync_mode="incremental",
        incremental_column="report_date"
        # No partition_function specified
    )
    
    source_handler, target_handler = mock_handlers
    target_handler.execute_query.return_value = [{'partition_number': 3}]
    
    strategy = StagingPartitionSwitchStrategy(source_handler, target_handler, config)
    partition_num = strategy._get_partition_number(20250207)
    
    actual_query = target_handler.execute_query.call_args[0][0]
    assert "$PARTITION.pf_TestTable(20250207)" in actual_query