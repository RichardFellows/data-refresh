import pytest
from unittest.mock import Mock, patch, MagicMock
from src.database import DatabaseConnection, DatabaseHandler
from src.config import DatabaseConfig, Settings


@pytest.fixture
def windows_db_config():
    return DatabaseConfig(
        server="test-server",
        database="TestDB",
        auth_type="windows"
    )


@pytest.fixture
def sql_db_config():
    return DatabaseConfig(
        server="test-server",
        database="TestDB",
        auth_type="sql",
        user="test_user",
        password="test_pass"
    )


@pytest.fixture
def settings():
    return Settings(
        default_batch_size=1000,
        connection_timeout=30,
        command_timeout=300,
        max_retries=3
    )


def test_windows_connection_string(windows_db_config, settings):
    connection = DatabaseConnection(windows_db_config, settings)
    
    expected = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=test-server;"
        "DATABASE=TestDB;"
        "Trusted_Connection=yes;"
        "Connection Timeout=30;"
    )
    
    assert connection._connection_string == expected


def test_sql_connection_string(sql_db_config, settings):
    connection = DatabaseConnection(sql_db_config, settings)
    
    expected = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=test-server;"
        "DATABASE=TestDB;"
        "UID=test_user;"
        "PWD=test_pass;"
        "Connection Timeout=30;"
    )
    
    assert connection._connection_string == expected


@patch('src.database.pyodbc.connect')
def test_test_connection_success(mock_connect, windows_db_config, settings):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    connection = DatabaseConnection(windows_db_config, settings)
    result = connection.test_connection()
    
    assert result is True
    mock_connect.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT 1")


@patch('src.database.pyodbc.connect')
def test_test_connection_failure(mock_connect, windows_db_config, settings):
    mock_connect.side_effect = Exception("Connection failed")
    
    connection = DatabaseConnection(windows_db_config, settings)
    result = connection.test_connection()
    
    assert result is False


@patch('src.database.pyodbc.connect')
def test_database_handler_execute_query(mock_connect, windows_db_config, settings):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    mock_cursor.description = [('col1',), ('col2',)]
    mock_cursor.fetchall.return_value = [('val1', 'val2'), ('val3', 'val4')]
    
    connection = DatabaseConnection(windows_db_config, settings)
    handler = DatabaseHandler(connection)
    
    result = handler.execute_query("SELECT * FROM test_table")
    
    expected = [
        {'col1': 'val1', 'col2': 'val2'},
        {'col1': 'val3', 'col2': 'val4'}
    ]
    
    assert result == expected
    mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table", ())


@patch('src.database.pyodbc.connect')
def test_database_handler_execute_non_query(mock_connect, windows_db_config, settings):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.rowcount = 5
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    connection = DatabaseConnection(windows_db_config, settings)
    handler = DatabaseHandler(connection)
    
    result = handler.execute_non_query("DELETE FROM test_table WHERE id = 1")
    
    assert result == 5
    mock_cursor.execute.assert_called_once_with("DELETE FROM test_table WHERE id = 1", ())
    mock_conn.commit.assert_called_once()


@patch('src.database.pyodbc.connect')
def test_get_max_value(mock_connect, windows_db_config, settings):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    mock_cursor.description = [('max_value',)]
    mock_cursor.fetchall.return_value = [(100,)]
    
    connection = DatabaseConnection(windows_db_config, settings)
    handler = DatabaseHandler(connection)
    
    result = handler.get_max_value("test_table", "id")
    
    assert result == 100
    mock_cursor.execute.assert_called_once_with("SELECT MAX(id) as max_value FROM test_table", ())


@patch('src.database.pyodbc.connect')
def test_get_table_count(mock_connect, windows_db_config, settings):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    mock_cursor.description = [('count',)]
    mock_cursor.fetchall.return_value = [(50,)]
    
    connection = DatabaseConnection(windows_db_config, settings)
    handler = DatabaseHandler(connection)
    
    result = handler.get_table_count("test_table")
    
    assert result == 50
    mock_cursor.execute.assert_called_once_with("SELECT COUNT(*) as count FROM test_table", ())