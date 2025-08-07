# Data Refresh System

A Python-based data synchronization system that maintains UAT database environments by copying data from production SQL Server databases with configurable refresh strategies and partition management.

## Features

- **Multiple Sync Strategies**: Simple copy and staging partition switch for different table types
- **Flexible Sync Modes**: Full replace, incremental, and smart sync options
- **Automatic Partition Management**: Creates missing partitions for date-based partitioned tables
- **Web Dashboard**: Monitor table status and trigger refreshes via web interface
- **Configuration-Driven**: No hardcoded database or table references
- **Comprehensive Testing**: Full test suite with mocked dependencies

## Quick Start

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd data-refresh
```

2. Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Configure database credentials:
```bash
cp .env.example .env
# Edit .env with your database credentials
```

4. Update configuration:
```bash
# Edit config/config.yaml with your database and table settings
```

### Basic Usage

**Test database connections:**
```bash
python data_refresh.py --test-connections
```

**Check table status:**
```bash
python data_refresh.py --status
```

**Refresh a specific table:**
```bash
python data_refresh.py --table Users --force
```

**Refresh all tables:**
```bash
python data_refresh.py --force
```

**Start web interface:**
```bash
python web/app.py
# Navigate to http://localhost:5000
```

## Architecture

### Core Components

- **`src/config.py`** - Configuration management with YAML and environment variables
- **`src/database.py`** - Database connection handling and SQL operations
- **`src/refresh_strategies.py`** - Strategy pattern for different sync approaches
- **`src/data_refresh.py`** - Main service orchestrating refreshes
- **`web/app.py`** - Flask web interface for monitoring and control

### Refresh Strategies

#### Simple Copy Strategy
Best for small to medium tables without complex indexing:
- **Full Replace**: Truncate target and copy all source data
- **Incremental**: Copy only newer records based on ID or date columns
- **Smart Sync**: Analyze target first, choose full or incremental automatically

#### Staging Partition Switch Strategy
Optimized for large partitioned tables:
- Load data into staging table with bulk operations
- Apply indexes and constraints to staging table
- Use SQL Server partition switching for atomic data replacement
- Automatically creates missing partitions for date-based schemes

## Configuration

### Database Configuration (`config/config.yaml`)

```yaml
databases:
  source:
    server: "prod-sql-server"
    database: "ProductionDB"
    auth_type: "windows"  # or "sql"
  target:
    server: "uat-sql-server" 
    database: "UATDB"
    auth_type: "sql"

tables:
  # Static reference data
  - name: "Users"
    strategy: "simple_copy"
    sync_mode: "full_replace"
    truncate_target: true
    
  # Large table with identity-based sync
  - name: "Orders" 
    strategy: "simple_copy"
    sync_mode: "incremental"
    incremental_column: "order_id"
    incremental_type: "identity"
    
  # Partitioned reporting table
  - name: "DailyReports"
    strategy: "staging_partition_switch"
    sync_mode: "incremental" 
    incremental_column: "report_date"
    incremental_type: "date"
    date_buffer_days: 7
    batch_size: 10000
    partition_function: "pf_DailyReports"
    partition_scheme: "ps_DailyReports"

settings:
  default_batch_size: 5000
  connection_timeout: 30
  command_timeout: 300
  max_retries: 3
```

### Environment Variables (`.env`)

```bash
# Database credentials - never in config files
SOURCE_DB_USER=readonly_user
SOURCE_DB_PASSWORD=secure_password
TARGET_DB_USER=refresh_user  
TARGET_DB_PASSWORD=secure_password
```

### Configuration Options

#### Table Configuration Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| `name` | Table name | ✅ | - |
| `strategy` | `simple_copy` or `staging_partition_switch` | ✅ | - |
| `sync_mode` | `full_replace`, `incremental`, or `smart_sync` | ✅ | - |
| `incremental_column` | Column for incremental sync | For incremental | - |
| `incremental_type` | `identity`, `date`, or `datetime` | For incremental | - |
| `truncate_target` | Truncate before full refresh | ❌ | `false` |
| `date_buffer_days` | Re-sync recent days to catch updates | ❌ | `0` |
| `batch_size` | Rows per batch operation | ❌ | `5000` |
| `partition_function` | SQL Server partition function name | For partitioned | `pf_{table_name}` |
| `partition_scheme` | SQL Server partition scheme name | For partitioned | `ps_{table_name}` |

#### Sync Mode Behaviors

- **`full_replace`**: Always truncate target and copy all source data
- **`incremental`**: Copy only records newer than max value in target
- **`smart_sync`**: Check if target is empty, then choose full or incremental

## Partition Management

The system automatically handles date-based partitioned tables:

### Partition Date Formats
Supports multiple input formats, converts to `YYYYMMDD` integer:
- `datetime` objects: `datetime(2025, 2, 7)` → `20250207`
- Integer format: `20250207` → `20250207`
- String format: `"20250207"` → `20250207`

### Automatic Partition Creation
1. Scans incoming data for unique partition dates
2. Queries existing partitions in target database
3. Creates missing partitions using `ALTER PARTITION FUNCTION...SPLIT RANGE`
4. Logs all partition operations for audit trail

### Example Workflow
For table with new data for dates `20250207` and `20250208`:
1. Check existing partitions: finds `20250206` exists
2. Create missing partitions: `20250207`, `20250208`
3. Load data into staging table
4. Apply indexes to staging table
5. Atomically switch partitions
6. Clean up staging resources

## Web Interface

The Flask web dashboard provides:

- **Connection Status**: Real-time database connectivity monitoring
- **Table Overview**: Row counts, sync status, and max values for all tables
- **Manual Triggers**: Refresh individual tables or all tables
- **Operation History**: View refresh results and timing
- **Error Reporting**: Detailed error messages and troubleshooting

Access at `http://localhost:5000` after starting with `python web/app.py`.

## Development

### Running Tests

```bash
# Install test dependencies (already in requirements.txt)
pip install pytest flake8 black mypy

# Run all tests
pytest tests/ -v

# Run specific test files
pytest tests/test_config.py -v
pytest tests/test_integration.py -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

### Code Quality

```bash
# Linting
flake8 src/ --max-line-length=120

# Code formatting
black src/ --line-length=120

# Type checking
mypy src/
```

### Project Structure

```
data-refresh/
├── src/                     # Main application code
│   ├── config.py           # Configuration management
│   ├── database.py         # Database operations
│   ├── refresh_strategies.py # Sync strategy implementations
│   └── data_refresh.py     # Main service
├── web/                    # Flask web interface
│   ├── app.py             # Web application
│   ├── templates/         # HTML templates
│   └── static/           # CSS/JS assets
├── tests/                 # Test suite
│   ├── test_config.py    # Configuration tests
│   └── test_integration.py # Integration tests
├── config/               # Configuration files
│   ├── config.yaml      # Main configuration
│   └── config-test.yaml # Test configuration
├── data_refresh.py      # CLI entry point
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

## Deployment

### Prerequisites

- Python 3.8+
- SQL Server ODBC Driver 17
- Network access to source and target databases
- Appropriate database permissions

### Production Setup

1. **Install ODBC Driver**:
```bash
# Ubuntu/Debian
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install msodbcsql17
```

2. **Configure Environment**:
```bash
# Production environment variables
export SOURCE_DB_USER=prod_reader
export SOURCE_DB_PASSWORD=secure_password
export TARGET_DB_USER=uat_writer
export TARGET_DB_PASSWORD=secure_password
```

3. **Schedule Regular Refreshes**:
```bash
# Crontab example - daily refresh at 2 AM
0 2 * * * /path/to/venv/bin/python /path/to/data_refresh.py --force
```

### Docker Deployment

```dockerfile
FROM python:3.11-slim

# Install ODBC driver
RUN apt-get update && apt-get install -y curl gnupg2
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
CMD ["python", "data_refresh.py", "--force"]
```

## Troubleshooting

### Common Issues

**Connection Failures**:
- Verify ODBC driver installation
- Check network connectivity to databases
- Validate credentials in `.env` file
- Ensure appropriate database permissions

**Partition Errors**:
- Verify partition function exists in target database
- Check partition scheme configuration
- Ensure partition column data types match
- Review SQL Server partition function syntax

**Performance Issues**:
- Adjust `batch_size` for large tables
- Consider using `staging_partition_switch` for large tables
- Monitor connection timeout settings
- Review database server resources

### Logging

Configure logging levels in your environment:
```python
import logging
logging.basicConfig(level=logging.DEBUG)  # For detailed logs
logging.basicConfig(level=logging.INFO)   # For normal operation
```

## Security Considerations

- Store database credentials in environment variables, never in code
- Use read-only connections for production database access
- Implement proper user authentication for web interface
- Monitor and log all data refresh operations
- Consider network security between environments
- Regularly rotate database credentials

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Write tests for new functionality
4. Ensure all tests pass: `pytest tests/`
5. Run code quality checks: `flake8 src/` and `black src/`
6. Submit a pull request

## License

[Add your license information here]

## Support

For questions or issues:
- Create an issue in the repository
- Review the troubleshooting section
- Check the configuration documentation
- Examine log files for error details