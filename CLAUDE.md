# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based data refresh system that synchronizes data between two SQL Server databases:
- **Source**: Production database (read-only)
- **Target**: UAT database (write operations)

The system maintains UAT data up-to-date for testing purposes by copying data from production while preserving schema synchronization.

## Architecture

### Core Components
- **Data Refresh Script**: Main Python script for database synchronization
- **Web UI**: Basic interface for developers to view table data and trigger refreshes
- **Database Handlers**: Modules for SQL Server connectivity and operations
- **Configuration Management**: Database connection strings, table mappings, and refresh policies

### Data Loading Strategy
- **Simple Tables**: Direct copy operations
- **Complex Tables**: Use staging tables with partition switching for tables with:
  - Large number of indexes
  - Partition schemes
  - High performance requirements

## Development Setup

### Dependencies
```bash
pip install -r requirements.txt
```

### Database Configuration
The system is fully configurable via deployment settings:
- All database connections defined in configuration files
- Table mappings and sync rules externally configured
- No hardcoded database or table references in code
- Environment-specific configuration support

### Testing
```bash
python -m pytest tests/
python -m pytest tests/test_specific_module.py  # Single test file
python -m pytest -k "test_function_name"        # Specific test
```

### Code Quality
```bash
flake8 .                    # Linting
black .                     # Code formatting
mypy .                      # Type checking
```

## Configuration Structure

### Primary Configuration (config.yaml)
**Required structure - all deployments must follow this format:**
```yaml
databases:
  source:
    server: "prod-sql-server"
    database: "ProductionDB"
    auth_type: "windows"  # or "sql"
    # credentials via environment variables
  target:
    server: "uat-sql-server" 
    database: "UATDB"
    auth_type: "sql"
    # credentials via environment variables

tables:
  # Static reference data - full refresh
  - name: "Users"
    strategy: "simple_copy"
    sync_mode: "full_replace"
    truncate_target: true
    
  # Large table with identity-based incremental sync
  - name: "Orders" 
    strategy: "simple_copy"
    sync_mode: "incremental"
    incremental_column: "order_id"
    incremental_type: "identity"  # Uses MAX(order_id) from target
    
  # Reporting table with date-based incremental sync
  - name: "DailyReports"
    strategy: "staging_partition_switch"
    sync_mode: "incremental" 
    incremental_column: "report_date"
    incremental_type: "date"
    date_buffer_days: 7  # Re-sync last 7 days to catch updates
    batch_size: 10000
    
  # Mixed approach - check existing data first
  - name: "ProductCatalog"
    strategy: "simple_copy"
    sync_mode: "smart_sync"  # Check target, decide full vs incremental
    incremental_column: "last_modified"
    incremental_type: "datetime"
    fallback_to_full: true  # If target empty, do full sync

settings:
  default_batch_size: 5000
  connection_timeout: 30
  command_timeout: 300
  max_retries: 3
```

### Environment Variables (Required)
```bash
# Database credentials - never in config files
SOURCE_DB_USER=readonly_user
SOURCE_DB_PASSWORD=secure_password
TARGET_DB_USER=refresh_user  
TARGET_DB_PASSWORD=secure_password
```

### Testing Configuration Options
**config-test.yaml** - for development/testing:
```yaml
# Override production settings for testing
databases:
  source:
    server: "localhost"
    database: "TestSource"
  target:
    server: "localhost"
    database: "TestTarget"

tables:
  - name: "Users"
    strategy: "simple_copy"
    row_limit: 100  # Testing only - limits rows copied

settings:
  dry_run: true  # Testing only - shows what would be done
  verbose_logging: true
```

### Command Line Overrides (Optional)
```bash
python data_refresh.py --config config-test.yaml --dry-run --table Users
python data_refresh.py --config config.yaml --force  # Skip confirmations
```

### Sync Mode Behaviors

**full_replace**: Always truncate target and copy all source data
- Best for: Small static reference tables, lookup data
- Query: `SELECT * FROM source_table`

**incremental**: Check target for existing data, only copy newer records
- Identity columns: `SELECT MAX(order_id) FROM target` → `SELECT * FROM source WHERE order_id > {max_id}`
- Date columns: `SELECT MAX(report_date) FROM target` → `SELECT * FROM source WHERE report_date > {max_date}`
- DateTime with buffer: Re-sync recent data to catch updates

**smart_sync**: Analyze target first, choose appropriate strategy
- If target empty → full refresh
- If target has data → incremental based on configured column
- Fallback to full refresh if incremental fails

## Key Implementation Considerations

### Database Operations
- Use connection pooling for efficiency
- Implement proper transaction handling
- Handle large datasets with chunked processing
- Ensure proper error handling and rollback mechanisms

### Security
- Store database credentials securely (environment variables or key vault)
- Use read-only connections for production database
- Implement audit logging for data refresh operations

### Performance
- Implement partition switching for large tables
- Use bulk insert operations where possible
- Consider table-level locking strategies
- Monitor and log operation performance

### Web UI Requirements
- Display current data status for each table in both environments
- Provide trigger mechanism for manual refreshes
- Show refresh history and status
- Implement user authentication and authorization

## Table Refresh Patterns

### Standard Copy Pattern
For simple tables without complex indexing or partitioning.

### Staging + Partition Switch Pattern
For high-performance tables:
1. Load data into staging table
2. Apply indexes and constraints
3. Use partition switching to replace target table
4. Clean up staging resources