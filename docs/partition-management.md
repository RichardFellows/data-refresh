# Partition Management and Cleanup Guide

This document covers advanced partition management strategies for SQL Server partitioned tables, specifically addressing partition boundary cleanup to prevent indefinite growth of partition functions.

## The Partition Boundary Growth Problem

When implementing daily partitioned tables with automated partition creation and data retention policies, a common issue arises: partition functions accumulate boundary values indefinitely.

### Problem Example
```sql
-- After months of daily partitions, you get this:
CREATE PARTITION FUNCTION pf_DailyReports (INT)
AS RANGE RIGHT 
FOR VALUES (20240101, 20240102, 20240103, ..., 20250207, 20250208, ...);
-- Thousands of unused boundaries accumulate over time!
```

**Impact:**
- Increased metadata storage requirements
- Slower partition function operations
- Management complexity
- Potential performance degradation

## Solution Strategies

### 1. Sliding Window Approach (Recommended)

Create a maintenance process that removes old partition boundaries when they're no longer needed.

```sql
-- Example: Remove partition boundaries older than 90 days
DECLARE @OldDate INT = CONVERT(INT, FORMAT(DATEADD(day, -90, GETDATE()), 'yyyyMMdd'));
DECLARE @BoundaryToRemove INT;

-- Find boundaries to remove
DECLARE boundary_cursor CURSOR FOR
SELECT CAST(value AS INT) as boundary_date
FROM sys.partition_range_values prv
INNER JOIN sys.partition_functions pf ON prv.function_id = pf.function_id
WHERE pf.name = 'pf_DailyReports' 
AND CAST(value AS INT) < @OldDate
ORDER BY CAST(value AS INT);

OPEN boundary_cursor;
FETCH NEXT FROM boundary_cursor INTO @BoundaryToRemove;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Merge the empty partition boundary
    ALTER PARTITION FUNCTION pf_DailyReports() MERGE RANGE (@BoundaryToRemove);
    PRINT 'Removed partition boundary: ' + CAST(@BoundaryToRemove AS VARCHAR(8));
    
    FETCH NEXT FROM boundary_cursor INTO @BoundaryToRemove;
END;

CLOSE boundary_cursor;
DEALLOCATE boundary_cursor;
```

### 2. Enhanced Data Refresh System Integration

Integration with the data refresh system to automatically manage partition cleanup:

```python
# Add to src/refresh_strategies.py
def _cleanup_old_partitions(self, retention_days: int = 90) -> List[int]:
    """Remove partition boundaries older than retention period"""
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    cutoff_int = int(cutoff_date.strftime('%Y%m%d'))
    
    # Get old boundaries
    query = f"""
    SELECT CAST(prv.value AS INT) as boundary_date
    FROM sys.partition_range_values prv
    INNER JOIN sys.partition_functions pf ON prv.function_id = pf.function_id
    WHERE pf.name = '{self.table_config.partition_function or f"pf_{self.table_config.name}"}'
    AND CAST(prv.value AS INT) < {cutoff_int}
    ORDER BY CAST(prv.value AS INT)
    """
    
    old_boundaries = self.target_handler.execute_query(query)
    removed_boundaries = []
    
    for boundary in old_boundaries:
        boundary_date = boundary['boundary_date']
        try:
            # Check if partition is empty before removing
            if self._is_partition_empty(boundary_date):
                merge_query = f"""
                ALTER PARTITION FUNCTION {self.table_config.partition_function or f"pf_{self.table_config.name}"}() 
                MERGE RANGE ({boundary_date})
                """
                self.target_handler.execute_non_query(merge_query)
                removed_boundaries.append(boundary_date)
                logger.info(f"Removed empty partition boundary: {boundary_date}")
        except Exception as e:
            logger.warning(f"Could not remove partition boundary {boundary_date}: {e}")
    
    return removed_boundaries
```

### 3. Configuration Enhancement

Add partition cleanup options to the YAML config:

```yaml
tables:
  - name: "DailyReports"
    strategy: "staging_partition_switch"
    sync_mode: "incremental"
    incremental_column: "report_date"
    incremental_type: "date"
    date_buffer_days: 7
    batch_size: 10000
    partition_function: "pf_DailyReports"
    partition_scheme: "ps_DailyReports"
    # New partition cleanup options
    partition_cleanup:
      enabled: true
      retention_days: 90
      cleanup_frequency: "weekly"  # daily, weekly, monthly
```

### 4. Automated Cleanup Stored Procedure

Create a dedicated stored procedure for regular cleanup:

```sql
CREATE PROCEDURE sp_CleanupPartitionBoundaries
    @TableName NVARCHAR(128),
    @RetentionDays INT = 90
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @PartitionFunction NVARCHAR(128) = 'pf_' + @TableName;
    DECLARE @OldDate INT = CONVERT(INT, FORMAT(DATEADD(day, -@RetentionDays, GETDATE()), 'yyyyMMdd'));
    DECLARE @BoundaryToRemove INT;
    DECLARE @PartitionNumber INT;
    DECLARE @RowCount BIGINT;
    
    DECLARE boundary_cursor CURSOR FOR
    SELECT CAST(value AS INT) as boundary_date
    FROM sys.partition_range_values prv
    INNER JOIN sys.partition_functions pf ON prv.function_id = pf.function_id
    WHERE pf.name = @PartitionFunction
    AND CAST(value AS INT) < @OldDate
    ORDER BY CAST(value AS INT);
    
    OPEN boundary_cursor;
    FETCH NEXT FROM boundary_cursor INTO @BoundaryToRemove;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Get partition number for this boundary
        SELECT @PartitionNumber = $PARTITION.pf_DailyReports(@BoundaryToRemove);
        
        -- Check if partition is empty
        DECLARE @SQL NVARCHAR(MAX) = N'SELECT @Count = COUNT(*) FROM ' + QUOTENAME(@TableName) + 
                                     N' WHERE $PARTITION.' + @PartitionFunction + N'(report_date) = ' + CAST(@PartitionNumber AS NVARCHAR(10));
        
        EXEC sp_executesql @SQL, N'@Count BIGINT OUTPUT', @Count = @RowCount OUTPUT;
        
        IF @RowCount = 0
        BEGIN
            -- Safe to remove empty partition boundary
            EXEC('ALTER PARTITION FUNCTION ' + @PartitionFunction + '() MERGE RANGE (' + @BoundaryToRemove + ')');
            PRINT 'Removed empty partition boundary: ' + CAST(@BoundaryToRemove AS VARCHAR(8));
        END
        ELSE
        BEGIN
            PRINT 'Skipped non-empty partition boundary: ' + CAST(@BoundaryToRemove AS VARCHAR(8)) + ' (Rows: ' + CAST(@RowCount AS VARCHAR(20)) + ')';
        END
        
        FETCH NEXT FROM boundary_cursor INTO @BoundaryToRemove;
    END;
    
    CLOSE boundary_cursor;
    DEALLOCATE boundary_cursor;
END;
```

### 5. Pre-Allocated Rolling Window

Instead of adding boundaries daily, pre-allocate a rolling window:

```sql
-- Create a 2-year rolling window (today - 1 year to today + 1 year)
DECLARE @StartDate DATE = DATEADD(year, -1, GETDATE());
DECLARE @EndDate DATE = DATEADD(year, 1, GETDATE());
DECLARE @CurrentDate DATE = @StartDate;
DECLARE @Values NVARCHAR(MAX) = '';

WHILE @CurrentDate <= @EndDate
BEGIN
    SET @Values = @Values + CAST(CONVERT(INT, FORMAT(@CurrentDate, 'yyyyMMdd')) AS VARCHAR(8));
    IF @CurrentDate < @EndDate
        SET @Values = @Values + ', ';
    SET @CurrentDate = DATEADD(day, 1, @CurrentDate);
END;

-- Recreate partition function with rolling window
DROP PARTITION SCHEME ps_DailyReports;
DROP PARTITION FUNCTION pf_DailyReports;

EXEC('CREATE PARTITION FUNCTION pf_DailyReports (INT) AS RANGE RIGHT FOR VALUES (' + @Values + ')');

-- Recreate partition scheme
CREATE PARTITION SCHEME ps_DailyReports AS PARTITION pf_DailyReports TO ([PRIMARY], [PRIMARY], ...);
```

## Partition Switching Configuration Guide

### Prerequisites

Before configuring partition switching, ensure your SQL Server table has:

1. **Partitioned table structure** with a partition function and scheme
2. **Date-based partitioning** using `YYYYMMDD` integer format
3. **Proper indexes** that align with the partition scheme
4. **Sufficient permissions** for partition operations

### Database Setup Example

```sql
-- Example: Create partition function for daily partitions
CREATE PARTITION FUNCTION pf_DailyReports (INT)
AS RANGE RIGHT 
FOR VALUES (20250101, 20250102, 20250103, ...);

-- Create partition scheme
CREATE PARTITION SCHEME ps_DailyReports
AS PARTITION pf_DailyReports 
TO ([PRIMARY], [PRIMARY], [PRIMARY], ...);

-- Create partitioned table
CREATE TABLE DailyReports (
    report_date INT NOT NULL,
    amount DECIMAL(18,2),
    region VARCHAR(50),
    -- other columns...
    INDEX IX_DailyReports_Date (report_date)
) ON ps_DailyReports(report_date);
```

### Table Configuration

Configure your partitioned table in `config/config.yaml`:

```yaml
tables:
  - name: "DailyReports"
    strategy: "staging_partition_switch"
    sync_mode: "incremental"
    incremental_column: "report_date"
    incremental_type: "date"
    date_buffer_days: 7
    batch_size: 10000
    partition_function: "pf_DailyReports"
    partition_scheme: "ps_DailyReports"
```

### Configuration Parameters

| Parameter | Purpose | Example Value |
|-----------|---------|---------------|
| `strategy` | Must be `"staging_partition_switch"` | `"staging_partition_switch"` |
| `sync_mode` | How to determine what data to sync | `"incremental"` |
| `incremental_column` | Column containing partition dates | `"report_date"` |
| `incremental_type` | Data type of partition column | `"date"` |
| `date_buffer_days` | Days to re-sync for late updates | `7` |
| `batch_size` | Rows per batch operation | `10000` |
| `partition_function` | Name of SQL Server partition function | `"pf_DailyReports"` |
| `partition_scheme` | Name of SQL Server partition scheme | `"ps_DailyReports"` |

### Data Format Requirements

Your data must have the partition column in one of these formats:

```python
# Integer format (preferred)
{'report_date': 20250207, 'amount': 100.50}

# DateTime objects (auto-converted)
{'report_date': datetime(2025, 2, 7), 'amount': 100.50}

# String format (auto-converted)
{'report_date': '20250207', 'amount': 100.50}
```

## Refresh Process Workflow

### Step-by-Step Execution

1. **Data Analysis**: System scans incoming data for unique partition dates
2. **Partition Check**: Queries target database for existing partitions
3. **Auto-Creation**: Creates missing partitions using `ALTER PARTITION FUNCTION...SPLIT RANGE`
4. **Staging Load**: Bulk loads data into temporary staging table
5. **Index Application**: Copies indexes and constraints to staging table
6. **Atomic Switch**: Uses partition switching for zero-downtime replacement
7. **Cleanup**: Removes staging tables and temporary objects

### Example Workflow

For a table refresh with data for dates `20250207` and `20250208`:

```
┌─────────────────┐    ┌──────────────────┐
│ Check Existing  │───▶│ Found: 20250206  │
│ Partitions      │    │ Missing: 207,208 │
└─────────────────┘    └──────────────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐    ┌──────────────────┐
│ Create Missing  │    │ ALTER PARTITION  │
│ Partitions      │    │ FUNCTION SPLIT   │
└─────────────────┘    └──────────────────┘
         │
         ▼
┌─────────────────┐    ┌──────────────────┐
│ Load Staging    │───▶│ Bulk Insert      │
│ Table          │    │ 50K rows         │
└─────────────────┘    └──────────────────┘
         │
         ▼
┌─────────────────┐    ┌──────────────────┐
│ Switch          │───▶│ Atomic Replace   │
│ Partitions      │    │ Zero Downtime    │
└─────────────────┘    └──────────────────┘
```

## Advanced Configuration Options

### Buffer Days for Late Updates

```yaml
date_buffer_days: 7  # Re-sync last 7 days to catch late updates
```

This ensures late-arriving data is captured by re-syncing recent partitions.

### Custom Partition Function Names

```yaml
partition_function: "pf_CustomReports"  # Override default naming
partition_scheme: "ps_CustomReports"
```

If not specified, defaults to `pf_{table_name}` and `ps_{table_name}`.

### Performance Tuning

```yaml
batch_size: 25000  # Larger batches for better performance
```

Adjust based on your server capacity and network speed.

## Testing Your Configuration

### 1. Dry Run Test
```bash
python data_refresh.py --table DailyReports --dry-run
```

### 2. Connection Test
```bash
python data_refresh.py --test-connections
```

### 3. Status Check
```bash
python data_refresh.py --status --table DailyReports
```

### 4. Small Batch Test
```bash
# Add row_limit for testing
tables:
  - name: "DailyReports"
    strategy: "staging_partition_switch"
    row_limit: 1000  # Test with limited data
```

## Troubleshooting Common Issues

### Partition Function Not Found
```
Error: Could not retrieve existing partitions
```
**Solution**: Verify partition function exists and has correct name in config.

### Permission Errors
```
Error: ALTER PARTITION FUNCTION failed
```
**Solution**: Ensure database user has `ALTER` permissions on partition functions.

### Date Format Issues
```
Warning: Could not parse partition date
```
**Solution**: Verify your data uses supported date formats (YYYYMMDD int, datetime, or parseable strings).

### Missing Partitions
```
Info: Created partition for date 20250207
```
**This is normal**: System automatically creates missing partitions as needed.

## Monitoring and Logging

The system logs all partition operations:

```
INFO: Starting staging partition switch refresh for table DailyReports
INFO: Created partition for date 20250207 on table DailyReports
INFO: Successfully switched partition 15 for date 20250207
INFO: Refresh completed. Partitions created: [20250207, 20250208]
```

## Recommended Implementation Strategy

For production environments with daily partitioned tables:

1. **Implement sliding window cleanup** using the stored procedure approach
2. **Schedule cleanup weekly** to balance performance and maintenance overhead
3. **Monitor cleanup results** through logging and system alerts
4. **Set retention period** based on business requirements (typically 90-365 days)
5. **Test cleanup process** in non-production environments first

### Sample Cleanup Schedule

```sql
-- Weekly cleanup job (SQL Server Agent)
EXEC sp_CleanupPartitionBoundaries 
    @TableName = 'DailyReports',
    @RetentionDays = 90;
    
EXEC sp_CleanupPartitionBoundaries 
    @TableName = 'TransactionLog',
    @RetentionDays = 365;
```

This approach maintains optimal partition function performance while preserving the benefits of automated partition switching for high-volume data processing.

## Best Practices

1. **Always verify partitions are empty** before removing boundaries
2. **Implement proper error handling** for cleanup operations
3. **Log all partition maintenance activities** for audit trails
4. **Test partition operations** in development environments first
5. **Monitor partition function metadata size** regularly
6. **Coordinate cleanup with data retention policies**
7. **Consider business requirements** when setting retention periods

By implementing these strategies, you can maintain efficient partition switching while preventing unbounded growth of partition function metadata.