# MSSQLDBSink - Project Summary

## 📁 Project Structure

```
MSSQLDBSink/
├── Program.cs                      # Main entry point and CLI argument handling
├── DatabaseSyncService.cs          # Core sync logic and database operations
├── AzureAdConnection.cs            # Azure AD Interactive connection logic
├── SyncRunResult.cs                # Result models for JSON reporting
├── MSSQLDBSink.csproj            # Project file with dependencies
├── README.md                       # Comprehensive documentation
├── .gitignore                      # Git ignore file
├── run-sync.bat                    # Windows batch script for easy execution
├── run-sync.ps1                    # PowerShell script for easy execution
└── setup-env.example.bat          # Environment variable setup template
```

## 🚀 Quick Start

### 1. Build the Project
```bash
cd MSSQLDBSink
dotnet build
```

### 2. Run the Sync

**Sync a single table:**
```bash
dotnet run -- "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users"
```

**Sync all tables:**
```bash
dotnet run -- "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB"
```

*Note: You will be prompted for Azure AD credentials when using server/database names.*

## 🔑 Key Features

### 1. **Smart Record Detection**
   - Uses primary keys to identify records
   - Only inserts records that don't exist in target
   - Skips existing records automatically
   - Count optimization: skips tables where target >= source

### 2. **Modern Authentication**
   - Uses **Azure Active Directory Interactive** authentication (when using server/db names)
   - Supports Multi-Factor Authentication (MFA)
   - Supports full connection strings for custom authentication

### 3. **Batch Processing**
   - Configurable batch size (default: 100,000)
   - Optimized for large datasets
   - Memory efficient

### 4. **Parallel Processing**
   - Multi-threaded table syncing
   - Configurable thread count
   - Thread-safe result tracking

### 5. **Connection Resiliency**
   - Automatic retry with exponential backoff
   - Handles transient SQL errors
   - Connection recovery on failures

### 6. **Bulk Insert Mode**
   - Fast truncate-and-replace option
   - Uses SqlBulkCopy for maximum performance
   - Bypasses row-by-row comparison

### 7. **No-PK Support**
   - Optional support for tables without primary keys
   - Deep comparison using all columns
   - Requires explicit flags for safety

### 8. **Progress Tracking & Resume**
   - Real-time console output
   - JSON result files with full run details
   - Enables manual resume by identifying failed tables
   - Optional primary key ordering for consistent continuation (`--order-by-pk`)

## 📦 Dependencies

- **Microsoft.Data.SqlClient** (6.1.3) - SQL Server connectivity
- **Dapper** (2.1.66) - Lightweight ORM for efficient queries
- **McMaster.Extensions.CommandLineUtils** (4.1.1) - Command-line parsing
- **Polly** (8.6.5) - Resilience and retry policies
- **System.Configuration.ConfigurationManager** (10.0.1) - Configuration support

## 🛠️ Technical Details

### How It Works

1. **Connection Phase**
   - Establishes connections using Azure AD or connection strings
   - Enforces read-only intent (`ApplicationIntent=ReadOnly`) on source connection
   - Validates connectivity with retry logic

2. **Discovery Phase**
   - Identifies tables to sync based on selection criteria
   - Retrieves primary key columns (or uses all columns for deep compare)
   - Counts records in both databases
   - Optionally orders source data by primary keys when `--order-by-pk` is enabled

3. **Optimization Phase**
   - Compares source and target counts
   - Skips tables where target >= source (unless `--clear-target`)

4. **Sync Phase**
   - **Normal Mode**: Fetches records in batches, checks existence, inserts missing
   - **Clear Target Mode**: Truncates target, bulk inserts all source records
   - Updates progress and result tracking in real-time

5. **Completion Phase**
   - Displays summary statistics
   - Saves JSON result file with all run details
   - Reports timing and performance

### Performance Characteristics

- **Time Complexity**: O(n) where n = source records
- **Space Complexity**: O(b) where b = batch size
- **Network Efficiency**: Batched operations reduce round trips
- **Database Load**: Read-heavy on source, write-heavy on target
- **Parallelism**: Table-level parallelism for multi-table syncs

## ⚙️ Configuration Options

### Batch Size Guidelines

| Record Size | Network Speed | Recommended Batch Size |
|------------|---------------|----------------------|
| Small      | Fast          | 2000-5000           |
| Small      | Slow          | 500-1000            |
| Large      | Fast          | 500-1000            |
| Large      | Slow          | 100-500             |

### Thread Count Guidelines

- **1 thread**: Default, safe for all scenarios
- **2-4 threads**: Good for multiple small tables
- **4+ threads**: Use with caution, monitor database connection limits

## 🔒 Security Best Practices

1. **Use Azure AD Authentication** (default when using server/db names) - avoids handling passwords
2. **Use connection strings** for custom authentication scenarios
3. **Use environment variables** for sensitive data if automating
4. **Use least-privilege accounts** (SELECT on source, INSERT on target)
5. **Enable firewall rules** to restrict database access
6. **Store credentials securely** - never commit connection strings to source control

## 📊 Example Output

```
MS SQL DB Sink - Database Record Sync Tool
===========================================

Source: source.database.windows.net (SourceDB)
Target: target.database.windows.net (TargetDB)
Batch Size: 1000
Parallel Threads: 2

Fetching list of tables from source database...
Found 3 tables to sync.

Syncing table: dbo.Users
  Table: dbo.Users | Keys: UserId | Source: 15,234 | Target: 12,100
  > dbo.Users: 15,234/15,234
  ✓ dbo.Users Completed in 12.45s - Inserted: 3,134, Skipped: 12,100

✓ Sync completed successfully!

Results saved to: results/sync-result-20251211_143022.json
```

## 🚨 Important Notes

### Requirements
- ✅ Target database must exist
- ✅ Target tables must exist with same schema
- ✅ Tables typically need primary keys (or use `--allow-no-pk --deep-compare`)
- ✅ User must have SELECT on source, INSERT on target

### Limitations
- ❌ Does NOT update existing records (unless `--clear-target`)
- ❌ Does NOT delete records from target (unless `--clear-target`)
- ❌ Does NOT modify table schemas
- ❌ Skips tables without primary keys by default
- ❌ Skips tables where target count >= source count (unless `--clear-target`)

### When to Use
- ✅ One-way synchronization (source → target)
- ✅ Incremental data migration
- ✅ Disaster recovery scenarios
- ✅ Dev/Test environment seeding
- ✅ Data warehouse loading
- ✅ Full table refresh (with `--clear-target`)

### When NOT to Use
- ❌ Two-way synchronization needed
- ❌ Record updates required (without `--clear-target`)
- ❌ Complex data transformations
- ❌ Real-time replication needed
- ❌ Schema changes involved

## 🐛 Troubleshooting

### Issue: "Table has no primary key"
**Solution**: Use `--allow-no-pk --deep-compare` to sync tables without PKs

### Issue: "Target has same or more records. Skipping..."
**Solution**: This is expected behavior. Use `--clear-target` if you want to replace all data.

### Issue: "Permission denied"
**Solution**: Grant SELECT on source and INSERT on target. For `--clear-target`, also grant DELETE/TRUNCATE.

### Issue: "Connection timeout"
**Solution**: Check firewall rules and whitelist your IP

## 📝 License

This project is licensed under a custom license. It is free for non-commercial use but cannot be included in other projects or redistributed.

## 🤝 Contributing

Contributions welcome! Feel free to submit issues or pull requests.

---

**Created**: December 2025
**Version**: 2.0.0
**Target Framework**: .NET 10.0
