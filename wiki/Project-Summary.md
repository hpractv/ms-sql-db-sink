# MSSQLDBSink - Project Summary

## 📁 Project Structure

```
MSSQLDBSink/
├── src/
│   ├── MSSQLDBSink/
│   │   ├── Program.cs                      # Main entry point and CLI argument handling
│   │   ├── DatabaseSyncService.cs          # Core sync logic and database operations
│   │   ├── AzureAdConnection.cs            # Azure AD authentication logic
│   │   ├── SyncRunResult.cs                # Result models for JSON reporting
│   │   └── MSSQLDBSink.csproj            # Project file with dependencies
│   └── MSSQLDBSink.Tests/                  # Unit tests
├── wiki/                                   # Detailed documentation
│   ├── Project-Summary.md                  # This file
│   └── Usage-Guide.md                      # Comprehensive usage guide
├── README.md                               # Main documentation
├── QUICK_REFERENCE.md                      # Quick command reference
├── .gitignore                              # Git ignore file
├── run-sync.bat                            # Windows batch script for easy execution
├── run-sync.ps1                            # PowerShell script for easy execution
└── setup-env.example.bat                   # Environment variable setup template
```

## 🚀 Quick Start

### 1. Build and Publish the Project

#### Development Build
```bash
cd /path/to/MSSQLDBSink
dotnet build
```

#### Production Release
```bash
dotnet publish -c Release -o ./publish
cd publish
```

### 2. Run the Sync

**Using the compiled executable (Recommended):**
```bash
# After publishing
cd publish
./MSSQLDBSink "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users"
```

**Using the DLL:**
```bash
dotnet MSSQLDBSink.dll "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users"
```

**From source (Development):**
```bash
dotnet run --project src/MSSQLDBSink/MSSQLDBSink.csproj -- "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users"
```

*Note: You will be prompted for Azure AD credentials when using server/database names.*

## 🔑 Key Features

### 1. **Smart Record Detection**
   - Uses primary keys to identify records
   - Only inserts records that don't exist in target
   - Skips existing records automatically
   - Count optimization: skips tables where target >= source

### 2. **Modern Authentication**
   - Uses **Azure Active Directory Default** authentication (when using server/db names)
   - Supports Multi-Factor Authentication (MFA)
   - Supports full connection strings for custom authentication
   - Automatic fallback for local/on-prem SQL servers

### 3. **Batch Processing**
   - Configurable batch size (default: 100,000)
   - Optimized for large datasets
   - Memory efficient

### 4. **Parallel Processing**
   - Multi-threaded table syncing
   - Configurable thread count (default: 4)
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
   - Real-time console output with Spectre.Console
   - JSON result files with full run details
   - Enables manual resume by identifying failed tables

## 📦 Dependencies

- **Microsoft.Data.SqlClient** (6.1.3) - SQL Server connectivity
- **Dapper** (2.1.66) - Lightweight ORM for efficient queries
- **McMaster.Extensions.CommandLineUtils** (4.1.1) - Command-line parsing
- **Polly** (8.6.5) - Resilience and retry policies
- **Spectre.Console** (0.49.1) - Rich console output and progress bars
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

3. **Optimization Phase**
   - Compares source and target counts
   - Skips tables where target >= source (unless `--clear-target`)

4. **Sync Phase**
   - **Normal Mode**: Fetches records in batches, checks existence, inserts missing
   - **Clear Target Mode**: Truncates target, bulk inserts all source records
   - Updates progress and result tracking in real-time

5. **Completion Phase**
   - Displays summary statistics with Spectre.Console tables
   - Saves JSON result file with all run details
   - Reports timing and performance metrics

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

**Default**: 100,000 (suitable for most scenarios with fast networks)

### Thread Count Guidelines

- **1 thread**: Safe for all scenarios, sequential processing
- **2-4 threads**: Good for multiple small tables (default: 4)
- **4+ threads**: Use with caution, monitor database connection limits
- **Max recommended**: 8 threads (depending on server capacity)

## 🔒 Security Best Practices

1. **Use Azure AD Authentication** (default when using server/db names) - avoids handling passwords
2. **Use connection strings** for custom authentication scenarios
3. **Use environment variables** for sensitive data if automating
4. **Use least-privilege accounts** (SELECT on source, INSERT on target)
5. **Enable firewall rules** to restrict database access
6. **Store credentials securely** - never commit connection strings to source control
7. **Use Azure Key Vault** or similar secret management for production deployments

## 📊 Example Output

```
┌─────────────────────────────────────────────────────────────┐
│ MS SQL DB Sink - Database Record Sync Tool                 │
└─────────────────────────────────────────────────────────────┘

Source: source.database.windows.net (SourceDB)
Target: target.database.windows.net (TargetDB)
Batch Size: 100,000
Parallel Threads: 4

Fetching list of tables from source database...
Found 3 tables to sync.

┌──────────────┬──────────┬──────────┬──────────┬────────────┐
│ Table        │ Source   │ Target   │ Inserted │ Duration   │
├──────────────┼──────────┼──────────┼──────────┼────────────┤
│ dbo.Users    │ 15,234   │ 12,100   │ 3,134    │ 12.45s     │
│ dbo.Orders   │ 50,000   │ 48,000   │ 2,000    │ 8.30s      │
│ dbo.Products │ 1,200    │ 1,200    │ 0        │ 0.15s      │
└──────────────┴──────────┴──────────┴──────────┴────────────┘

✓ Sync completed successfully!

Results saved to: results/sync-result-20260108_143022.json
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

### Issue: "Azure AD authentication failed"
**Solution**: Run `az login` or use connection strings with SQL authentication

## 🔧 Building and Publishing

### Debug Build
```bash
dotnet build
# Output: src/MSSQLDBSink/bin/Debug/net10.0/
```

### Release Build
```bash
dotnet build -c Release
# Output: src/MSSQLDBSink/bin/Release/net10.0/
```

### Publish for Distribution
```bash
# Framework-dependent (requires .NET runtime)
dotnet publish -c Release -o ./publish

# Self-contained (includes .NET runtime)
dotnet publish -c Release -r win-x64 --self-contained -o ./publish-win
dotnet publish -c Release -r linux-x64 --self-contained -o ./publish-linux
dotnet publish -c Release -r osx-x64 --self-contained -o ./publish-mac
```

### Running Tests
```bash
dotnet test
```

## 📝 License

This project is licensed under a custom license. It is free for non-commercial use but cannot be included in other projects or redistributed.

## 🤝 Contributing

Contributions welcome! Feel free to submit issues or pull requests.

---

**Version**: 2.0.0  
**Target Framework**: .NET 10.0  
**Repository**: https://github.com/hpractv/ms-sql-db-sink  
**Last Updated**: January 2026
