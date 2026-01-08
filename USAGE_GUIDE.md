# Usage Guide - MSSQLDBSink

> **📝 Note**: This documentation has been moved and enhanced in the wiki!
> 
> Please see the **[wiki/Usage-Guide.md](wiki/Usage-Guide.md)** for the most up-to-date and comprehensive usage documentation.
>
> The wiki version includes:
> - Updated examples using compiled executable (not just `dotnet run`)
> - Enhanced build and publish instructions
> - More detailed scenarios and use cases
> - Comprehensive troubleshooting section
> - PowerShell and batch script examples
>
> **Quick Links:**
> - [Wiki Home](wiki/Home.md)
> - [Usage Guide (Enhanced)](wiki/Usage-Guide.md)
> - [Project Summary (Technical)](wiki/Project-Summary.md)

---

## 📖 Table of Contents
1. [Quick Start](#quick-start)
2. [Command Line Usage](#command-line-usage)
3. [Using Batch Scripts](#using-batch-scripts)
4. [Using PowerShell](#using-powershell)
5. [Common Scenarios](#common-scenarios)
6. [Best Practices](#best-practices)

---

## Quick Start

### Step 1: Build and Publish the Application
```bash
cd MSSQLDBSink
dotnet publish -c Release -o ./publish
cd publish
```

### Step 2: Run Your First Sync
```bash
./MSSQLDBSink "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users"
```
*Note: Authentication is handled automatically:*
*   *Azure SQL (`*.database.windows.net`): Uses Azure AD Default credentials (VS, CLI, Env Vars, or Interactive).*
*   *Local/On-Prem SQL: Uses Integrated Security (Windows Auth).*

---

## Command Line Usage

### Basic Syntax

Using the compiled executable:
```bash
./MSSQLDBSink [sourceServer] [sourceDb] [targetServer] [targetDb] [tableName] [options]
```

Using the compiled DLL:
```bash
dotnet MSSQLDBSink.dll [sourceServer] [sourceDb] [targetServer] [targetDb] [tableName] [options]
```

From source (development):
```bash
dotnet run --project src/MSSQLDBSink/MSSQLDBSink.csproj -- [arguments]
```

### Arguments

#### 1. Source Server (Optional if `--source-conn` provided)
The DNS name of the source SQL server (e.g., `mysource.database.windows.net`).

#### 2. Source Database (Optional if `--source-conn` provided)
The name of the source database.

#### 3. Target Server (Optional if `--target-conn` provided)
The DNS name of the target SQL server.

#### 4. Target Database (Optional if `--target-conn` provided)
The name of the target database.

#### 5. Table/Schema List (Optional)
Specific tables or schemas to sync. Can be a single name or a comma-separated list.

- **Examples**:
  - `dbo.Users` (Single table)
  - `Sales` (All tables in Sales schema)
  - `dbo.Users, Sales.Orders` (List of tables)
  - `dbo.Users, HR` (Table and Schema mixed)
  - `all` or `*` (All tables in all schemas)

- **If omitted**: Syncs all tables in all schemas in the database

### Options

- `-b|--batch-size <N>`: Records per batch (default: 1000)
- `-t|--threads <N>`: Degree of parallelism (default: 1)
- `--source-conn <STRING>`: Full connection string for source (overrides server/db)
- `--target-conn <STRING>`: Full connection string for target (overrides server/db)
- `--allow-no-pk`: Allow processing tables without primary keys
- `--deep-compare`: Use all columns as composite key (requires `--allow-no-pk`)
- `--clear-target`: Truncate target and bulk insert (fast)
- `--target-columns-only`: Import only columns that exist in target
- `--ignore-column`: Skip specific columns (Format: `Table.Col` or `Col`)
- `--map-column`: Map source to target column (Format: `Table.Source=Target`)
- `-o|--output-dir <DIR>`: Directory for JSON results (default: results)
- `-?|-h|--help`: Show help

---

## Using Batch Scripts

### Windows Batch File

Create a simple wrapper for the compiled executable:

**sync.bat:**
```batch
@echo off
cd publish
MSSQLDBSink.exe %*
```

**Usage:**
```cmd
sync.bat "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users" --batch-size 2000
```

Or if using from source (development):

**run-sync.bat:**
```batch
dotnet run --project src/MSSQLDBSink/MSSQLDBSink.csproj -- %*
```

### Environment Variables Approach

**Step 1:** Copy and edit the example file
```cmd
copy setup-env.example.bat setup-env.bat
notepad setup-env.bat
```

**Step 2:** Edit your server and database names in setup-env.bat

**Step 3:** Run the setup script
```cmd
setup-env.bat
```

**Step 4:** Use the environment variables with the compiled executable
```cmd
cd publish
MSSQLDBSink.exe "%SOURCE_SERVER%" "%SOURCE_DB%" "%TARGET_SERVER%" "%TARGET_DB%" "dbo.Users"
```

---

## Using PowerShell

### PowerShell Script

**run-sync.ps1:**
```powershell
.\run-sync.ps1 -SourceServer "source..." -SourceDb "SourceDB" -TargetServer "target..." -TargetDb "TargetDB" -TableName "dbo.Users" -Threads 4
```

### Parameters

- `-SourceServer`: Source server DNS name (required)
- `-SourceDb`: Source database name (required)
- `-TargetServer`: Target server DNS name (required)
- `-TargetDb`: Target database name (required)
- `-TableName`: Optional table name(s)
- `-BatchSize`: Optional batch size (default: 1000)
- `-Threads`: Optional thread count (default: 1)
- `-AllowNoPk`: Allow tables without primary keys
- `-DeepCompare`: Use deep comparison
- `-ClearTarget`: Clear target and bulk insert
- `-OutputDir`: Output directory for results

### Examples

**Sync single table:**
```powershell
.\run-sync.ps1 `
    -SourceServer "source.database.windows.net" `
    -SourceDb "SourceDB" `
    -TargetServer "target.database.windows.net" `
    -TargetDb "TargetDB" `
    -TableName "dbo.Users"
```

**Sync all tables with parallel processing:**
```powershell
.\run-sync.ps1 `
    -SourceServer "source.database.windows.net" `
    -SourceDb "SourceDB" `
    -TargetServer "target.database.windows.net" `
    -TargetDb "TargetDB" `
    -Threads 4
```

**Clear target and bulk insert:**
```powershell
.\run-sync.ps1 `
    -SourceServer "source.database.windows.net" `
    -SourceDb "SourceDB" `
    -TargetServer "target.database.windows.net" `
    -TargetDb "TargetDB" `
    -TableName "dbo.Users" `
    -ClearTarget
```

---

## Common Scenarios

### Scenario 1: Initial Database Seeding

**Goal**: Copy all data from production to a new test environment

**Command:**
```bash
./MSSQLDBSink "prod.database.windows.net" "ProdDB" "test.database.windows.net" "TestDB"
```

**Expected Result**: All tables from production copied to test (only new records)

---

### Scenario 2: Daily Incremental Sync

**Goal**: Sync new records added today to your reporting database

**Command:**
```bash
./MSSQLDBSink "oltp.database.windows.net" "TransactionalDB" "olap.database.windows.net" "ReportingDB" "dbo.Transactions" --batch-size 2000 --threads 2
```

**Expected Result**: Only new transactions are inserted into reporting database

---

### Scenario 3: Specific Table Migration

**Goal**: Migrate just the Users table from old to new system

**Command:**
```bash
./MSSQLDBSink "old.database.windows.net" "OldSystem" "new.database.windows.net" "NewSystem" "dbo.Users" --batch-size 500
```

**Expected Result**: Users table synced with 500 records per batch

---

### Scenario 4: Full Table Refresh

**Goal**: Completely replace target table with source data

**Command:**
```bash
./MSSQLDBSink "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users" --clear-target
```

**Expected Result**: Target table is truncated and all source records are bulk inserted

---

### Scenario 5: Sync Tables Without Primary Keys

**Goal**: Sync log tables that don't have primary keys

**Command:**
```bash
./MSSQLDBSink "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Logs" --allow-no-pk --deep-compare
```

**Expected Result**: Logs table synced using all columns for comparison

---

### Scenario 6: Using Connection Strings

**Goal**: Use custom connection strings or SQL Authentication

**Command:**
```bash
./MSSQLDBSink --source-conn "Server=source...;Database=SourceDB;User Id=user;Password=pass;..." --target-conn "Server=target...;Database=TargetDB;..." "dbo.Users"
```

**Expected Result**: Sync using provided connection strings

---

### Scenario 7: Local/On-Prem to Azure Sync

**Goal**: Sync from a local SQL Server to an Azure SQL Database

**Command:**
```bash
./MSSQLDBSink "localhost" "LocalSourceDB" "target.database.windows.net" "AzureTargetDB" "dbo.Users"
```

**Expected Result**:
- Source connects using Windows Authentication (Integrated Security)
- Target connects using Azure AD (Default Credentials)
- Data is synced from local to cloud

---

### Scenario 8: Ignoring Columns

**Goal**: Sync a table but skip sensitive or computed columns.

**Command:**
```bash
./MSSQLDBSink "source..." "SourceDB" "target..." "TargetDB" "dbo.Users" --ignore-column "PasswordHash" --ignore-column "LastLoginIP"
```

**Expected Result**:
- `dbo.Users` is synced.
- `PasswordHash` and `LastLoginIP` columns are completely ignored (not selected from source, not inserted into target).

---

## Best Practices

### 1. Security
✅ **DO:**
- Use Azure AD authentication when possible (Default Credentials supported)
- Use Integrated Security for local connections
- Use environment variables for connection strings if automating
- Use Azure Key Vault in production
- Create dedicated SQL users with minimal permissions (SELECT/INSERT only)
- Enable firewall rules on SQL Server to restrict access

❌ **DON'T:**
- Hardcode passwords in scripts
- Commit connection strings to source control
- Use admin accounts unnecessarily

### 2. Performance
✅ **DO:**
- Run from Azure (closer to databases = faster)
- Tune batch size based on record size
- Use `--threads` for multiple tables (be mindful of connection limits)
- Use `--clear-target` for full refresh scenarios
- Run during off-peak hours for large syncs
- Monitor database DTUs/vCores

❌ **DON'T:**
- Use very large batch sizes (>5000)
- Use too many threads (monitor connection pool)
- Run during peak business hours
- Sync unnecessary tables

### 3. Reliability
✅ **DO:**
- Test on small tables first
- Verify target schema matches source
- Check that tables have primary keys (or use appropriate flags)
- Monitor the console output
- Review JSON result files for failed tables
- Use result files to resume failed syncs

❌ **DON'T:**
- Sync to production without testing
- Ignore error messages
- Ignore count optimization warnings

### 4. Operational
✅ **DO:**
- Review JSON result files after each run
- Document your sync strategy
- Keep server details verified
- Use versioned result files to track progress

❌ **DON'T:**
- Run multiple syncs simultaneously on same table
- Forget to validate results
- Ignore permission errors

---

## Troubleshooting Tips

### Connection Issues
```
Error: Connection timeout
Solution: Ensure firewall allows your IP and server names are correct.
```

### Permission Issues
```
Error: SELECT permission denied
Solution: GRANT SELECT ON DATABASE::SourceDB TO [your_user]
```

```
Error: INSERT permission denied
Solution: GRANT INSERT ON DATABASE::TargetDB TO [your_user]
```

### Table Issues
```
Warning: Table has no primary key. Skipping...
Solution: Use --allow-no-pk --deep-compare to sync tables without PKs
```

```
Info: Target has same or more records. Skipping...
Solution: This is expected. Use --clear-target if you want to replace all data.
```

### Performance Issues
```
Issue: Sync is very slow
Solutions:
- Reduce batch size
- Check network latency
- Verify database performance tier
- Run from Azure VM in same region
- Use --clear-target for full refresh
```

---

## Example Session

```
PS C:\MSSQLDBSink\publish> ./MSSQLDBSink "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users"

MS SQL DB Sink - Database Record Sync Tool
===========================================

Source: source.database.windows.net (SourceDB)
Target: target.database.windows.net (TargetDB)
Batch Size: 1000
Parallel Threads: 1

Fetching list of tables from source database...
Found 1 tables to sync.

Syncing table: dbo.Users
  Table: dbo.Users | Keys: UserId | Source: 15,234 | Target: 12,100
  > dbo.Users: 15,234/15,234
  ✓ dbo.Users Completed in 12.45s - Inserted: 3,134, Skipped: 12,100

✓ Sync completed successfully!

Results saved to: results/sync-result-20251211_143022.json
```

---

## Need Help?

1. Check the README.md for detailed documentation
2. Review PROJECT_SUMMARY.md for technical details
3. Check server/db names are correct
4. Verify database permissions
5. Ensure tables have primary keys (or use appropriate flags)
6. Review console output for specific errors
7. Check JSON result files in the results directory
8. Run with `--help` for all available options

---

**Version**: 2.0.0
**Last Updated**: December 2025
