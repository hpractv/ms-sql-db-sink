# Usage Guide - MSSQLDBSink

## 📖 Table of Contents
1. [Quick Start](#quick-start)
2. [Installation and Setup](#installation-and-setup)
3. [Command Line Usage](#command-line-usage)
4. [Using Batch Scripts](#using-batch-scripts)
5. [Using PowerShell](#using-powershell)
6. [Common Scenarios](#common-scenarios)
7. [Best Practices](#best-practices)
8. [Troubleshooting](#troubleshooting)

---

## Quick Start

### Step 1: Build and Publish the Application

#### Option A: Quick Build (Development)
```bash
cd /path/to/MSSQLDBSink
dotnet build
```

#### Option B: Publish for Production
```bash
dotnet publish -c Release -o ./publish
cd publish
```

### Step 2: Run Your First Sync

**Using the compiled executable (after publish):**
```bash
./MSSQLDBSink "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users"
```

**Using DLL (after build or publish):**
```bash
dotnet MSSQLDBSink.dll "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users"
```

**From source (Development):**
```bash
dotnet run --project src/MSSQLDBSink/MSSQLDBSink.csproj -- "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users"
```

*Note: Authentication is handled automatically:*
- *Azure SQL (`*.database.windows.net`): Uses Azure AD Default credentials (VS, CLI, Env Vars, or Interactive).*
- *Local/On-Prem SQL: Uses Integrated Security (Windows Auth).*

---

## Installation and Setup

### Prerequisites
- .NET 10.0 SDK or later
- Access to both source and target SQL Server databases
- Tables in target database must already exist with the same schema as source

### Installation Methods

#### Method 1: Clone from GitHub
```bash
git clone https://github.com/hpractv/ms-sql-db-sink.git
cd ms-sql-db-sink
dotnet build
```

#### Method 2: Download Release
1. Download the latest release from GitHub
2. Extract to a folder
3. No build required if using pre-compiled binaries

### Publishing Options

#### Framework-Dependent Deployment
Requires .NET runtime installed on target machine:
```bash
dotnet publish -c Release -o ./publish
```

#### Self-Contained Deployment
Includes .NET runtime (larger, but no dependencies):

**Windows:**
```bash
dotnet publish -c Release -r win-x64 --self-contained -o ./publish-win
```

**Linux:**
```bash
dotnet publish -c Release -r linux-x64 --self-contained -o ./publish-linux
```

**macOS:**
```bash
dotnet publish -c Release -r osx-x64 --self-contained -o ./publish-mac
```

---

## Command Line Usage

### Basic Syntax

```bash
# Using compiled executable
./MSSQLDBSink [sourceServer] [sourceDb] [targetServer] [targetDb] [tableName] [options]

# Using DLL
dotnet MSSQLDBSink.dll [sourceServer] [sourceDb] [targetServer] [targetDb] [tableName] [options]

# From source
dotnet run --project src/MSSQLDBSink/MSSQLDBSink.csproj -- [arguments]
```

### Arguments

#### 1. Source Server (Optional if `--source-conn` provided)
The DNS name of the source SQL server (e.g., `mysource.database.windows.net` or `localhost`).

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
  - (Omit) - Syncs all tables in all schemas

### Options

- `-b|--batch-size <N>`: Records per batch (default: 100000)
- `-t|--threads <N>`: Degree of parallelism (default: 4)
- `--source-conn <STRING>`: Full connection string for source (overrides server/db)
- `--target-conn <STRING>`: Full connection string for target (overrides server/db)
- `--allow-no-pk`: Allow processing tables without primary keys
- `--deep-compare`: Use all columns as composite key (requires `--allow-no-pk`)
- `--clear-target`: Truncate target and bulk insert (fast)
- `--target-columns-only`: Import only columns that exist in target
- `--ignore-column`: Skip specific columns (Format: `Table.Col` or `Col`)
- `--map-column`: Map source to target column (Format: `Table.Source=Target`)
- `-o|--output-dir <DIR>`: Directory for JSON results (default: results)
- `--compare-counts-and-schema`: Read-only mode to compare databases
- `-?|-h|--help`: Show help

---

## Using Batch Scripts

### Windows Batch File

Create a simple wrapper script:

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

### Environment Variables Approach

**Step 1:** Create a configuration script

**setup-env.bat:**
```batch
@echo off
SET SOURCE_SERVER=source.database.windows.net
SET SOURCE_DB=SourceDB
SET TARGET_SERVER=target.database.windows.net
SET TARGET_DB=TargetDB
```

**Step 2:** Run the setup script
```cmd
setup-env.bat
```

**Step 3:** Use the environment variables
```cmd
cd publish
MSSQLDBSink.exe "%SOURCE_SERVER%" "%SOURCE_DB%" "%TARGET_SERVER%" "%TARGET_DB%" "dbo.Users"
```

### Linux/Mac Shell Script

**sync.sh:**
```bash
#!/bin/bash
cd publish
./MSSQLDBSink "$@"
```

Make it executable:
```bash
chmod +x sync.sh
```

**Usage:**
```bash
./sync.sh "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users"
```

---

## Using PowerShell

### PowerShell Script

The repository includes a PowerShell script for convenience:

**run-sync.ps1:**
```powershell
param(
    [Parameter(Mandatory=$true)]
    [string]$SourceServer,
    
    [Parameter(Mandatory=$true)]
    [string]$SourceDb,
    
    [Parameter(Mandatory=$true)]
    [string]$TargetServer,
    
    [Parameter(Mandatory=$true)]
    [string]$TargetDb,
    
    [string]$TableName = "",
    [int]$BatchSize = 100000,
    [int]$Threads = 4,
    [switch]$AllowNoPk,
    [switch]$DeepCompare,
    [switch]$ClearTarget,
    [string]$OutputDir = "results"
)

$args = @()
$args += $SourceServer, $SourceDb, $TargetServer, $TargetDb

if ($TableName) { $args += $TableName }
if ($BatchSize) { $args += "--batch-size", $BatchSize }
if ($Threads) { $args += "--threads", $Threads }
if ($AllowNoPk) { $args += "--allow-no-pk" }
if ($DeepCompare) { $args += "--deep-compare" }
if ($ClearTarget) { $args += "--clear-target" }
if ($OutputDir) { $args += "--output-dir", $OutputDir }

& "./publish/MSSQLDBSink" @args
```

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
    -Threads 8
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
./MSSQLDBSink "oltp.database.windows.net" "TransactionalDB" "olap.database.windows.net" "ReportingDB" "dbo.Transactions" --batch-size 5000 --threads 2
```

**Expected Result**: Only new transactions are inserted into reporting database

---

### Scenario 3: Specific Table Migration

**Goal**: Migrate just the Users table from old to new system

**Command:**
```bash
./MSSQLDBSink "old.database.windows.net" "OldSystem" "new.database.windows.net" "NewSystem" "dbo.Users" --batch-size 1000
```

**Expected Result**: Users table synced with 1000 records per batch

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
./MSSQLDBSink --source-conn "Server=source.db.net;Database=SourceDB;User Id=user;Password=pass;Encrypt=True;" --target-conn "Server=target.db.net;Database=TargetDB;Integrated Security=True;" "dbo.Users"
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

### Scenario 8: Ignoring Sensitive Columns

**Goal**: Sync a table but skip sensitive or computed columns

**Command:**
```bash
./MSSQLDBSink "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users" --ignore-column "PasswordHash" --ignore-column "LastLoginIP" --ignore-column "SSN"
```

**Expected Result**:
- `dbo.Users` is synced
- `PasswordHash`, `LastLoginIP`, and `SSN` columns are completely ignored

---

### Scenario 9: Multi-Table Sync with Specific Tables

**Goal**: Sync only specific critical tables

**Command:**
```bash
./MSSQLDBSink "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users, dbo.Orders, dbo.Products, Sales.Invoices" --threads 4
```

**Expected Result**: Four specified tables synced in parallel

---

### Scenario 10: Schema-Only Sync

**Goal**: Sync all tables in a specific schema

**Command:**
```bash
./MSSQLDBSink "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "Sales" --threads 4
```

**Expected Result**: All tables in the Sales schema are synced

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
- Never commit connection strings with credentials to source control

❌ **DON'T:**
- Hardcode passwords in scripts
- Commit connection strings to source control
- Use admin accounts unnecessarily
- Share credentials via email or chat

### 2. Performance
✅ **DO:**
- Run from Azure (closer to databases = faster)
- Tune batch size based on record size
- Use `--threads` for multiple tables (be mindful of connection limits)
- Use `--clear-target` for full refresh scenarios
- Run during off-peak hours for large syncs
- Monitor database DTUs/vCores
- Start with small tables to test performance

❌ **DON'T:**
- Use very large batch sizes (>10000) without testing
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
- Back up target database before large syncs

❌ **DON'T:**
- Sync to production without testing
- Ignore error messages
- Ignore count optimization warnings
- Run multiple syncs on the same table simultaneously

### 4. Operational
✅ **DO:**
- Review JSON result files after each run
- Document your sync strategy
- Keep server details verified
- Use versioned result files to track progress
- Set up alerting for failed syncs in production
- Keep published binaries in version control or artifact storage

❌ **DON'T:**
- Run multiple syncs simultaneously on same table
- Forget to validate results
- Ignore permission errors
- Delete result files immediately (keep for audit trail)

---

## Troubleshooting

### Connection Issues

#### Problem: Connection timeout
```
Error: A connection attempt failed because the connected party did not properly respond...
```
**Solutions:**
1. Ensure firewall allows your IP
2. Verify server names are correct
3. Check network connectivity
4. For Azure SQL, whitelist your IP in Azure Portal

#### Problem: Azure AD authentication failed
```
Error: Azure AD authentication failed
```
**Solutions:**
1. Run `az login` to authenticate with Azure CLI
2. Use Visual Studio to sign in to Azure
3. Use connection strings with SQL authentication instead
4. Check that your account has access to the database

#### Problem: Certificate validation errors
```
Error: The certificate chain was issued by an authority that is not trusted
```
**Solution:** Add `TrustServerCertificate=True` to connection string or use `--source-conn`/`--target-conn` options

---

### Permission Issues

#### Problem: SELECT permission denied
```
Error: The SELECT permission was denied on the object 'Users'
```
**Solution:**
```sql
-- Grant SELECT on specific table
GRANT SELECT ON dbo.Users TO [your_user]

-- Or grant SELECT on entire database
GRANT SELECT ON DATABASE::SourceDB TO [your_user]
```

#### Problem: INSERT permission denied
```
Error: The INSERT permission was denied on the object 'Users'
```
**Solution:**
```sql
-- Grant INSERT on specific table
GRANT INSERT ON dbo.Users TO [your_user]

-- Or grant INSERT on entire database
GRANT INSERT ON DATABASE::TargetDB TO [your_user]
```

#### Problem: TRUNCATE permission denied (with --clear-target)
```
Error: Cannot truncate table 'Users' because the statement failed
```
**Solution:**
```sql
GRANT DELETE ON dbo.Users TO [your_user]
-- Or
ALTER ROLE db_ddladmin ADD MEMBER [your_user]
```

---

### Table Issues

#### Problem: Table has no primary key
```
Warning: Table 'dbo.Logs' has no primary key. Skipping...
```
**Solution:** Use `--allow-no-pk --deep-compare` to sync tables without primary keys
```bash
./MSSQLDBSink "source..." "SourceDB" "target..." "TargetDB" "dbo.Logs" --allow-no-pk --deep-compare
```

#### Problem: Target has same or more records
```
Info: Target table 'dbo.Users' has 1000 records, source has 1000. Skipping...
```
**Solution:** This is expected optimization behavior. Use `--clear-target` if you want to replace all data:
```bash
./MSSQLDBSink "source..." "SourceDB" "target..." "TargetDB" "dbo.Users" --clear-target
```

#### Problem: Schema mismatch
```
Error: Column 'Email' does not exist in target table
```
**Solution:** Ensure target table has the same schema as source, or use `--target-columns-only` to sync only matching columns

---

### Performance Issues

#### Problem: Sync is very slow
**Solutions:**
1. Reduce batch size (try 1000-5000)
2. Check network latency between source and target
3. Verify database performance tier (DTUs/vCores)
4. Run from Azure VM in same region
5. Use `--clear-target` for full refresh (much faster)
6. Increase threads for multi-table syncs: `--threads 8`

#### Problem: Out of memory errors
**Solutions:**
1. Reduce batch size significantly (try 500-1000)
2. Reduce thread count
3. Run on machine with more memory

---

### Application Issues

#### Problem: .NET runtime not found
```
Error: You must install or update .NET to run this application
```
**Solutions:**
1. Install .NET 10.0 SDK or runtime
2. Use self-contained deployment (no runtime needed)
3. Download from: https://dotnet.microsoft.com/download

#### Problem: DLL not found errors
**Solution:** Ensure you're running from the publish directory with all dependencies:
```bash
cd publish
./MSSQLDBSink [arguments]
```

---

## Example Sessions

### Example 1: Successful Single Table Sync

```bash
$ ./MSSQLDBSink "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users"

┌─────────────────────────────────────────────────────────────┐
│ MS SQL DB Sink - Database Record Sync Tool                 │
└─────────────────────────────────────────────────────────────┘

Source: source.database.windows.net (SourceDB)
Target: target.database.windows.net (TargetDB)
Batch Size: 100,000
Parallel Threads: 4

Fetching list of tables from source database...
Found 1 table to sync.

Syncing table: dbo.Users
  Table: dbo.Users | Keys: UserId | Source: 15,234 | Target: 12,100
  [████████████████████████████████████████] 100% (15,234/15,234)
  ✓ dbo.Users Completed in 12.45s - Inserted: 3,134, Skipped: 12,100

✓ Sync completed successfully!

Results saved to: results/sync-result-20260108_143022.json
```

### Example 2: Multi-Table Parallel Sync

```bash
$ ./MSSQLDBSink "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" --threads 4

┌─────────────────────────────────────────────────────────────┐
│ MS SQL DB Sink - Database Record Sync Tool                 │
└─────────────────────────────────────────────────────────────┘

Source: source.database.windows.net (SourceDB)
Target: target.database.windows.net (TargetDB)
Batch Size: 100,000
Parallel Threads: 4

Fetching list of tables from source database...
Found 12 tables to sync.

┌──────────────────┬──────────┬──────────┬──────────┬────────────┐
│ Table            │ Source   │ Target   │ Inserted │ Duration   │
├──────────────────┼──────────┼──────────┼──────────┼────────────┤
│ dbo.Users        │ 15,234   │ 12,100   │ 3,134    │ 12.45s     │
│ dbo.Orders       │ 50,000   │ 48,000   │ 2,000    │ 8.30s      │
│ dbo.Products     │ 1,200    │ 1,200    │ 0        │ 0.15s      │
│ Sales.Invoices   │ 32,400   │ 30,000   │ 2,400    │ 15.20s     │
│ ... (8 more)     │          │          │          │            │
└──────────────────┴──────────┴──────────┴──────────┴────────────┘

✓ Sync completed successfully!

Total: 12 tables, 98,834 records synced in 45.3s

Results saved to: results/sync-result-20260108_143525.json
```

---

## Need Help?

1. Check the [README.md](../README.md) for overview documentation
2. Review [Project-Summary.md](Project-Summary.md) for technical details
3. Check [QUICK_REFERENCE.md](../QUICK_REFERENCE.md) for common commands
4. Verify server/db names are correct
5. Verify database permissions
6. Ensure tables have primary keys (or use appropriate flags)
7. Review console output for specific errors
8. Check JSON result files in the results directory
9. Run with `--help` for all available options

---

**Version**: 2.0.0  
**Last Updated**: January 2026  
**Repository**: https://github.com/hpractv/ms-sql-db-sink
