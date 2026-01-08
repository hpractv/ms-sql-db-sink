# MSSQLDBSink

A high-performance C# console application that synchronizes records from a source SQL Server database to a target database, inserting only records that don't exist in the target.

This project intends to be the "kitchen sink" solution for transferring SQL Server database data, capable of handling various scenarios, edge cases, and data topologies.

> [!CAUTION]
> **WARNING: USE AT YOUR OWN RISK. ALWAYS BACK UP YOUR DATA BEFORE USING THIS TOOL.**
> This tool modifies data in the target database. While checks are in place and the tool detects primary keys to avoid duplicates, data loss or corruption is possible if used incorrectly or if unexpected errors occur. The authors provide no warranty or guarantee.

## 📚 Documentation

- **[Usage Guide](wiki/Usage-Guide.md)** - Comprehensive usage examples, scenarios, and best practices
- **[Project Summary](wiki/Project-Summary.md)** - Technical architecture and implementation details

## Features

- ✅ **Smart Sync**: Only inserts records that don't exist in the target database (compares by primary keys)
- ✅ **Batch Processing**: Processes records in configurable batches for optimal performance
- ✅ **Single or Multi-Table**: Sync a specific table or all tables in the database
- ✅ **Progress Tracking**: Real-time console output showing sync progress
- ✅ **Transaction Safety**: Uses database transactions to ensure data integrity
- ✅ **Primary Key Detection**: Automatically detects and uses primary keys for comparison
- ✅ **Schema Support**: Handles tables in different schemas (e.g., dbo.Users, sales.Orders)
- ✅ **Parallel Processing**: Multi-threaded table syncing for improved performance
- ✅ **Connection Resiliency**: Automatic retry with exponential backoff for transient errors
- ✅ **Resume Capability**: JSON result files track progress for resuming failed syncs
- ✅ **Count Optimization**: Skips tables where target already has same or more records
- ✅ **Bulk Insert Mode**: Fast truncate-and-replace option for full table refresh
- ✅ **No-PK Support**: Optional support for tables without primary keys using deep comparison

## Prerequisites

- .NET 10.0 or later
- Access to both source and target SQL Server databases
- Tables in target database must already exist with the same schema as source

## Installation

### Option 1: Build from Source
1. Clone or download this repository
2. Navigate to the project directory
3. Build the project:
   ```bash
   dotnet build
   ```

### Option 2: Publish for Distribution
1. Build and publish the application:
   ```bash
   dotnet publish -c Release -o ./publish
   ```
2. The compiled executable will be in the `publish` folder
3. Run the executable directly:
   ```bash
   cd publish
   ./MSSQLDBSink [arguments]
   ```
   Or on Windows:
   ```cmd
   cd publish
   MSSQLDBSink.exe [arguments]
   ```

## Usage

### Command Line Syntax

Using the compiled executable:
```bash
./MSSQLDBSink [sourceServer] [sourceDb] [targetServer] [targetDb] [tableName] [options]
```

Or using the .NET CLI (from source):
```bash
dotnet run --project src/MSSQLDBSink/MSSQLDBSink.csproj -- [sourceServer] [sourceDb] [targetServer] [targetDb] [tableName] [options]
```

Or using the compiled DLL:
```bash
dotnet MSSQLDBSink.dll [sourceServer] [sourceDb] [targetServer] [targetDb] [tableName] [options]
```

### Arguments

- **sourceServer** (optional if `--source-conn` provided): The DNS name of the source SQL server
- **sourceDb** (optional if `--source-conn` provided): The name of the source database
- **targetServer** (optional if `--target-conn` provided): The DNS name of the target SQL server
- **targetDb** (optional if `--target-conn` provided): The name of the target database
- **tableName** (optional): Specific table(s) or schema(s) to sync. Comma-separated. Use `all` or `*` for all tables.

### Options

- `-b|--batch-size <N>`: Records per batch (default: 1000)
- `-t|--threads <N>`: Degree of parallelism for table syncing (default: 1)
- `--source-conn <STRING>`: Full connection string for source database (overrides server/db arguments)
- `--target-conn <STRING>`: Full connection string for target database (overrides server/db arguments)
- `--allow-no-pk`: Allow processing tables without primary keys
- `--deep-compare`: Use all columns as composite key for comparison (requires `--allow-no-pk`)
- `--clear-target`: Truncate target table and bulk insert (fast, no comparison)
- `-o|--output-dir <DIR>`: Directory for saving JSON results (default: results)
- `-?|-h|--help`: Show help information

### Authentication

When using server/database name arguments, the tool automatically uses **Azure Active Directory Interactive** authentication (supports MFA). When using connection strings, authentication is determined by the connection string.

**Authentication Methods:**

1. **Azure SQL** (`*.database.windows.net`):
   - Uses **Azure Active Directory Default** authentication
   - Tries: VS Creds → CLI (`az login`) → Env Vars → Managed Identity → Interactive Browser

2. **Local / On-Prem SQL** (e.g., `localhost`, `MyServer`):
   - Uses **Integrated Security** (Windows Authentication) by default
   - Sets `TrustServerCertificate=True` and `Encrypt=False` for compatibility

3. **Custom Connection Strings** (`--source-conn` / `--target-conn`):
   - Uses exactly what you provide in the string

### Examples

#### Sync a Single Table

```bash
./MSSQLDBSink "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users"
```

#### Sync Multiple Tables and Schemas

```bash
./MSSQLDBSink "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users, Sales, HR.Employees"
```

#### Sync All Tables with Parallel Processing

```bash
./MSSQLDBSink "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "all" --threads 4
```

#### Using Connection Strings

```bash
./MSSQLDBSink --source-conn "Server=source...;Database=SourceDB;..." --target-conn "Server=target...;Database=TargetDB;..." "dbo.Users"
```

#### Clear Target and Bulk Insert

```bash
./MSSQLDBSink "source..." "SourceDB" "target..." "TargetDB" "dbo.Users" --clear-target
```

#### Sync Tables Without Primary Keys

```bash
./MSSQLDBSink "source..." "SourceDB" "target..." "TargetDB" "dbo.Logs" --allow-no-pk --deep-compare
```

## How It Works

1. **Connect**: Establishes connections to both source and target databases
2. **Analyze**: Identifies primary keys and columns for the table(s)
3. **Count Check**: Compares record counts - skips if target >= source (unless `--clear-target`)
4. **Fetch**: Retrieves records from source database in batches
5. **Compare**: Checks each record against target database using primary key(s) or all columns
6. **Insert**: Inserts only records that don't exist in target (or bulk inserts if `--clear-target`)
7. **Report**: Displays progress, summary statistics, and saves JSON result file

## Result Files

The tool generates JSON result files in the output directory (default: `results/`) with:
- Run ID and timestamps
- All command-line parameters used
- Per-table sync results (status, counts, duration, errors)
- Enables manual resume by identifying failed/skipped tables

## 🚀 Common Commands

### Sync Single Table
```bash
./MSSQLDBSink "source..." "SourceDB" "target..." "TargetDB" "dbo.Users"
```

### Sync Multiple Tables/Schemas
```bash
./MSSQLDBSink "source..." "SourceDB" "target..." "TargetDB" "dbo.Users, Sales, HR.Employees"
```

### Sync All Tables
```bash
./MSSQLDBSink "source..." "SourceDB" "target..." "TargetDB"
```

### Sync with Options
```bash
./MSSQLDBSink "source..." "SourceDB" "target..." "TargetDB" "dbo.Users" --batch-size 2000 --threads 4
```

### Clear Target and Bulk Insert
```bash
./MSSQLDBSink "source..." "SourceDB" "target..." "TargetDB" "dbo.Users" --clear-target
```

### Ignore Specific Columns
```bash
./MSSQLDBSink "source..." "SourceDB" "target..." "TargetDB" --ignore-column "PasswordHash" --ignore-column "Users.LastLogin"
```

### Using Connection Strings
```bash
./MSSQLDBSink --source-conn "Server=..." --target-conn "Server=..." "dbo.Users"
```

### Using PowerShell
```powershell
.\run-sync.ps1 -SourceServer "source..." -SourceDb "SourceDB" -TargetServer "target..." -TargetDb "TargetDB" -TableName "dbo.Users" -Threads 4
```

### Running from Source (Development)
```bash
dotnet run --project src/MSSQLDBSink/MSSQLDBSink.csproj -- "source..." "SourceDB" "target..." "TargetDB" "dbo.Users"
```

## Batch Size Guidelines

| Record Size | Network Speed | Recommended Batch Size |
|------------|---------------|----------------------|
| Small      | Fast          | 2000-5000           |
| Small      | Slow          | 500-1000            |
| Large      | Fast          | 500-1000            |
| Large      | Slow          | 100-500             |

## Required Permissions

**Source Database:**
- SELECT on tables
- VIEW DEFINITION

**Target Database:**
- INSERT on tables
- VIEW DEFINITION
- DELETE/TRUNCATE (if using `--clear-target`)

## 📊 What It Does

✅ Inserts records that don't exist in target  
✅ Skips records that already exist  
✅ Skips tables where target count >= source count  
✅ Bulk inserts when `--clear-target` is used  
❌ Does NOT update existing records (unless `--clear-target`)  
❌ Does NOT delete records (unless `--clear-target`)  
❌ Does NOT modify schemas

## 🔑 Prerequisites

- Target database must exist
- Target tables must exist (same schema as source)
- Tables typically need primary keys (use `--allow-no-pk --deep-compare` for tables without PKs)
- Proper database permissions

## 🐛 Quick Troubleshooting

| Error              | Solution                           |
| ------------------ | ---------------------------------- |
| Connection timeout | Check firewall rules, whitelist IP |
| Permission denied  | Grant SELECT/INSERT permissions    |
| No primary key     | Use `--allow-no-pk --deep-compare` |
| Duplicate key      | Check schema matches exactly       |
| Target >= Source   | Use `--clear-target` to force sync |

## 📞 Getting Help

1. Review the **[Usage Guide](wiki/Usage-Guide.md)** for comprehensive examples and scenarios
2. Check the **[Project Summary](wiki/Project-Summary.md)** for technical details
3. Run with `--help` for all command-line options
4. Review JSON result files in `results/` directory

## License

This project is licensed under a custom license. It is free for non-commercial use but cannot be included in other projects or redistributed. See [LICENSE](LICENSE) for details.

## Contributing

Feel free to submit issues.
