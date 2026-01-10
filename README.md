# MSSQLDBSink

A high-performance C# console application that synchronizes records from a source SQL Server database to a target database, inserting only records that don't exist in the target.

This project intends to be the "kitchen sink" solution for transferring SQL Server database data, capable of handling various scenarios, edge cases, and data topologies.

> [!CAUTION]
> **WARNING: USE AT YOUR OWN RISK. ALWAYS BACK UP YOUR DATA BEFORE USING THIS TOOL.**
> This tool modifies data in the target database. While checks are in place and the tool detects primary keys to avoid duplicates, data loss or corruption is possible if used incorrectly or if unexpected errors occur. The authors provide no warranty or guarantee.

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

1. Clone or download this repository
2. Navigate to the project directory
3. Build the project:
   ```bash
   dotnet build
   ```

## Usage

### Command Line Syntax

```bash
MSSQLDBSink [sourceServer] [sourceDb] [targetServer] [targetDb] [tableName] [options]
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
- `--start-row <OFFSETS>`: Comma-separated list of starting row numbers to skip for each table (e.g., "0,1000,500")
- `-o|--output-dir <DIR>`: Directory for saving JSON results (default: results)
- `-?|-h|--help`: Show help information

### Authentication

When using server/database name arguments, the tool automatically uses **Azure Active Directory Interactive** authentication (supports MFA). When using connection strings, authentication is determined by the connection string.

### Examples

#### Sync a Single Table

```bash
dotnet run -- "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users"
```

#### Sync Multiple Tables and Schemas

```bash
dotnet run -- "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "dbo.Users, Sales, HR.Employees"
```

#### Sync All Tables with Parallel Processing

```bash
dotnet run -- "source.database.windows.net" "SourceDB" "target.database.windows.net" "TargetDB" "all" --threads 4
```

#### Using Connection Strings

```bash
dotnet run -- --source-conn "Server=source...;Database=SourceDB;..." --target-conn "Server=target...;Database=TargetDB;..." "dbo.Users"
```

#### Clear Target and Bulk Insert

```bash
dotnet run -- "source..." "SourceDB" "target..." "TargetDB" "dbo.Users" --clear-target
```

#### Sync Tables Without Primary Keys

```bash
dotnet run -- "source..." "SourceDB" "target..." "TargetDB" "dbo.Logs" --allow-no-pk --deep-compare
```

#### Resume Sync with Start Row Offsets

If a sync job fails partway through, you can resume by skipping already-processed records:

```bash
# Sync 3 tables, but the second table already has 1000 records synced
dotnet run -- "source..." "SourceDB" "target..." "TargetDB" "dbo.Users,dbo.Orders,dbo.Products" --start-row "0,1000,0"
```

This will:
- Start syncing `dbo.Users` from row 0 (beginning)
- Start syncing `dbo.Orders` from row 1000 (skip first 1000 records)
- Start syncing `dbo.Products` from row 0 (beginning)

**Note**: The order of row offsets must match the order of tables being synced.

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

## License

This project is licensed under a custom license. It is free for non-commercial use but cannot be included in other projects or redistributed. See [LICENSE](LICENSE) for details.

## Contributing

Feel free to submit issues.
