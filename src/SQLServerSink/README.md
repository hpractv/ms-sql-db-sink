# SQL Server Sink

A high-performance C# console application that synchronizes records from a source SQL Server database to a target database, inserting only records that don't exist in the target.

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
SQLServerSink [sourceServer] [sourceDb] [targetServer] [targetDb] [tableName] [options]
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

## Performance Tips

- **Batch Size**: Adjust based on your record size and network speed
  - Smaller batches (100-500): Better for large records or slow networks
  - Larger batches (2000-5000): Better for small records or fast networks
- **Threads**: Use `--threads` to parallelize table syncing (be mindful of database connection limits)
- **Clear Target**: Use `--clear-target` for full refresh scenarios (much faster than row-by-row)
- **Indexes**: Ensure primary keys are properly indexed in both databases
- **Network**: Run from a location with good connectivity to both databases
- **Peak Hours**: Schedule large syncs during off-peak hours

## Limitations

- Target tables must already exist with matching schema
- By default, requires primary keys on tables (use `--allow-no-pk --deep-compare` for tables without PKs)
- Does not update existing records (insert-only, unless `--clear-target`)
- Does not delete records from target that don't exist in source
- Count optimization skips tables where target >= source (unless `--clear-target`)

## Security Notes

- Connection strings may contain sensitive information - handle with care
- Consider using Azure Key Vault for storing connection strings in production
- Use least-privilege database accounts (INSERT and SELECT permissions only)
- Enable firewall rules on SQL Server to restrict access

## Troubleshooting

### "Table has no primary key. Skipping..."
- The table doesn't have a primary key defined
- Use `--allow-no-pk --deep-compare` to sync tables without PKs

### "Target has same or more records. Skipping..."
- Target already has all records from source
- Use `--clear-target` if you want to replace all data

### "Connection timeout"
- Check firewall rules on SQL Server
- Verify connection strings are correct
- Ensure your IP is whitelisted

### "Permission denied"
- Database user needs SELECT permission on source
- Database user needs INSERT permission on target
- Database user needs VIEW DEFINITION permission to read metadata
- For `--clear-target`, user needs DELETE/TRUNCATE permission

## License

This project is licensed under the GNU General Public License v3.0. See [LICENSE](../LICENSE) for details.

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.
