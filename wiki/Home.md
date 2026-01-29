# MSSQLDBSink Wiki

Welcome to the MSSQLDBSink documentation wiki! This comprehensive guide provides detailed information about using the database synchronization tool.

## 📚 Documentation Structure

### Main Documentation
- **[README](../README.md)** - Overview, quick start, and essential information
- **[Quick Reference](../QUICK_REFERENCE.md)** - Common commands and troubleshooting tips

### Detailed Guides (This Wiki)
- **[Project Summary](Project-Summary)** - Technical architecture, features, and implementation details
- **[Usage Guide](Usage-Guide)** - Comprehensive usage examples, scenarios, and best practices

## 🚀 Quick Links

### Getting Started
1. [Installation and Setup](Usage-Guide#installation-and-setup)
2. [Quick Start Guide](Usage-Guide#quick-start)
3. [Command Line Usage](Usage-Guide#command-line-usage)

### Common Tasks
- [Sync a Single Table](Usage-Guide#scenario-3-specific-table-migration)
- [Sync All Tables](Usage-Guide#scenario-1-initial-database-seeding)
- [Full Table Refresh](Usage-Guide#scenario-4-full-table-refresh)
- [Using Connection Strings](Usage-Guide#scenario-6-using-connection-strings)

### Technical Information
- [Key Features](Project-Summary#key-features)
- [How It Works](Project-Summary#how-it-works)
- [Performance Guidelines](Project-Summary#configuration-options)
- [Security Best Practices](Project-Summary#security-best-practices)

### Help & Support
- [Troubleshooting Guide](Usage-Guide#troubleshooting)
- [Best Practices](Usage-Guide#best-practices)
- [Common Scenarios](Usage-Guide#common-scenarios)

## 📖 What is MSSQLDBSink?

MSSQLDBSink is a high-performance C# console application that synchronizes records from a source SQL Server database to a target database, inserting only records that don't exist in the target.

### Key Features at a Glance
- ✅ **Smart Sync** - Only inserts missing records (compares by primary keys)
- ✅ **Batch Processing** - Configurable batches for optimal performance
- ✅ **Multi-Table Support** - Sync specific tables, schemas, or entire databases
- ✅ **Parallel Processing** - Multi-threaded for improved performance
- ✅ **Azure AD Auth** - Modern authentication with MFA support
- ✅ **Progress Tracking** - Real-time console output and JSON result files
- ✅ **Resume Capability** - Track progress and resume failed syncs

## 🎯 Common Use Cases

1. **Dev/Test Environment Seeding** - Copy production data to test environments
2. **Incremental Data Migration** - Daily/periodic syncs of new records
3. **Disaster Recovery** - Backup database synchronization
4. **Data Warehouse Loading** - ETL operations for reporting databases
5. **Database Consolidation** - Merge data from multiple sources

## 📦 Quick Start Example

After building/publishing the application:

```bash
# Sync a single table
./MSSQLDBSink "source.database.windows.net" "SourceDB" \
              "target.database.windows.net" "TargetDB" \
              "dbo.Users"

# Sync all tables with parallel processing
./MSSQLDBSink "source.database.windows.net" "SourceDB" \
              "target.database.windows.net" "TargetDB" \
              --threads 4

# Full table refresh (truncate and reload)
./MSSQLDBSink "source.database.windows.net" "SourceDB" \
              "target.database.windows.net" "TargetDB" \
              "dbo.Users" --clear-target
```

## 🔧 Installation

### Using Published Binary
```bash
# Publish the application
dotnet publish -c Release -o ./publish

# Run the executable
cd publish
./MSSQLDBSink [arguments]
```

### From Source (Development)
```bash
# Clone the repository
git clone https://github.com/hpractv/ms-sql-db-sink.git
cd ms-sql-db-sink

# Build and run
dotnet build
dotnet run --project src/MSSQLDBSink/MSSQLDBSink.csproj -- [arguments]
```

See [Installation and Setup](Usage-Guide#installation-and-setup) for detailed instructions.

## 📋 Documentation Index

### By Topic

#### Installation & Setup
- [Prerequisites](Usage-Guide#prerequisites)
- [Installation Methods](Usage-Guide#installation-methods)
- [Publishing Options](Usage-Guide#publishing-options)

#### Basic Usage
- [Command Line Syntax](Usage-Guide#command-line-usage)
- [Arguments and Options](Usage-Guide#arguments)
- [Authentication Methods](../README.md#authentication)

#### Advanced Usage
- [Batch Scripts](Usage-Guide#using-batch-scripts)
- [PowerShell Integration](Usage-Guide#using-powershell)
- [Connection Strings](Usage-Guide#scenario-6-using-connection-strings)

#### Configuration
- [Batch Size Guidelines](Project-Summary#batch-size-guidelines)
- [Thread Count Guidelines](Project-Summary#thread-count-guidelines)
- [Performance Tuning](Project-Summary#performance-characteristics)

#### Troubleshooting
- [Connection Issues](Usage-Guide#connection-issues)
- [Permission Issues](Usage-Guide#permission-issues)
- [Table Issues](Usage-Guide#table-issues)
- [Performance Issues](Usage-Guide#performance-issues)

#### Development
- [Project Structure](Project-Summary#project-structure)
- [Dependencies](Project-Summary#dependencies)
- [Building and Publishing](Project-Summary#building-and-publishing)

## ⚠️ Important Notes

> **WARNING: USE AT YOUR OWN RISK. ALWAYS BACK UP YOUR DATA BEFORE USING THIS TOOL.**
>
> This tool modifies data in the target database. While checks are in place and the tool detects primary keys to avoid duplicates, data loss or corruption is possible if used incorrectly or if unexpected errors occur. The authors provide no warranty or guarantee.

### What It Does
- ✅ Inserts records that don't exist in target
- ✅ Skips records that already exist
- ✅ Skips tables where target count >= source count

### What It Does NOT Do
- ❌ Does NOT update existing records (unless `--clear-target`)
- ❌ Does NOT delete records (unless `--clear-target`)
- ❌ Does NOT modify schemas

## 📞 Getting Help

If you need assistance:

1. **Start with the basics**: Check the [README](../README.md) for quick overview
2. **Common commands**: See [Quick Reference](../QUICK_REFERENCE.md) for command examples
3. **Detailed scenarios**: Review the [Usage Guide](Usage-Guide) for comprehensive examples
4. **Technical details**: Check [Project Summary](Project-Summary) for architecture info
5. **Troubleshooting**: Use the [troubleshooting section](Usage-Guide#troubleshooting)
6. **Command help**: Run `./MSSQLDBSink --help` for all options
7. **Result files**: Check JSON files in `results/` directory for run details

## 📝 License

This project is licensed under a custom license. It is free for non-commercial use but cannot be included in other projects or redistributed. See [LICENSE](../LICENSE) for details.

## 🤝 Contributing

Contributions welcome! Feel free to submit issues or pull requests at the [GitHub repository](https://github.com/hpractv/ms-sql-db-sink).

---

**Project**: MSSQLDBSink  
**Version**: 2.0.0  
**Framework**: .NET 10.0  
**Repository**: https://github.com/hpractv/ms-sql-db-sink  
**Last Updated**: January 2026
