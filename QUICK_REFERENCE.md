# Quick Reference Card

## 🚀 Common Commands

### Sync Single Table
```bash
dotnet run -- "source..." "SourceDB" "target..." "TargetDB" "dbo.Users"
```

### Sync Multiple Tables/Schemas
```bash
dotnet run -- "source..." "SourceDB" "target..." "TargetDB" "dbo.Users, Sales, HR.Employees"
```

### Sync All Tables
```bash
dotnet run -- "source..." "SourceDB" "target..." "TargetDB"
```

### Sync with Options
```bash
dotnet run -- "source..." "SourceDB" "target..." "TargetDB" "dbo.Users" --batch-size 2000 --threads 4
```

### Clear Target and Bulk Insert
```bash
dotnet run -- "source..." "SourceDB" "target..." "TargetDB" "dbo.Users" --clear-target
```

### Using Connection Strings
```bash
dotnet run -- --source-conn "Server=..." --target-conn "Server=..." "dbo.Users"
```

### Using PowerShell
```powershell
.\run-sync.ps1 -SourceServer "source..." -SourceDb "SourceDB" -TargetServer "target..." -TargetDb "TargetDB" -TableName "dbo.Users" -Threads 4
```

## 📝 Authentication
When using server/database names, authentication uses **Azure Active Directory Interactive** (MFA supported).
When using connection strings (`--source-conn`/`--target-conn`), authentication is determined by the connection string.

## 🔧 Batch Size Guidelines
- Small records + Fast network: **2000-5000**
- Small records + Slow network: **500-1000**
- Large records + Fast network: **500-1000**
- Large records + Slow network: **100-500**

## ✅ Required Permissions
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

## 📁 Project Files
- **Program.cs** - Entry point with CLI parsing
- **AzureAdConnection.cs** - Azure AD connection logic
- **DatabaseSyncService.cs** - Core sync logic
- **SyncRunResult.cs** - Result models for JSON output
- **README.md** - Full documentation
- **USAGE_GUIDE.md** - Detailed usage examples
- **PROJECT_SUMMARY.md** - Technical details

## 🐛 Quick Troubleshooting
| Error              | Solution                           |
| ------------------ | ---------------------------------- |
| Connection timeout | Check firewall rules, whitelist IP |
| Permission denied  | Grant SELECT/INSERT permissions    |
| No primary key     | Use `--allow-no-pk --deep-compare` |
| Duplicate key      | Check schema matches exactly       |
| Target >= Source   | Use `--clear-target` to force sync |

## 📞 Getting Help
1. Check **README.md** for detailed docs
2. Review **USAGE_GUIDE.md** for examples
3. Check **PROJECT_SUMMARY.md** for technical info
4. Run with `--help` for command-line options
5. Review JSON result files in `results/` directory

---
**Project**: MSSQLDBSink | **Framework**: .NET 10.0
