# Wiki Documentation

This folder contains detailed documentation for the MSSQLDBSink project.

## 🔄 Wiki Synchronization

This `wiki/` folder is automatically synced to the GitHub wiki at `https://github.com/hpractv/ms-sql-db-sink/wiki` via GitHub Actions. Any changes pushed to the main branch will automatically update the public wiki.

**To update the wiki:**
1. Edit files in this folder
2. Commit and push to main
3. The sync happens automatically via GitHub Actions

## 📚 Documentation Files

- **[Home.md](Home.md)** - Wiki home page with overview and navigation links
- **[Project-Summary.md](Project-Summary)** - Technical architecture, features, and implementation details
- **[Usage-Guide.md](Usage-Guide)** - Comprehensive usage examples, scenarios, and best practices

## 🔗 Quick Links

### Getting Started
- [Installation and Setup](Usage-Guide#installation-and-setup)
- [Quick Start](Usage-Guide#quick-start)
- [Command Line Usage](Usage-Guide#command-line-usage)

### Technical Information
- [Key Features](Project-Summary#key-features)
- [How It Works](Project-Summary#how-it-works)
- [Performance Guidelines](Project-Summary#configuration-options)

### Help
- [Common Scenarios](Usage-Guide#common-scenarios)
- [Best Practices](Usage-Guide#best-practices)
- [Troubleshooting](Usage-Guide#troubleshooting)

## 📖 Documentation Hierarchy

```
Repository Root
├── README.md                    # Main overview and quick start
├── QUICK_REFERENCE.md           # Common commands and quick tips
├── PROJECT_SUMMARY.md           # Deprecated - redirects to wiki
├── USAGE_GUIDE.md               # Deprecated - redirects to wiki
└── wiki/                        # Detailed documentation (you are here)
    ├── Home.md                  # Wiki landing page
    ├── Project-Summary.md       # Enhanced technical docs
    └── Usage-Guide.md           # Enhanced usage guide
```

## ⚙️ Using This Documentation

### For Quick Reference
Start with the [main README](../README.md) or [Quick Reference](../QUICK_REFERENCE.md).

### For Detailed Learning
Start with [Wiki Home](Home.md) and follow the links to specific topics.

### For Troubleshooting
Go directly to the [Troubleshooting section](Usage-Guide#troubleshooting).

### For Development
Review [Project Summary](Project-Summary) for technical details.

---

**Note**: All documentation in this wiki uses compiled executable examples (`./MSSQLDBSink`) rather than `dotnet run` commands for production use. Development instructions using `dotnet run` are also provided where appropriate.
