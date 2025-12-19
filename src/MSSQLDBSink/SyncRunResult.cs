namespace MSSQLDBSink;

using System.Collections.Concurrent;

public class SyncRunResult
{
    public string RunId { get; set; } = Guid.NewGuid().ToString();
    public DateTime StartTime { get; set; } = DateTime.UtcNow;
    public DateTime? EndTime { get; set; }
    public SyncParameters Parameters { get; set; } = new();

    // Using ConcurrentDictionary to prevent duplicate table entries and support thread-safe access
    private ConcurrentDictionary<string, TableSyncResult> _tableResults = new(StringComparer.OrdinalIgnoreCase);

    public List<TableSyncResult> Tables
    {
        get => _tableResults.Values.ToList();
        set => _tableResults = new ConcurrentDictionary<string, TableSyncResult>(
            value.ToDictionary(t => t.TableName, t => t, StringComparer.OrdinalIgnoreCase));
    }

    public void AddOrUpdateTable(TableSyncResult tableResult)
    {
        _tableResults.AddOrUpdate(tableResult.TableName, tableResult, (key, existing) => tableResult);
    }

    public string Status { get; set; } = "Running"; // "Running", "Completed", "Failed"
}

public class SyncParameters
{
    public string? SourceServer { get; set; }
    public string? SourceDb { get; set; }
    public string? SourceConnectionString { get; set; }
    public string? TargetServer { get; set; }
    public string? TargetDb { get; set; }
    public string? TargetConnectionString { get; set; }
    public string? TableName { get; set; }
    public int BatchSize { get; set; }
    public int ThreadCount { get; set; }
    public bool AllowNoPk { get; set; }
    public bool DeepCompare { get; set; }
    public bool ClearTarget { get; set; }
    public bool TargetColumnsOnly { get; set; }

    /// <summary>
    /// Column mappings: TableName -> (SourceColumn -> TargetColumn)
    /// Format: "Schema.Table.SourceCol=TargetCol"
    /// </summary>
    public Dictionary<string, Dictionary<string, string>> ColumnMappings { get; set; } = new();
}

public class TableSyncResult
{
    public string TableName { get; set; } = string.Empty;
    public string Status { get; set; } = "Pending"; // "Completed", "Failed", "Skipped", "Pending"
    public int SourceCount { get; set; }
    public int TargetCount { get; set; }
    public int Inserted { get; set; }
    public int Skipped { get; set; }
    public string? ErrorMessage { get; set; }
    public string? ErrorDetails { get; set; } // Full exception details including stack trace
    public string? ErrorType { get; set; } // Exception type name
    public double DurationSeconds { get; set; }
    public DateTime? StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public SchemaErrors? SchemaErrors { get; set; }
}

public class SchemaErrors
{
    public List<string> MissingColumnsInTarget { get; set; } = new();
    public List<string> MissingColumnsInSource { get; set; } = new();
    public List<string> ExcludedColumns { get; set; } = new(); // Generated/computed columns
    public List<string> CommonColumns { get; set; } = new(); // Columns used for sync
    public string? SchemaMismatchDetails { get; set; }
}

