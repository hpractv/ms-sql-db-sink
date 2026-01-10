using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using MSSQLDBSink;
using McMaster.Extensions.CommandLineUtils;
using Spectre.Console;

namespace MSSQLDBSink;

[Command(Name = "MSSQLDBSink", Description = "Synchronizes records between SQL Server databases.")]
[HelpOption("-?|-h|--help")]
public class Program
{
    public static int Main(string[] args) => CommandLineApplication.Execute<Program>(args);

    [Argument(0, Description = "Source Server (e.g., source.database.windows.net) - Optional if --source-conn provided")]
    public string? SourceServer { get; set; }

    [Argument(1, Description = "Source Database Name - Optional if --source-conn provided")]
    public string? SourceDb { get; set; }

    [Argument(2, Description = "Target Server (e.g., target.database.windows.net) - Optional if --target-conn provided")]
    public string? TargetServer { get; set; }

    [Argument(3, Description = "Target Database Name - Optional if --target-conn provided")]
    public string? TargetDb { get; set; }

    [Argument(4, Description = "Optional: Table name(s) to sync. Comma-separated. Use 'all' or '*' for all tables.")]
    public string? TableName { get; set; }

    [Option("-b|--batch-size", Description = "Records per batch (default: 100000)")]
    public int BatchSize { get; set; } = 100_000;

    [Option("-t|--threads", Description = "Degree of parallelism for table syncing (default: 4)")]
    public int ThreadCount { get; set; } = 4;

    [Option("--source-conn", Description = "Full connection string for source database (overrides server/db arguments)")]
    public string? SourceConnectionString { get; set; }

    [Option("--target-conn", Description = "Full connection string for target database (overrides server/db arguments)")]
    public string? TargetConnectionString { get; set; }

    [Option("--allow-no-pk", Description = "Allow processing tables without primary keys")]
    public bool AllowNoPk { get; set; }

    [Option("--deep-compare", Description = "Use all columns as composite key for comparison (requires --allow-no-pk)")]
    public bool DeepCompare { get; set; }

    [Option("--clear-target", Description = "Truncate target table and bulk insert (fast, no comparison)")]
    public bool ClearTarget { get; set; }

    [Option("--target-columns-only", Description = "Import only columns that exist in target (default: false)")]
    public bool TargetColumnsOnly { get; set; }

    [Option("--ignore-column", Description = "Column(s) to ignore/skip during sync. Format: Schema.Table.Column or Table.Column or just Column (can specify multiple)")]
    public string[]? IgnoreColumn { get; set; }

    [Option("--map-column", Description = "Map source column to target column. Format: Schema.Table.SourceCol=TargetCol (can specify multiple)")]
    public string[]? MapColumn { get; set; }

    [Option("--start-row", Description = "Starting row number(s) to skip for each table. Comma-separated list matching table order (e.g., '0,1000,500' for 3 tables)")]
    public string? StartRow { get; set; }

    [Option("--order-by-pk", Description = "Order source data by primary keys for consistent continuation (default: false, will default to true in next major version)")]
    public bool OrderByPrimaryKey { get; set; }

    [Option("-o|--output-dir", Description = "Directory for saving JSON results (default: results)")]
    public string OutputDir { get; set; } = "results";

    [Option("--compare-counts-and-schema", Description = "Compare record counts and schema differences between source and target without syncing (read-only mode)")]
    public bool CompareCountsAndSchema { get; set; }

    private async Task OnExecuteAsync()
    {
        AnsiConsole.Write(new FigletText("MS SQL DB Sink").Color(Color.Cyan1));
        AnsiConsole.WriteLine();

        // Validate arguments
        if (string.IsNullOrWhiteSpace(SourceConnectionString) && (string.IsNullOrWhiteSpace(SourceServer) || string.IsNullOrWhiteSpace(SourceDb)))
        {
            AnsiConsole.MarkupLine("[red]Error:[/] Either provide --source-conn or both SourceServer and SourceDb arguments.");
            return;
        }

        if (string.IsNullOrWhiteSpace(TargetConnectionString) && (string.IsNullOrWhiteSpace(TargetServer) || string.IsNullOrWhiteSpace(TargetDb)))
        {
            AnsiConsole.MarkupLine("[red]Error:[/] Either provide --target-conn or both TargetServer and TargetDb arguments.");
            return;
        }

        if (DeepCompare && !AllowNoPk)
        {
            AnsiConsole.MarkupLine("[red]Error:[/] --deep-compare requires --allow-no-pk to be set.");
            return;
        }

        string sourceConnStr;
        string targetConnStr;

        if (!string.IsNullOrWhiteSpace(SourceConnectionString))
        {
            sourceConnStr = SourceConnectionString;
            AnsiConsole.MarkupLine($"[cyan]Source:[/] Connection String provided");
        }
        else
        {
            sourceConnStr = GetConnectionString(SourceServer!, SourceDb!);
            var builder = new SqlConnectionStringBuilder(sourceConnStr);
            var authInfo = builder.Authentication != SqlAuthenticationMethod.NotSpecified
                ? builder.Authentication.ToString()
                : (builder.IntegratedSecurity ? "Integrated Security" : "SQL Auth");

            AnsiConsole.MarkupLine($"[cyan]Source:[/] {SourceServer} ([grey]{SourceDb}[/]) - [green]{authInfo}[/]");
        }

        if (!string.IsNullOrWhiteSpace(TargetConnectionString))
        {
            targetConnStr = TargetConnectionString;
            AnsiConsole.MarkupLine($"[cyan]Target:[/] Connection String provided");
        }
        else
        {
            targetConnStr = GetConnectionString(TargetServer!, TargetDb!);
            var builder = new SqlConnectionStringBuilder(targetConnStr);
            var authInfo = builder.Authentication != SqlAuthenticationMethod.NotSpecified
                ? builder.Authentication.ToString()
                : (builder.IntegratedSecurity ? "Integrated Security" : "SQL Auth");

            AnsiConsole.MarkupLine($"[cyan]Target:[/] {TargetServer} ([grey]{TargetDb}[/]) - [green]{authInfo}[/]");
        }

        var info = new Table();
        info.AddColumn("Setting");
        info.AddColumn("Value");
        info.AddRow("[cyan]Batch Size[/]", $"{BatchSize:N0}");
        info.AddRow("[cyan]Parallel Threads[/]", $"{ThreadCount}");
        info.AddRow("[cyan]Allow No PK[/]", AllowNoPk ? "[green]Yes[/]" : "[red]No[/]");
        info.AddRow("[cyan]Deep Compare[/]", DeepCompare ? "[green]Yes[/]" : "[red]No[/]");
        info.AddRow("[cyan]Clear Target[/]", ClearTarget ? "[green]Yes[/]" : "[red]No[/]");
        info.AddRow("[cyan]Target Columns Only[/]", TargetColumnsOnly ? "[green]Yes[/]" : "[red]No[/]");
        info.AddRow("[cyan]Ignored Columns[/]", IgnoreColumn?.Length > 0 ? $"{IgnoreColumn.Length} column(s)" : "[grey]None[/]");
        info.AddRow("[cyan]Column Mappings[/]", MapColumn?.Length > 0 ? $"{MapColumn.Length} mapping(s)" : "[grey]None[/]");
        info.AddRow("[cyan]Start Row Offsets[/]", !string.IsNullOrWhiteSpace(StartRow) ? StartRow : "[grey]None[/]");
        info.AddRow("[cyan]Order By Primary Key[/]", OrderByPrimaryKey ? "[green]Yes[/]" : "[red]No[/]");
        info.AddRow("[cyan]Compare Counts & Schema[/]", CompareCountsAndSchema ? "[green]Yes[/]" : "[red]No[/]");
        info.AddRow("[cyan]Output Directory[/]", OutputDir);
        AnsiConsole.Write(info);
        AnsiConsole.WriteLine();

        // Parse column mappings
        var columnMappings = ParseColumnMappings(MapColumn);
        // Parse ignored columns
        var ignoredColumns = ParseIgnoredColumns(IgnoreColumn);
        // Parse start row offsets
        var startRowOffsets = ParseStartRowOffsets(StartRow);

        if (columnMappings.Count > 0)
        {
            AnsiConsole.MarkupLine("[cyan]Column Mappings:[/]");
            foreach (var tableMapping in columnMappings)
            {
                foreach (var colMapping in tableMapping.Value)
                {
                    AnsiConsole.MarkupLine($"  [grey]{tableMapping.Key}:[/] {colMapping.Key} → {colMapping.Value}");
                }
            }
            AnsiConsole.WriteLine();
        }

        if (ignoredColumns.Count > 0)
        {
            AnsiConsole.MarkupLine("[cyan]Ignored Columns:[/]");
            foreach (var tableIgnore in ignoredColumns)
            {
                if (tableIgnore.Key == "*")
                {
                    AnsiConsole.MarkupLine($"  [grey]All Tables:[/] {string.Join(", ", tableIgnore.Value)}");
                }
                else
                {
                    AnsiConsole.MarkupLine($"  [grey]{tableIgnore.Key}:[/] {string.Join(", ", tableIgnore.Value)}");
                }
            }
            AnsiConsole.WriteLine();
        }

        if (startRowOffsets.Count > 0)
        {
            AnsiConsole.MarkupLine("[cyan]Start Row Offsets:[/]");
            for (int i = 0; i < startRowOffsets.Count; i++)
            {
                AnsiConsole.MarkupLine($"  [grey]Table {i + 1}:[/] Skip first {startRowOffsets[i]:N0} rows");
            }
            AnsiConsole.WriteLine();
        }

        // Ensure output directory exists
        Directory.CreateDirectory(OutputDir);

        var syncService = new DatabaseSyncService(
            sourceConnStr,
            targetConnStr,
            BatchSize,
            AllowNoPk,
            DeepCompare,
            ClearTarget,
            TargetColumnsOnly,
            OutputDir);

        try
        {
            var tableSelections = string.IsNullOrWhiteSpace(TableName)
                ? new[] { "all" }
                : TableName.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            if (CompareCountsAndSchema)
            {
                // Compare counts and schema mode - read-only operation
                await syncService.CompareCountsAndSchemaAsync(tableSelections);
                AnsiConsole.MarkupLine("\n[green]✓[/] Count and schema comparison completed successfully!");
            }
            else
            {
                // Normal sync mode
                var parameters = new SyncParameters
                {
                    SourceServer = SourceServer,
                    SourceDb = SourceDb,
                    SourceConnectionString = SourceConnectionString,
                    TargetServer = TargetServer,
                    TargetDb = TargetDb,
                    TargetConnectionString = TargetConnectionString,
                    TableName = TableName,
                    BatchSize = BatchSize,
                    ThreadCount = ThreadCount,
                    AllowNoPk = AllowNoPk,
                    DeepCompare = DeepCompare,
                    ClearTarget = ClearTarget,
                    TargetColumnsOnly = TargetColumnsOnly,
                    ColumnMappings = columnMappings,
                    IgnoredColumns = ignoredColumns,
                    StartRowOffsets = startRowOffsets,
                    OrderByPrimaryKey = OrderByPrimaryKey
                };

                await syncService.SyncTablesAsync(tableSelections, ThreadCount, parameters);
                AnsiConsole.MarkupLine("\n[green]✓[/] Sync completed successfully!");
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"\n[red]✗[/] Error during sync: [red]{Markup.Escape(ex.Message)}[/]");
            AnsiConsole.WriteLine($"Stack trace: {ex.StackTrace}");
        }
    }

    /// <summary>
    /// Parses ignored column arguments into a dictionary structure.
    /// Format: "Schema.Table.Column" or "Table.Column" (dbo) or "Column" (all tables)
    /// </summary>
    private static Dictionary<string, HashSet<string>> ParseIgnoredColumns(string[]? ignores)
    {
        var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

        if (ignores == null || ignores.Length == 0)
            return result;

        foreach (var ignore in ignores)
        {
            if (string.IsNullOrWhiteSpace(ignore))
                continue;

            var parts = ignore.Trim().Split('.');
            string tableName;
            string columnName;

            if (parts.Length == 3)
            {
                // Schema.Table.Column
                tableName = $"{parts[0]}.{parts[1]}";
                columnName = parts[2];
            }
            else if (parts.Length == 2)
            {
                // Table.Column (assume dbo)
                tableName = $"dbo.{parts[0]}";
                columnName = parts[1];
            }
            else
            {
                // Just Column (applies to all tables)
                tableName = "*";
                columnName = parts[0];
            }

            if (!result.ContainsKey(tableName))
            {
                result[tableName] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            }
            result[tableName].Add(columnName);
        }

        return result;
    }

    /// <summary>
    /// Parses column mapping arguments into a dictionary structure.
    /// Format: "Schema.Table.SourceCol=TargetCol" or "Table.SourceCol=TargetCol" (assumes dbo schema)
    /// </summary>
    private static Dictionary<string, Dictionary<string, string>> ParseColumnMappings(string[]? mappings)
    {
        var result = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);

        if (mappings == null || mappings.Length == 0)
            return result;

        foreach (var mapping in mappings)
        {
            if (string.IsNullOrWhiteSpace(mapping))
                continue;

            // Split on '=' to get the column mapping part
            var parts = mapping.Split('=', 2);
            if (parts.Length != 2)
            {
                AnsiConsole.MarkupLine($"[yellow]Warning:[/] Invalid column mapping format: '{mapping}'. Expected 'Table.SourceCol=TargetCol'");
                continue;
            }

            var leftSide = parts[0].Trim();
            var targetColumn = parts[1].Trim();

            // Parse the left side: could be "Schema.Table.Column" or "Table.Column"
            var leftParts = leftSide.Split('.');

            string tableName;
            string sourceColumn;

            if (leftParts.Length == 3)
            {
                // Schema.Table.Column
                tableName = $"{leftParts[0]}.{leftParts[1]}";
                sourceColumn = leftParts[2];
            }
            else if (leftParts.Length == 2)
            {
                // Table.Column (assume dbo schema)
                tableName = $"dbo.{leftParts[0]}";
                sourceColumn = leftParts[1];
            }
            else
            {
                AnsiConsole.MarkupLine($"[yellow]Warning:[/] Invalid column mapping format: '{mapping}'. Expected 'Table.SourceCol=TargetCol' or 'Schema.Table.SourceCol=TargetCol'");
                continue;
            }

            if (!result.ContainsKey(tableName))
            {
                result[tableName] = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            }

            result[tableName][sourceColumn] = targetColumn;
        }

        return result;
    }

    /// <summary>
    /// Parses start row offset argument into a list of integers.
    /// Format: "0,1000,500" - comma-separated list of row numbers to skip for each table
    /// The order matches the order of tables being synced
    /// </summary>
    public static List<int> ParseStartRowOffsets(string? startRow)
    {
        var result = new List<int>();

        if (string.IsNullOrWhiteSpace(startRow))
            return result;

        var parts = startRow.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        foreach (var part in parts)
        {
            if (int.TryParse(part, out int offset))
            {
                if (offset < 0)
                {
                    AnsiConsole.MarkupLine($"[yellow]Warning:[/] Invalid start row offset '{part}'. Must be non-negative. Using 0 instead.");
                    result.Add(0);
                }
                else
                {
                    result.Add(offset);
                }
            }
            else
            {
                AnsiConsole.MarkupLine($"[yellow]Warning:[/] Invalid start row offset '{part}'. Using 0 instead.");
                result.Add(0);
            }
        }

        return result;
    }

    private string GetConnectionString(string server, string database)
    {
        // Check if it's likely an Azure SQL Database
        if (server.Contains(".database.windows.net", StringComparison.OrdinalIgnoreCase))
        {
            return new AzureAdConnection(server, database).ConnectionString;
        }

        // For local/on-prem, default to Integrated Security (Windows Auth)
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = server,
            InitialCatalog = database,
            IntegratedSecurity = true,
            TrustServerCertificate = true,
            Encrypt = false // Often needed for local dev
        };
        return builder.ConnectionString;
    }
}
