using System.Data;
using System.Diagnostics;
using System.Text.Json;
using Dapper;
using Microsoft.Data.SqlClient;
using Polly;
using System.Collections.Concurrent;
using Spectre.Console;
using ConsoleProgressBar;
using SQLServerSink.Interfaces;

namespace SQLServerSink;

public class DatabaseSyncService : IDatabaseSyncService
{
    private readonly string _sourceConnectionString;
    private readonly string _targetConnectionString;
    private readonly int _batchSize;
    private readonly bool _allowNoPk;
    private readonly bool _deepCompare;
    private readonly bool _clearTarget;
    private readonly bool _targetColumnsOnly;
    private readonly string _outputDir;
    private SyncRunResult? _runResult;
    private readonly object _resultLock = new object();
    private const int _commandTimeout = 0; // Infinite timeout

    // Column mappings: TableName -> (SourceColumn -> TargetColumn)
    private Dictionary<string, Dictionary<string, string>> _columnMappings = new(StringComparer.OrdinalIgnoreCase);

    public DatabaseSyncService(
        string sourceConnectionString,
        string targetConnectionString,
        int batchSize = 100_000,
        bool allowNoPk = false,
        bool deepCompare = false,
        bool clearTarget = false,
        bool targetColumnsOnly = false,
        string outputDir = "results")
    {
        _sourceConnectionString = EnsureReadOnly(EnsureConnectionTimeout(sourceConnectionString));
        _targetConnectionString = EnsureConnectionTimeout(targetConnectionString);
        _batchSize = batchSize;
        _allowNoPk = allowNoPk;
        _deepCompare = deepCompare;
        _clearTarget = clearTarget;
        _targetColumnsOnly = targetColumnsOnly;
        _outputDir = outputDir;
    }

    private string EnsureConnectionTimeout(string connectionString)
    {
        try
        {
            if (connectionString.Contains("Timeout=", StringComparison.OrdinalIgnoreCase) ||
                connectionString.Contains("Connect Timeout", StringComparison.OrdinalIgnoreCase) ||
                connectionString.Contains("Connection Timeout", StringComparison.OrdinalIgnoreCase))
            {
                return connectionString;
            }

            var builder = new SqlConnectionStringBuilder(connectionString);
            builder.ConnectTimeout = 0;
            return builder.ConnectionString;
        }
        catch
        {
            return connectionString;
        }
    }

    private string EnsureReadOnly(string connectionString)
    {
        try
        {
            var builder = new SqlConnectionStringBuilder(connectionString);
            builder.ApplicationIntent = ApplicationIntent.ReadOnly;
            return builder.ConnectionString;
        }
        catch
        {
            return connectionString;
        }
    }

    /// <summary>
    /// Compares record counts and schema differences between source and target databases for selected tables.
    /// </summary>
    public async Task CompareCountsAndSchemaAsync(IEnumerable<string> selections)
    {
        AnsiConsole.MarkupLine("[cyan]Fetching list of tables from source database...[/]");

        var tablesToCompare = await GetTablesToSyncAsync(selections);

        if (tablesToCompare.Count == 0)
        {
            AnsiConsole.WriteLine("[yellow]⚠[/] No tables found matching the criteria.\n");
            return;
        }

        AnsiConsole.MarkupLine($"[cyan]Found {tablesToCompare.Count} tables to compare.[/]\n");

        var comparisonResults = new List<(string TableName, int SourceCount, int TargetCount, bool CountMatch, List<string> MissingInTarget, List<string> MissingInSource, bool SchemaMatch)>();
        var retryPolicy = GetRetryPolicy();

        // Fetch counts and schema for all tables
        foreach (var tableInfo in tablesToCompare)
        {
            var tableName = $"{tableInfo.SchemaName}.{tableInfo.TableName}";

            try
            {
                // Get counts
                var sourceCount = await retryPolicy.ExecuteAsync(async () =>
                {
                    using var conn = new SqlConnection(_sourceConnectionString);
                    await conn.OpenAsync();
                    return await CountRecordsAsync(conn, tableName);
                });

                var targetCount = await retryPolicy.ExecuteAsync(async () =>
                {
                    using var conn = new SqlConnection(_targetConnectionString);
                    await conn.OpenAsync();
                    return await CountRecordsAsync(conn, tableName);
                });

                // Get schema (columns)
                var sourceColumns = await retryPolicy.ExecuteAsync(async () =>
                {
                    using var conn = new SqlConnection(_sourceConnectionString);
                    await conn.OpenAsync();
                    return await GetColumnNamesAsync(conn, tableName);
                });

                var targetColumns = await retryPolicy.ExecuteAsync(async () =>
                {
                    using var conn = new SqlConnection(_targetConnectionString);
                    await conn.OpenAsync();
                    return await GetColumnNamesAsync(conn, tableName);
                });

                // Compare schemas
                var sourceColSet = new HashSet<string>(sourceColumns, StringComparer.OrdinalIgnoreCase);
                var targetColSet = new HashSet<string>(targetColumns, StringComparer.OrdinalIgnoreCase);

                var missingInTarget = sourceColumns.Where(c => !targetColSet.Contains(c)).ToList();
                var missingInSource = targetColumns.Where(c => !sourceColSet.Contains(c)).ToList();
                var schemaMatch = !missingInTarget.Any() && !missingInSource.Any();

                comparisonResults.Add((tableName, sourceCount, targetCount, sourceCount == targetCount, missingInTarget, missingInSource, schemaMatch));
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]✗ Error comparing {Markup.Escape(tableName)}: {Markup.Escape(ex.Message)}[/]");
                comparisonResults.Add((tableName, -1, -1, false, new List<string>(), new List<string>(), false));
            }
        }

        // Display results
        AnsiConsole.WriteLine();
        var table = new Table();
        table.Title = new TableTitle("[bold cyan]Table Count & Schema Comparison[/]");
        table.AddColumn("Table Name");
        table.AddColumn(new TableColumn("Source Count").RightAligned());
        table.AddColumn(new TableColumn("Target Count").RightAligned());
        table.AddColumn(new TableColumn("Difference").RightAligned());
        table.AddColumn("Schema Diff");
        table.AddColumn("Status");

        int matchCount = 0;
        int countMismatchCount = 0;
        int schemaMismatchCount = 0;
        int bothMismatchCount = 0;
        int errorCount = 0;

        foreach (var result in comparisonResults.OrderBy(r => r.TableName))
        {
            if (result.SourceCount == -1 || result.TargetCount == -1)
            {
                // Error case
                table.AddRow(
                    $"[red]{Markup.Escape(result.TableName)}[/]",
                    "[red]ERROR[/]",
                    "[red]ERROR[/]",
                    "[red]N/A[/]",
                    "[red]ERROR[/]",
                    "[red]✗ Error[/]"
                );
                errorCount++;
            }
            else if (result.CountMatch && result.SchemaMatch)
            {
                // Perfect match - counts and schema
                table.AddRow(
                    $"[gray]{Markup.Escape(result.TableName)}[/]",
                    $"[gray]{result.SourceCount:N0}[/]",
                    $"[gray]{result.TargetCount:N0}[/]",
                    $"[gray]0[/]",
                    "[green]✓[/]",
                    "[green]✓ Match[/]"
                );
                matchCount++;
            }
            else
            {
                // Some kind of mismatch
                var diff = result.SourceCount - result.TargetCount;
                var diffStr = diff > 0 ? $"+{diff:N0}" : diff.ToString("N0");

                // Build schema diff summary
                var schemaDiffParts = new List<string>();
                if (result.MissingInTarget.Any())
                {
                    schemaDiffParts.Add($"Missing in target: {result.MissingInTarget.Count}");
                }
                if (result.MissingInSource.Any())
                {
                    schemaDiffParts.Add($"Extra in target: {result.MissingInSource.Count}");
                }
                var schemaDiff = schemaDiffParts.Any() ? string.Join(", ", schemaDiffParts) : "✓";

                // Determine color based on type of mismatch
                bool hasCountMismatch = !result.CountMatch;
                bool hasSchemaMismatch = !result.SchemaMatch;

                if (hasCountMismatch && hasSchemaMismatch)
                {
                    table.AddRow(
                        $"[red]{Markup.Escape(result.TableName)}[/]",
                        $"[red]{result.SourceCount:N0}[/]",
                        $"[red]{result.TargetCount:N0}[/]",
                        $"[red]{diffStr}[/]",
                        $"[red]{Markup.Escape(schemaDiff)}[/]",
                        "[red]✗ Both[/]"
                    );
                    bothMismatchCount++;
                }
                else if (hasSchemaMismatch)
                {
                    table.AddRow(
                        $"[yellow]{Markup.Escape(result.TableName)}[/]",
                        $"[gray]{result.SourceCount:N0}[/]",
                        $"[gray]{result.TargetCount:N0}[/]",
                        $"[gray]{diffStr}[/]",
                        $"[yellow]{Markup.Escape(schemaDiff)}[/]",
                        "[yellow]⚠ Schema[/]"
                    );
                    schemaMismatchCount++;
                }
                else // hasCountMismatch only
                {
                    table.AddRow(
                        $"[yellow]{Markup.Escape(result.TableName)}[/]",
                        $"[yellow]{result.SourceCount:N0}[/]",
                        $"[yellow]{result.TargetCount:N0}[/]",
                        $"[yellow]{diffStr}[/]",
                        $"[green]✓[/]",
                        "[yellow]⚠ Count[/]"
                    );
                    countMismatchCount++;
                }
            }
        }

        AnsiConsole.Write(table);
        AnsiConsole.WriteLine();

        // Summary
        var summaryTable = new Table();
        summaryTable.AddColumn("Summary");
        summaryTable.AddColumn("Count");
        summaryTable.AddRow("[green]✓ Perfect Match[/]", $"[green]{matchCount}[/]");
        summaryTable.AddRow("[yellow]⚠ Count Mismatch[/]", $"[yellow]{countMismatchCount}[/]");
        summaryTable.AddRow("[yellow]⚠ Schema Mismatch[/]", $"[yellow]{schemaMismatchCount}[/]");
        summaryTable.AddRow("[red]✗ Both Mismatched[/]", $"[red]{bothMismatchCount}[/]");
        if (errorCount > 0)
        {
            summaryTable.AddRow("[red]✗ Errors[/]", $"[red]{errorCount}[/]");
        }
        summaryTable.AddRow("[cyan]Total Tables[/]", $"[cyan]{comparisonResults.Count}[/]");

        AnsiConsole.Write(summaryTable);
        AnsiConsole.WriteLine();

        // Show detailed schema differences if any
        var tablesWithSchemaDiff = comparisonResults.Where(r => !r.SchemaMatch && r.SourceCount != -1).ToList();
        if (tablesWithSchemaDiff.Any())
        {
            AnsiConsole.MarkupLine("[yellow]Schema Differences Details:[/]");
            foreach (var result in tablesWithSchemaDiff.OrderBy(r => r.TableName))
            {
                AnsiConsole.MarkupLine($"\n[cyan]{Markup.Escape(result.TableName)}:[/]");
                if (result.MissingInTarget.Any())
                {
                    AnsiConsole.MarkupLine($"  [yellow]Missing in target ({result.MissingInTarget.Count}):[/] {string.Join(", ", result.MissingInTarget.Select(Markup.Escape))}");
                }
                if (result.MissingInSource.Any())
                {
                    AnsiConsole.MarkupLine($"  [yellow]Extra in target ({result.MissingInSource.Count}):[/] {string.Join(", ", result.MissingInSource.Select(Markup.Escape))}");
                }
            }
            AnsiConsole.WriteLine();
        }
    }

    public async Task SyncTablesAsync(IEnumerable<string> selections, int threadCount = 1, SyncParameters? parameters = null)
    {
        _runResult = new SyncRunResult
        {
            Parameters = parameters ?? new SyncParameters(),
            StartTime = DateTime.UtcNow
        };

        // Store column mappings from parameters
        _columnMappings = parameters?.ColumnMappings ?? new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);

        AnsiConsole.WriteLine("Fetching list of tables from source database...");

        var tablesToSync = await GetTablesToSyncAsync(selections);

        if (tablesToSync.Count == 0)
        {
            AnsiConsole.WriteLine("[yellow]⚠[/] No tables found matching the criteria.\n");
            _runResult.Status = "Completed";
            _runResult.EndTime = DateTime.UtcNow;
            await SaveResultFileAsync();
            return;
        }

        AnsiConsole.WriteLine($"Found {tablesToSync.Count} tables to sync.\n");

        int successCount = 0;
        int failCount = 0;
        int completedTables = 0;
        object progressLock = new();

        // Analyze dependencies and sort tables by execution level
        AnsiConsole.MarkupLine("[cyan]Analyzing table dependencies...[/]");
        var dependencies = await GetTableDependenciesAsync(tablesToSync);
        var executionLevels = TopologicalSort(tablesToSync, dependencies);

        AnsiConsole.MarkupLine($"[green]✓[/] Organized {tablesToSync.Count} tables into {executionLevels.Count} execution level(s)\n");

        // If ClearTarget is enabled, disable global constraints and temporal tables
        List<string> constraintDisableErrors = new();
        List<string> constraintEnableErrors = new();
        List<string> temporalEnableErrors = new();
        Dictionary<string, string> disabledTemporalTables = new();

        if (_clearTarget)
        {
            AnsiConsole.MarkupLine("[yellow]⚠ Disabling all foreign key constraints on target for bulk load...[/]");
            constraintDisableErrors = await ToggleGlobalConstraintsAsync(false);

            AnsiConsole.MarkupLine("[yellow]⚠ Disabling SYSTEM_VERSIONING for temporal tables...[/]");
            disabledTemporalTables = await DisableAllTemporalTablesAsync(tablesToSync);
        }

        var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = threadCount };

        using (var progressBar = new ProgressBar())
        {
            progressBar.HierarchicalProgress.Report(0, $"Starting sync of {tablesToSync.Count} table(s)...");

            try
            {
                // Process tables level by level (parallel within level, sequential across levels)
                for (int levelIndex = 0; levelIndex < executionLevels.Count; levelIndex++)
                {
                    var level = executionLevels[levelIndex];

                    if (executionLevels.Count > 1)
                    {
                        AnsiConsole.MarkupLine($"[cyan]Processing Level {levelIndex + 1}/{executionLevels.Count} ({level.Count} tables)...[/]");
                    }

                    await Parallel.ForEachAsync(level, parallelOptions, async (table, ct) =>
                    {
                        string fullTableName = $"{table.SchemaName}.{table.TableName}";

                        try
                        {
                            AnsiConsole.MarkupLine($"[gray]→ Starting {Markup.Escape(fullTableName)}[/]");
                            await SyncTableAsync(fullTableName, progressTask: null);

                            lock (progressLock)
                            {
                                completedTables++;
                                successCount++;
                                var progress = (double)completedTables / tablesToSync.Count;
                                progressBar.HierarchicalProgress.Report(progress, $"Completed {completedTables}/{tablesToSync.Count} tables");
                            }
                        }
                        catch (Exception ex)
                        {
                            lock (progressLock)
                            {
                                completedTables++;
                                failCount++;
                                var progress = (double)completedTables / tablesToSync.Count;
                                progressBar.HierarchicalProgress.Report(progress, $"Completed {completedTables}/{tablesToSync.Count} tables (with failures)");
                            }

                            AnsiConsole.MarkupLine($"[red]✗ {Markup.Escape(fullTableName)} failed:[/] [gray]{Markup.Escape(ex.Message)}[/]");
                        }
                    });
                }
            }
            finally
            {
                // Re-enable temporal tables and constraints
                if (_clearTarget)
                {
                    if (disabledTemporalTables.Any())
                    {
                        AnsiConsole.MarkupLine("[yellow]⚠ Re-enabling SYSTEM_VERSIONING for temporal tables...[/]");
                        temporalEnableErrors = await EnableAllTemporalTablesAsync(disabledTemporalTables);
                    }

                    AnsiConsole.MarkupLine("[yellow]⚠ Re-enabling all foreign key constraints...[/]");
                    constraintEnableErrors = await ToggleGlobalConstraintsAsync(true);
                }

                progressBar.HierarchicalProgress.Report(1.0, "Sync complete");
            }
        }

        _runResult.Status = "Completed";
        _runResult.EndTime = DateTime.UtcNow;

        // Wait a moment for progress bars to finish
        await Task.Delay(500);

        // Clear progress and show final summary
        AnsiConsole.Clear();

        var summaryTable = new Table();
        summaryTable.Title = new TableTitle("[bold green]✓ Sync Completed[/]");
        summaryTable.AddColumn("Status");
        summaryTable.AddColumn("Count");

        summaryTable.AddRow("[green]✓ Successful[/]", $"[green]{successCount}[/]");
        summaryTable.AddRow("[red]✗ Failed[/]", $"[red]{failCount}[/]");
        summaryTable.AddRow("[yellow]⚠ Skipped[/]", $"[yellow]{_runResult.Tables.Count(t => t.Status == "Skipped")}[/]");
        summaryTable.AddRow("[cyan]Total Tables[/]", $"[cyan]{tablesToSync.Count}[/]");

        AnsiConsole.Write(summaryTable);
        AnsiConsole.WriteLine();

        // Show constraint and temporal table errors if any
        if (constraintDisableErrors.Any() || constraintEnableErrors.Any() || temporalEnableErrors.Any())
        {
            AnsiConsole.MarkupLine("[yellow]Constraint & Temporal Table Management Messages:[/]");

            if (constraintDisableErrors.Any())
            {
                foreach (var error in constraintDisableErrors)
                {
                    AnsiConsole.MarkupLine($"[gray]  {Markup.Escape(error)}[/]");
                }
            }

            if (temporalEnableErrors.Any())
            {
                foreach (var error in temporalEnableErrors)
                {
                    AnsiConsole.MarkupLine($"[gray]  {Markup.Escape(error)}[/]");
                }
            }

            if (constraintEnableErrors.Any())
            {
                foreach (var error in constraintEnableErrors)
                {
                    AnsiConsole.MarkupLine($"[gray]  {Markup.Escape(error)}[/]");
                }
            }

            AnsiConsole.WriteLine();
        }

        // Show failed tables if any
        var failedTables = _runResult.Tables.Where(t => t.Status == "Failed").ToList();
        if (failedTables.Any())
        {
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[red]Failed Tables:[/]");
            var failedTable = new Table();
            failedTable.AddColumn("Table");
            failedTable.AddColumn("Error");
            foreach (var failed in failedTables)
            {
                var errorMsg = failed.ErrorMessage ?? "Unknown error";
                if (errorMsg.Length > 100) errorMsg = errorMsg.Substring(0, 97) + "...";
                // Escape table names to prevent Spectre.Console from interpreting schema names as color codes
                var escapedTableName = Markup.Escape(failed.TableName);
                var escapedErrorMsg = Markup.Escape(errorMsg);
                failedTable.AddRow($"[red]{escapedTableName}[/]", $"[gray]{escapedErrorMsg}[/]");
            }
            AnsiConsole.Write(failedTable);
            AnsiConsole.WriteLine();
        }

        AnsiConsole.MarkupLine($"[green]✓[/] Sync completed at [cyan]{DateTime.Now:yyyy-MM-dd HH:mm:ss}[/]");
        AnsiConsole.MarkupLine($"[gray]Results saved to: {_outputDir}[/]");

        await SaveResultFileAsync();
    }

    private async Task SaveResultFileAsync()
    {
        if (_runResult == null) return;

        try
        {
            Directory.CreateDirectory(_outputDir);
            var timestamp = _runResult.StartTime.ToString("yyyyMMdd_HHmmss");
            var fileName = Path.Combine(_outputDir, $"sync-result-{timestamp}.json");

            var options = new JsonSerializerOptions { WriteIndented = true };
            var json = JsonSerializer.Serialize(_runResult, options);

            await File.WriteAllTextAsync(fileName, json);
            // Console.WriteLine($"\nResults saved to: {fileName}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Warning: Failed to save result file: {ex.Message}");
        }
    }

    private async Task<List<TableInfo>> GetTablesToSyncAsync(IEnumerable<string> selections)
    {
        var retryPolicy = GetRetryPolicy();

        return await retryPolicy.ExecuteAsync(async () =>
        {
            using var sourceConnection = new SqlConnection(_sourceConnectionString);
            using var targetConnection = new SqlConnection(_targetConnectionString);
            await sourceConnection.OpenAsync();
            await targetConnection.OpenAsync();

            var allTables = await sourceConnection.QueryAsync<TableInfo>(
                @"SELECT
                    TABLE_SCHEMA as SchemaName,
                    TABLE_NAME as TableName
                  FROM INFORMATION_SCHEMA.TABLES
                  WHERE TABLE_TYPE = 'BASE TABLE'
                  ORDER BY TABLE_SCHEMA, TABLE_NAME", commandTimeout: _commandTimeout);

            // Get tables that exist in target database
            var targetTables = await targetConnection.QueryAsync<TableInfo>(
                @"SELECT
                    TABLE_SCHEMA as SchemaName,
                    TABLE_NAME as TableName
                  FROM INFORMATION_SCHEMA.TABLES
                  WHERE TABLE_TYPE = 'BASE TABLE'
                  ORDER BY TABLE_SCHEMA, TABLE_NAME", commandTimeout: _commandTimeout);

            var targetTableSet = new HashSet<string>(targetTables.Select(t => $"{t.SchemaName}.{t.TableName}"), StringComparer.OrdinalIgnoreCase);

            var tablesToSync = new List<TableInfo>();
            var selectionList = selections.Select(s => s.Trim()).ToList();

            if (selectionList.Any(s => s.Equals("all", StringComparison.OrdinalIgnoreCase) || s == "*"))
            {
                // Only include tables that exist in both databases
                tablesToSync = allTables
                    .Where(t => targetTableSet.Contains($"{t.SchemaName}.{t.TableName}"))
                    .ToList();
            }
            else
            {
                foreach (var table in allTables)
                {
                    if (IsTableMatch(table, selectionList))
                    {
                        // Only include if table exists in target database
                        if (targetTableSet.Contains($"{table.SchemaName}.{table.TableName}"))
                        {
                            tablesToSync.Add(table);
                        }
                    }
                }
            }
            return tablesToSync;
        });
    }

    private bool IsTableMatch(TableInfo table, List<string> selections)
    {
        string fullTableName = $"{table.SchemaName}.{table.TableName}";

        foreach (var selection in selections)
        {
            if (fullTableName.Equals(selection, StringComparison.OrdinalIgnoreCase)) return true;
            if (table.SchemaName.Equals(selection, StringComparison.OrdinalIgnoreCase)) return true;
            if (!selection.Contains('.') &&
                table.SchemaName.Equals("dbo", StringComparison.OrdinalIgnoreCase) &&
                table.TableName.Equals(selection, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    private string GetDisplayName(string tableName, int maxLength = 45)
    {
        return tableName.Length > maxLength ? tableName.Substring(0, maxLength - 3) + "..." : tableName;
    }

    /// <summary>
    /// Gets column mappings for a specific table (SourceColumn -> TargetColumn).
    /// </summary>
    private Dictionary<string, string> GetTableColumnMappings(string tableName)
    {
        if (_columnMappings.TryGetValue(tableName, out var mappings))
        {
            return mappings;
        }
        return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Applies column mappings to source columns and returns a tuple of:
    /// - List of target column names (to use for INSERT)
    /// - Dictionary mapping target column names to original source column names (for SELECT)
    /// </summary>
    private (List<string> targetColumns, Dictionary<string, string> targetToSourceMap) ApplyColumnMappings(
        List<string> sourceColumns,
        List<string> targetColumns,
        Dictionary<string, string> sourceToTargetMappings)
    {
        var resultColumns = new List<string>();
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var targetColSet = new HashSet<string>(targetColumns, StringComparer.OrdinalIgnoreCase);

        foreach (var sourceCol in sourceColumns)
        {
            string targetCol;

            // Check if there's a mapping for this source column
            if (sourceToTargetMappings.TryGetValue(sourceCol, out var mappedTarget))
            {
                targetCol = mappedTarget;
            }
            else
            {
                targetCol = sourceCol;
            }

            // Only include if target column exists
            if (targetColSet.Contains(targetCol))
            {
                if (!resultColumns.Contains(targetCol, StringComparer.OrdinalIgnoreCase))
                {
                    resultColumns.Add(targetCol);
                    targetToSourceMap[targetCol] = sourceCol;
                }
            }
        }

        return (resultColumns, targetToSourceMap);
    }

    public async Task SyncTableAsync(string tableName, ProgressTask? progressTask = null)
    {
        var stopwatch = Stopwatch.StartNew();
        var tableResult = new TableSyncResult
        {
            TableName = tableName,
            StartTime = DateTime.UtcNow,
            Status = "Running"
        };

        try
        {
            // Per-table Spectre progress is disabled when using ConsoleProgressBar.

            var retryPolicy = GetRetryPolicy();

        // 1. Analyze Table (Get Keys & Columns) - Retryable
        // Get column mappings for this table (SourceCol -> TargetCol)
        var tableColumnMappings = GetTableColumnMappings(tableName);

        var (primaryKeys, columns, schemaErrors, targetToSourceMap) = await retryPolicy.ExecuteAsync(async () =>
        {
            using var sourceConnection = new SqlConnection(_sourceConnectionString);
            using var targetConnection = new SqlConnection(_targetConnectionString);
            await sourceConnection.OpenAsync();
            await targetConnection.OpenAsync();

            var pk = await GetPrimaryKeyColumnsAsync(sourceConnection, tableName);
            var sourceCols = await GetColumnNamesAsync(sourceConnection, tableName);
            var targetCols = await GetColumnNamesAsync(targetConnection, tableName);

            // Track schema differences
            var schemaErrors = new SchemaErrors();
            var targetColSet = new HashSet<string>(targetCols, StringComparer.OrdinalIgnoreCase);

            // Apply column mappings to determine effective source column names
            // This maps SourceCol -> TargetCol, so we need to see which source columns can match target
            var (mappedColumns, targetToSource) = ApplyColumnMappings(sourceCols, targetCols, tableColumnMappings);

            // For schema error tracking, we need the original source column set
            var sourceColSet = new HashSet<string>(sourceCols, StringComparer.OrdinalIgnoreCase);

            // After applying mappings, determine which source columns don't have a match in target
            // A source column is "missing in target" if:
            // - It doesn't exist in target AND
            // - It doesn't have a mapping to a column that exists in target
            schemaErrors.MissingColumnsInTarget = sourceCols
                .Where(c => {
                    // If there's a mapping, check if the mapped target exists
                    if (tableColumnMappings.TryGetValue(c, out var mappedTarget))
                    {
                        return !targetColSet.Contains(mappedTarget);
                    }
                    // No mapping, check if source column name exists in target
                    return !targetColSet.Contains(c);
                })
                .ToList();

            // Find columns missing in source (target has but source doesn't)
            schemaErrors.MissingColumnsInSource = targetCols
                .Where(c => !targetToSource.ContainsKey(c))
                .ToList();

            // Determine which columns to use based on flag
            List<string> columnsToUse;
            Dictionary<string, string> finalTargetToSource;

            if (_targetColumnsOnly)
            {
                // Use only columns that exist in target (even if source has more)
                // Filter mappedColumns to only include those in target
                columnsToUse = mappedColumns.Where(c => targetColSet.Contains(c)).ToList();
                finalTargetToSource = targetToSource
                    .Where(kvp => columnsToUse.Contains(kvp.Key, StringComparer.OrdinalIgnoreCase))
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value, StringComparer.OrdinalIgnoreCase);
                schemaErrors.CommonColumns = columnsToUse;
            }
            else
            {
                // Default: Use mapped columns (which are already filtered to exist in target)
                columnsToUse = mappedColumns;
                finalTargetToSource = targetToSource;
                schemaErrors.CommonColumns = columnsToUse;

                // If target has columns not in source, note them but don't use them
                if (schemaErrors.MissingColumnsInSource.Any())
                {
                    schemaErrors.ExcludedColumns = schemaErrors.MissingColumnsInSource;
                }
            }

            // Verify all primary key columns are available
            // Primary keys are in terms of source column names, need to check if they map to target
            var pkMissing = pk.Where(pkCol => {
                // Check if PK column is in the final columns (either directly or via mapping)
                var targetName = tableColumnMappings.TryGetValue(pkCol, out var mapped) ? mapped : pkCol;
                return !columnsToUse.Contains(targetName, StringComparer.OrdinalIgnoreCase);
            }).ToList();

            if (pkMissing.Any())
            {
                throw new InvalidOperationException($"Primary key column(s) missing in target: {string.Join(", ", pkMissing)}");
            }

            // Build schema mismatch details if any
            if (schemaErrors.MissingColumnsInTarget.Any() || schemaErrors.MissingColumnsInSource.Any())
            {
                var details = new List<string>();
                if (schemaErrors.MissingColumnsInTarget.Any())
                {
                    details.Add($"Missing in target: {string.Join(", ", schemaErrors.MissingColumnsInTarget)}");
                }
                if (schemaErrors.MissingColumnsInSource.Any())
                {
                    details.Add($"Missing in source: {string.Join(", ", schemaErrors.MissingColumnsInSource)}");
                }
                schemaErrors.SchemaMismatchDetails = string.Join("; ", details);
            }

            // Log column mappings being applied
            if (tableColumnMappings.Any())
            {
                var appliedMappings = tableColumnMappings
                    .Where(kvp => finalTargetToSource.ContainsKey(kvp.Value))
                    .ToList();
                if (appliedMappings.Any())
                {
                    // Console.WriteLine($"  Column mappings for {tableName}: {string.Join(", ", appliedMappings.Select(m => $"{m.Key}->{m.Value}"))}");
                }
            }

            return (pk, columnsToUse, schemaErrors, finalTargetToSource);
        });

        // Store schema errors in result
        tableResult.SchemaErrors = schemaErrors;

        // Handle No-PK case
        if (!primaryKeys.Any())
        {
            if (!_allowNoPk)
            {
                tableResult.Status = "Skipped";
                tableResult.ErrorMessage = "No primary key and --allow-no-pk not set";
                tableResult.EndTime = DateTime.UtcNow;
                tableResult.DurationSeconds = stopwatch.Elapsed.TotalSeconds;
                progressTask?.Value = 100;
                lock (_resultLock)
                {
                    _runResult?.AddOrUpdateTable(tableResult);
                }
                await SaveResultFileAsync();
                return;
            }

            if (!_deepCompare)
            {
                tableResult.Status = "Skipped";
                tableResult.ErrorMessage = "No primary key and --deep-compare not set (requires --allow-no-pk)";
                tableResult.EndTime = DateTime.UtcNow;
                tableResult.DurationSeconds = stopwatch.Elapsed.TotalSeconds;
                progressTask?.Value = 100;
                lock (_resultLock)
                {
                    _runResult?.AddOrUpdateTable(tableResult);
                }
                await SaveResultFileAsync();
                return;
            }

            // Use all columns as "primary key" for deep compare
            primaryKeys = columns;
        }

        // 2. Count Records - Retryable (table already verified to exist in both databases)
        int sourceCount = await retryPolicy.ExecuteAsync(async () =>
        {
            using var conn = new SqlConnection(_sourceConnectionString);
            await conn.OpenAsync();
            return await CountRecordsAsync(conn, tableName);
        });

        int targetCount = await retryPolicy.ExecuteAsync(async () =>
        {
            using var conn = new SqlConnection(_targetConnectionString);
            await conn.OpenAsync();
            // Table existence already verified in GetTablesToSyncAsync, but handle gracefully if missing
            try
            {
                return await CountRecordsAsync(conn, tableName);
            }
            catch (SqlException)
            {
                // Table doesn't exist in target - return 0
                return 0;
            }
        });

        tableResult.SourceCount = sourceCount;
        tableResult.TargetCount = targetCount;

        // Count check optimization REMOVED
        // Previously, we skipped if targetCount >= sourceCount.
        // However, this prevents syncing when the target has more records (but missing some source records)
        // or when counts are equal but content is different (e.g., disjoint sets).
        // We now proceed to ProcessBatchAsync to check for missing records row-by-row.


        // Clear target mode: use bulk insert for speed
        if (_clearTarget)
        {
            await ClearTargetAndBulkInsertAsync(tableName, columns, targetToSourceMap, sourceCount, tableResult, stopwatch, progressTask);
            return;
        }

        progressTask?.Description($"[cyan]{GetDisplayName(tableName)}[/] [gray]Source: {sourceCount:N0} | Target: {targetCount:N0}[/]");

        // 3. Sync Batches (normal mode)
        int offset = 0;
        int totalInserted = 0;
        int totalSkipped = 0;

        while (offset < sourceCount)
        {
            try
            {
                var (inserted, skipped) = await retryPolicy.ExecuteAsync(async () =>
                {
                    return await ProcessBatchAsync(tableName, columns, primaryKeys, targetToSourceMap, offset, tableResult);
                });

                totalInserted += inserted;
                totalSkipped += skipped;
                offset += _batchSize;
            }
            catch (Exception ex)
            {
                // Capture error details but let outer try-catch handle adding to results
                tableResult.Status = "Failed";
                tableResult.ErrorMessage = ex.Message;
                tableResult.ErrorType = ex.GetType().FullName;
                tableResult.ErrorDetails = FormatExceptionDetails(ex);

                // Check if error is schema-related
                if (ex is SqlException sqlEx)
                {
                    if (IsSchemaError(sqlEx))
                    {
                        if (tableResult.SchemaErrors == null)
                        {
                            tableResult.SchemaErrors = new SchemaErrors();
                        }
                        if (string.IsNullOrEmpty(tableResult.SchemaErrors.SchemaMismatchDetails))
                        {
                            tableResult.SchemaErrors.SchemaMismatchDetails = sqlEx.Message;
                        }
                        else
                        {
                            tableResult.SchemaErrors.SchemaMismatchDetails += "; " + sqlEx.Message;
                        }
                    }
                }

                throw;
            }
        }

        stopwatch.Stop();
        tableResult.Status = "Completed";
        tableResult.Inserted = totalInserted;
        tableResult.Skipped = totalSkipped;
        tableResult.EndTime = DateTime.UtcNow;
        tableResult.DurationSeconds = stopwatch.Elapsed.TotalSeconds;

            lock (_resultLock)
            {
                _runResult?.AddOrUpdateTable(tableResult);
            }
            await SaveResultFileAsync();
        }
        catch (Exception ex)
        {
            // Ensure result is always added, even on failure
            stopwatch.Stop();
            tableResult.Status = "Failed";
            tableResult.ErrorMessage = ex.Message;
            tableResult.ErrorType = ex.GetType().FullName;
            tableResult.ErrorDetails = FormatExceptionDetails(ex);
            tableResult.EndTime = DateTime.UtcNow;
            tableResult.DurationSeconds = stopwatch.Elapsed.TotalSeconds;

            // Check if error is schema-related
            if (ex is SqlException sqlEx)
            {
                if (IsSchemaError(sqlEx))
                {
                    if (tableResult.SchemaErrors == null)
                    {
                        tableResult.SchemaErrors = new SchemaErrors();
                    }
                    if (string.IsNullOrEmpty(tableResult.SchemaErrors.SchemaMismatchDetails))
                    {
                        tableResult.SchemaErrors.SchemaMismatchDetails = sqlEx.Message;
                    }
                    else
                    {
                        tableResult.SchemaErrors.SchemaMismatchDetails += "; " + sqlEx.Message;
                    }
                }
            }

            lock (_resultLock)
            {
                _runResult?.AddOrUpdateTable(tableResult);
            }
            await SaveResultFileAsync();

            // Re-throw to allow caller to handle
            throw;
        }
    }

    private async Task ClearTargetTableAsync(string tableName)
    {
        using var targetConnection = new SqlConnection(_targetConnectionString);
        await targetConnection.OpenAsync();

        // Skip temporal history tables - they are read-only
        if (await IsTemporalHistoryTableAsync(targetConnection, tableName))
        {
            return; // Skip clearing temporal history tables
        }

        var retryPolicy = GetRetryPolicy();

        await retryPolicy.ExecuteAsync(async () =>
        {
            // Try TRUNCATE first (fastest, but fails with FK constraints)
            try
            {
                await targetConnection.ExecuteAsync($"TRUNCATE TABLE {tableName}", commandTimeout: _commandTimeout);
                return;
            }
            catch (SqlException ex) when (ex.Number == 4712) // Cannot delete from temporal history table
            {
                // Temporal history table - skip
                return;
            }
            catch (SqlException)
            {
                // TRUNCATE failed, likely due to FK constraints
            }

            // Try disabling constraints, deleting, then re-enabling
            try
            {
                await targetConnection.ExecuteAsync($"ALTER TABLE {tableName} NOCHECK CONSTRAINT ALL", commandTimeout: _commandTimeout);
                await targetConnection.ExecuteAsync($"DELETE FROM {tableName}", commandTimeout: _commandTimeout);
                await targetConnection.ExecuteAsync($"ALTER TABLE {tableName} CHECK CONSTRAINT ALL", commandTimeout: _commandTimeout);
            }
            catch (SqlException ex) when (ex.Number == 4712) // Cannot delete from temporal history table
            {
                // Temporal history table - skip
                return;
            }
            catch (SqlException ex) when (ex.Number == 547) // Reference constraint conflict
            {
                // Even with constraints disabled, some operations may fail
                // This is expected for tables with complex FK relationships
                // Skip clearing this table
                return;
            }
            catch (SqlException)
            {
                // If that fails, try just DELETE (may fail if FK constraints prevent it)
                // This will throw and be caught by the retry policy
                try
                {
                    await targetConnection.ExecuteAsync($"DELETE FROM {tableName}", commandTimeout: _commandTimeout);
                }
                catch (SqlException ex2) when (ex2.Number == 4712 || ex2.Number == 547)
                {
                    // Temporal history table or reference constraint - skip
                    return;
                }
            }
        });
    }

    private async Task ClearTargetAndBulkInsertAsync(
        string tableName,
        List<string> columns,
        Dictionary<string, string> targetToSourceMap,
        int sourceCount,
        TableSyncResult tableResult,
        Stopwatch stopwatch,
        ProgressTask? progressTask = null)
    {
        using var sourceConnection = new SqlConnection(_sourceConnectionString);
        using var targetConnection = new SqlConnection(_targetConnectionString);

        await sourceConnection.OpenAsync();
        await targetConnection.OpenAsync();

        // Temporal table handling is now done globally in SyncTablesAsync
        // No need for per-table temporal handling here

        var retryPolicy = GetRetryPolicy();

        try
        {
            await retryPolicy.ExecuteAsync(async () =>
                {
                    // Try TRUNCATE first, fall back to DELETE if FK constraints exist
                    var qualifiedTable = FormatTableName(tableName);

                    try
                    {
                        await targetConnection.ExecuteAsync($"TRUNCATE TABLE {qualifiedTable}", commandTimeout: _commandTimeout);
                    }
                    catch (SqlException ex) when (ex.Number == 4712) // Cannot delete from temporal history table
                    {
                        // Temporal history table - should have been handled globally, but skip if somehow encountered
                        tableResult.Status = "Skipped";
                        tableResult.ErrorMessage = "Cannot delete rows from a temporal history table (SYSTEM_VERSIONING not disabled)";
                        return;
                    }
                    catch (SqlException)
                    {
                        // Try disabling constraints, deleting, then re-enabling
                        // This handles cases where FK constraints prevent deletion
                        try
                        {
                            await targetConnection.ExecuteAsync($"ALTER TABLE {qualifiedTable} NOCHECK CONSTRAINT ALL", commandTimeout: _commandTimeout);
                            await targetConnection.ExecuteAsync($"DELETE FROM {qualifiedTable}", commandTimeout: _commandTimeout);
                            await targetConnection.ExecuteAsync($"ALTER TABLE {qualifiedTable} CHECK CONSTRAINT ALL", commandTimeout: _commandTimeout);
                        }
                        catch (SqlException ex2) when (ex2.Number == 4712) // Cannot delete from temporal history table
                        {
                            // Temporal history table - skip
                            tableResult.Status = "Skipped";
                            tableResult.ErrorMessage = "Cannot delete rows from a temporal history table (SYSTEM_VERSIONING not disabled)";
                            return;
                        }
                        catch (SqlException ex2) when (ex2.Number == 547) // Reference constraint conflict
                        {
                            // Even with constraints disabled, some operations may fail
                            // This is expected for tables with complex FK relationships
                            // Skip clearing this table
                            tableResult.Status = "Skipped";
                            tableResult.ErrorMessage = $"Cannot delete from table due to reference constraints: {ex2.Message}";
                            return;
                        }
                        catch
                        {
                            // If that fails, try just DELETE and let it throw if it fails
                            try
                            {
                                await targetConnection.ExecuteAsync($"DELETE FROM {qualifiedTable}", commandTimeout: _commandTimeout);
                            }
                            catch (SqlException ex3) when (ex3.Number == 4712 || ex3.Number == 547)
                            {
                                // Temporal history table or reference constraint - skip
                                tableResult.Status = "Skipped";
                                tableResult.ErrorMessage = ex3.Number == 4712
                                    ? "Cannot delete rows from a temporal history table (SYSTEM_VERSIONING not disabled)"
                                    : $"Cannot delete from table due to reference constraints: {ex3.Message}";
                                return;
                            }
                        }
                    }

                // Use SqlBulkCopy for fast bulk insert
                    using var bulkCopy = new SqlBulkCopy(targetConnection)
                    {
                        DestinationTableName = qualifiedTable,
                        BatchSize = _batchSize,
                        BulkCopyTimeout = _commandTimeout // 10 minutes
                    };

                    if (progressTask != null && sourceCount > 0)
                    {
                        bulkCopy.NotifyAfter = _batchSize;
                        bulkCopy.SqlRowsCopied += (sender, e) =>
                        {
                            var progress = (double)e.RowsCopied / sourceCount * 100;
                            progressTask.Value = Math.Min(progress, 100);
                            progressTask.Description($"[cyan]{GetDisplayName(tableName)}[/] [gray]Bulk inserting: {e.RowsCopied:N0}/{sourceCount:N0}[/]");
                        };
                    }

                    // Map source columns to target columns.
                    // The reader will expose column names as target column names due to AS aliases below,
                    // so we can map 1:1 from target to target.
                    foreach (var targetCol in columns)
                    {
                        bulkCopy.ColumnMappings.Add(targetCol, targetCol);
                    }

                    // Build SELECT using source column names (with aliases to match target names for SqlBulkCopy)
                    var selectParts = columns.Select(targetCol => {
                        var sourceCol = targetToSourceMap.TryGetValue(targetCol, out var mapped) ? mapped : targetCol;
                        // If source and target names differ, use alias
                        if (!sourceCol.Equals(targetCol, StringComparison.OrdinalIgnoreCase))
                        {
                            return $"[{sourceCol}] AS [{targetCol}]";
                        }
                        return $"[{sourceCol}]";
                    });
                    string columnList = string.Join(", ", selectParts);
                    string query = $"SELECT {columnList} FROM {qualifiedTable}";

                    using var command = new SqlCommand(query, sourceConnection) { CommandTimeout = _commandTimeout };
                    using var reader = await command.ExecuteReaderAsync();

                    try
                    {
                        await bulkCopy.WriteToServerAsync(reader);
                    }
                    catch (SqlException ex) when (ex.Number == 213 || ex.Number == 4891 || ex.Message.Contains("schema change"))
                    {
                        // Schema change during bulk insert - handle gracefully
                        tableResult.Status = "Failed";
                        tableResult.ErrorMessage = ex.Number == 4891
                            ? $"Insert bulk failed due to a schema change of the target table: {ex.Message}"
                            : $"Insert bulk failed due to a schema change of the target table: {ex.Message}";
                        tableResult.ErrorType = ex.GetType().FullName;
                        tableResult.ErrorDetails = FormatExceptionDetails(ex);
                        return;
                    }
                    catch (SqlException ex) when (ex.Number == 515) // Cannot insert NULL
                    {
                        // NULL constraint violation - some rows may have been inserted
                        // We'll mark as partial success
                        tableResult.Status = "Failed";
                        tableResult.ErrorMessage = $"Cannot insert NULL values: {ex.Message}";
                        tableResult.ErrorType = ex.GetType().FullName;
                        tableResult.ErrorDetails = FormatExceptionDetails(ex);
                        return;
                    }

                tableResult.Inserted = sourceCount;
                tableResult.Status = "Completed";
                    tableResult.EndTime = DateTime.UtcNow;
                    tableResult.DurationSeconds = stopwatch.Elapsed.TotalSeconds;

                    // Console.WriteLine($"  ✓ {tableName} Bulk inserted {sourceCount:N0} records in {stopwatch.Elapsed.TotalSeconds:F2}s");
                });
        }
        catch (Exception ex)
        {
            tableResult.Status = "Failed";
            tableResult.ErrorMessage = ex.Message;
            tableResult.ErrorType = ex.GetType().FullName;
            tableResult.ErrorDetails = FormatExceptionDetails(ex);
        }

        // Temporal table re-enabling is now handled globally in SyncTablesAsync
        tableResult.EndTime = DateTime.UtcNow;
        tableResult.DurationSeconds = stopwatch.Elapsed.TotalSeconds;

        lock (_resultLock)
        {
            _runResult?.AddOrUpdateTable(tableResult);
        }
        await SaveResultFileAsync();
    }

    private async Task<(int inserted, int skipped)> ProcessBatchAsync(
        string tableName,
        List<string> columns,
        List<string> primaryKeys,
        Dictionary<string, string> targetToSourceMap,
        int offset,
        TableSyncResult? tableResult = null)
    {
        using var sourceConnection = new SqlConnection(_sourceConnectionString);
        using var targetConnection = new SqlConnection(_targetConnectionString);

        await sourceConnection.OpenAsync();
        await targetConnection.OpenAsync();

        var sourceRecords = await FetchRecordsBatchAsync(sourceConnection, tableName, columns, primaryKeys, targetToSourceMap, offset, _batchSize);

        if (!sourceRecords.Any()) return (0, 0);

        return await InsertMissingRecordsAsync(targetConnection, tableName, columns, primaryKeys, targetToSourceMap, sourceRecords, tableResult);
    }

    private AsyncPolicy GetRetryPolicy()
    {
        return Policy
            .Handle<SqlException>(IsTransientError)
            .Or<TimeoutException>()
            .WaitAndRetryAsync(3, retryAttempt =>
                TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                (exception, timeSpan, retryCount, context) =>
                {
                    // Console.WriteLine($"    ⚠ Connection lost. Retrying in {timeSpan.TotalSeconds}s... (Attempt {retryCount})");
                });
    }

    private bool IsTransientError(SqlException ex)
    {
        var transientNumbers = new[] { 40613, 40197, 40501, 10928, 10929, 233, 64 };
        return transientNumbers.Contains(ex.Number);
    }

    private bool IsSchemaError(SqlException ex)
    {
        // SQL error numbers related to schema issues
        var schemaErrorNumbers = new[] { 207, 208, 213, 515, 547 }; // Invalid column, Invalid object, Column already exists, NULL constraint, FK constraint
        var schemaErrorKeywords = new[] { "Invalid column", "Invalid object name", "column", "does not exist", "IDENTITY_INSERT", "GENERATED ALWAYS" };

        if (schemaErrorNumbers.Contains(ex.Number))
            return true;

        var message = ex.Message.ToLowerInvariant();
        return schemaErrorKeywords.Any(keyword => message.Contains(keyword.ToLowerInvariant()));
    }

    private string FormatExceptionDetails(Exception ex)
    {
        var details = new System.Text.StringBuilder();
        details.AppendLine($"Exception Type: {ex.GetType().FullName}");
        details.AppendLine($"Message: {ex.Message}");

        if (!string.IsNullOrEmpty(ex.StackTrace))
        {
            details.AppendLine();
            details.AppendLine("Stack Trace:");
            details.AppendLine(ex.StackTrace);
        }

        // Include inner exceptions
        var innerEx = ex.InnerException;
        int depth = 0;
        while (innerEx != null && depth < 5) // Limit depth to prevent infinite loops
        {
            details.AppendLine();
            details.AppendLine($"Inner Exception #{depth + 1}:");
            details.AppendLine($"  Type: {innerEx.GetType().FullName}");
            details.AppendLine($"  Message: {innerEx.Message}");
            if (!string.IsNullOrEmpty(innerEx.StackTrace))
            {
                details.AppendLine($"  Stack Trace: {innerEx.StackTrace}");
            }
            innerEx = innerEx.InnerException;
            depth++;
        }

        // For SqlException, include additional details
        if (ex is SqlException sqlEx)
        {
            details.AppendLine();
            details.AppendLine("SQL Server Details:");
            details.AppendLine($"  Error Number: {sqlEx.Number}");
            details.AppendLine($"  Severity: {sqlEx.Class}");
            details.AppendLine($"  State: {sqlEx.State}");
            if (sqlEx.Errors.Count > 0)
            {
                details.AppendLine("  Error Details:");
                foreach (SqlError error in sqlEx.Errors)
                {
                    details.AppendLine($"    [{error.Number}] {error.Message} (Line {error.LineNumber}, Procedure: {error.Procedure})");
                }
            }
        }

        return details.ToString();
    }

    private async Task<List<string>> GetPrimaryKeyColumnsAsync(SqlConnection connection, string tableName)
    {
        var parts = tableName.Split('.');
        string schema = parts.Length > 1 ? parts[0] : "dbo";
        string table = parts.Length > 1 ? parts[1] : parts[0];

        var primaryKeys = await connection.QueryAsync<string>(
            @"SELECT k.COLUMN_NAME
              FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
              JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
                ON tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = k.TABLE_SCHEMA
              WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.TABLE_SCHEMA = @Schema
                AND tc.TABLE_NAME = @Table
              ORDER BY k.ORDINAL_POSITION",
            new { Schema = schema, Table = table }, commandTimeout: _commandTimeout);

        return primaryKeys.ToList();
    }

    private async Task<List<string>> GetColumnNamesAsync(SqlConnection connection, string tableName)
    {
        var parts = tableName.Split('.');
        string schema = parts.Length > 1 ? parts[0] : "dbo";
        string table = parts.Length > 1 ? parts[1] : parts[0];

        // Get columns, excluding GENERATED ALWAYS columns and computed columns
        // Use sys.columns to check for generated/computed columns since INFORMATION_SCHEMA doesn't have GENERATION_TYPE
        var columns = await connection.QueryAsync<string>(
            @"SELECT c.COLUMN_NAME
              FROM INFORMATION_SCHEMA.COLUMNS c
              INNER JOIN sys.tables t ON t.name = c.TABLE_NAME
              INNER JOIN sys.schemas s ON s.schema_id = t.schema_id AND s.name = c.TABLE_SCHEMA
              INNER JOIN sys.columns sc ON sc.object_id = t.object_id AND sc.name = c.COLUMN_NAME
              WHERE c.TABLE_SCHEMA = @Schema
                AND c.TABLE_NAME = @Table
                AND sc.is_computed = 0
                AND (sc.generated_always_type = 0 OR sc.generated_always_type IS NULL)
              ORDER BY c.ORDINAL_POSITION",
            new { Schema = schema, Table = table }, commandTimeout: _commandTimeout);

        return columns.ToList();
    }

    private async Task<bool> HasIdentityColumnAsync(SqlConnection connection, string tableName)
    {
        var parts = tableName.Split('.');
        string schema = parts.Length > 1 ? parts[0] : "dbo";
        string table = parts.Length > 1 ? parts[1] : parts[0];

        var hasIdentity = await connection.ExecuteScalarAsync<int>(
            @"SELECT COUNT(*)
              FROM sys.columns c
              INNER JOIN sys.tables t ON c.object_id = t.object_id
              INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
              WHERE s.name = @Schema
                AND t.name = @Table
                AND c.is_identity = 1",
            new { Schema = schema, Table = table }, commandTimeout: _commandTimeout);

        return hasIdentity > 0;
    }

    private async Task<bool> IsTemporalHistoryTableAsync(SqlConnection connection, string tableName)
    {
        var parts = tableName.Split('.');
        string schema = parts.Length > 1 ? parts[0] : "dbo";
        string table = parts.Length > 1 ? parts[1] : parts[0];

        // Check if this table is a temporal history table
        // Temporal history tables are read-only and cannot be deleted from
        var isHistory = await connection.ExecuteScalarAsync<int>(
            @"SELECT COUNT(*)
              FROM sys.tables t
              INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
              INNER JOIN sys.tables pt ON pt.history_table_id = t.object_id
              WHERE s.name = @Schema
                AND t.name = @Table",
            new { Schema = schema, Table = table }, commandTimeout: _commandTimeout);

        return isHistory > 0;
    }

    /// <summary>
    /// Returns whether the given table is a system-versioned temporal *base* table,
    /// and, if so, the fully-qualified history table name.
    /// </summary>
    private async Task<(bool IsTemporalBase, string? HistoryTableFullName)> GetTemporalBaseTableInfoAsync(
        SqlConnection connection,
        string tableName)
    {
        var formatted = FormatTableName(tableName);
        var info = await connection.QuerySingleOrDefaultAsync<(int temporal_type, string? history_schema, string? history_table)>(
            @"SELECT
                  t.temporal_type,
                  hs.name AS history_schema,
                  ht.name AS history_table
              FROM sys.tables t
              INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
              LEFT JOIN sys.tables ht ON t.history_table_id = ht.object_id
              LEFT JOIN sys.schemas hs ON ht.schema_id = hs.schema_id
              WHERE t.object_id = OBJECT_ID(@FormattedName)",
            new { FormattedName = formatted }, commandTimeout: _commandTimeout);

        // temporal_type: 0 = non-temporal, 1 = history table, 2 = system-versioned (base) table
        if (info.temporal_type == 2 && !string.IsNullOrEmpty(info.history_schema) && !string.IsNullOrEmpty(info.history_table))
        {
            var historyFullName = $"[{info.history_schema}].[{info.history_table}]";
            AnsiConsole.MarkupLine($"[gray]  → Detected temporal base table: {Markup.Escape(tableName)}. History table: {Markup.Escape(historyFullName)}[/]");
            return (true, historyFullName);
        }

        return (false, null);
    }

    /// <summary>
    /// Returns whether the given table is a temporal history table, and if so,
    /// returns the parent (base) temporal table name.
    /// </summary>
    private async Task<(bool IsHistoryTable, string? ParentTableName)> GetTemporalParentTableInfoAsync(
        SqlConnection connection,
        string tableName)
    {
        var formatted = FormatTableName(tableName);
        var info = await connection.QuerySingleOrDefaultAsync<(string? parent_schema, string? parent_table)>(
            @"SELECT
                  ps.name AS parent_schema,
                  pt.name AS parent_table
              FROM sys.tables t
              INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
              INNER JOIN sys.tables pt ON t.object_id = pt.history_table_id
              INNER JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
              WHERE t.object_id = OBJECT_ID(@FormattedName)",
            new { FormattedName = formatted }, commandTimeout: _commandTimeout);

        if (!string.IsNullOrEmpty(info.parent_schema) && !string.IsNullOrEmpty(info.parent_table))
        {
            var parentFullName = $"[{info.parent_schema}].[{info.parent_table}]";
            AnsiConsole.MarkupLine($"[gray]  → Detected temporal history table: {Markup.Escape(tableName)}. Parent table: {Markup.Escape(parentFullName)}[/]");
            return (true, parentFullName);
        }

        return (false, null);
    }

    private static string FormatTableName(string tableName)
    {
        var parts = tableName.Split('.');
        if (parts.Length > 1)
        {
            return $"[{parts[0]}].[{parts[1]}]";
        }
        return $"[{tableName}]";
    }

    private async Task<int> CountRecordsAsync(SqlConnection connection, string tableName)
    {
        var qualified = FormatTableName(tableName);
        return await connection.ExecuteScalarAsync<int>($"SELECT COUNT(*) FROM {qualified}", commandTimeout: _commandTimeout);
    }

    private async Task<List<Dictionary<string, object?>>> FetchRecordsBatchAsync(
        SqlConnection connection,
        string tableName,
        List<string> columns,
        List<string> primaryKeys,
        Dictionary<string, string> targetToSourceMap,
        int offset,
        int batchSize)
    {
        // Build SELECT using source column names with aliases to target names
        // columns contains target column names, targetToSourceMap maps target -> source
        var selectParts = columns.Select(targetCol => {
            var sourceCol = targetToSourceMap.TryGetValue(targetCol, out var mapped) ? mapped : targetCol;
            // If source and target names differ, use alias so the result has target column names
            if (!sourceCol.Equals(targetCol, StringComparison.OrdinalIgnoreCase))
            {
                return $"[{sourceCol}] AS [{targetCol}]";
            }
            return $"[{sourceCol}]";
        });
        string columnList = string.Join(", ", selectParts);

        // Determine ORDER BY clause
        // Use primary keys if available, otherwise fallback to first column or (SELECT NULL)
        string orderByClause;
        if (primaryKeys != null && primaryKeys.Any())
        {
            // Primary keys are in source column names (usually).
            // We need to make sure we use the source column names for sorting.
            // (The primaryKeys list passed in *is* source column names).
            orderByClause = string.Join(", ", primaryKeys.Select(pk => $"[{pk}]"));
        }
        else if (columns.Any())
        {
             // Fallback to first available column (better than nothing)
             // We need the source column name
             var firstTarget = columns.First();
             var firstSource = targetToSourceMap.TryGetValue(firstTarget, out var mapped) ? mapped : firstTarget;
             orderByClause = $"[{firstSource}]";
        }
        else
        {
            orderByClause = "(SELECT NULL)";
        }

        string query = $@"
            SELECT {columnList}
            FROM {tableName}
            ORDER BY {orderByClause}
            OFFSET {offset} ROWS
            FETCH NEXT {batchSize} ROWS ONLY";

        var records = await connection.QueryAsync(query, commandTimeout: _commandTimeout);

        return records.Select(record =>
        {
            var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            var recordDict = (IDictionary<string, object>)record;
            foreach (var kvp in recordDict)
            {
                // Keys will be target column names due to the AS alias
                dict[kvp.Key] = kvp.Value;
            }
            return dict;
        }).ToList();
    }

    private async Task<(int inserted, int skipped)> InsertMissingRecordsAsync(
        SqlConnection connection,
        string tableName,
        List<string> columns,
        List<string> primaryKeys,
        Dictionary<string, string> targetToSourceMap,
        List<Dictionary<string, object?>> sourceRecords,
        TableSyncResult? tableResult = null)
    {
        // 1. Setup Staging Table Name
        // Using a unique name for the temp table to avoid collisions
        var stagingTableName = $"#SyncStaging_{Guid.NewGuid():N}";

        // Check if table has identity column
        // Check this BEFORE starting the transaction, as HasIdentityColumnAsync doesn't support transaction passing
        bool hasIdentity = await HasIdentityColumnAsync(connection, tableName);
        // bool identityInsertEnabled = false; // Variable not used in this scope anymore

        using var transaction = connection.BeginTransaction();

        try
        {
            // 2. Create Staging Table (Clone structure of target)
            // We use TOP 0 to create an empty table with the same schema
            await connection.ExecuteAsync(
                $"SELECT TOP 0 * INTO {stagingTableName} FROM {tableName} WHERE 1 = 0",
                transaction: transaction,
                commandTimeout: _commandTimeout);

            // 3. Prepare Data for Bulk Copy
            // We create a DataTable to hold the batch data
            var dataTable = new DataTable();
            foreach (var col in columns)
            {
                // We use typeof(object) to allow any type that comes from the source
                dataTable.Columns.Add(col, typeof(object));
            }

            foreach (var record in sourceRecords)
            {
                var row = dataTable.NewRow();
                foreach (var col in columns)
                {
                    // Use DBNull.Value for nulls
                    row[col] = record.ContainsKey(col) ? (record[col] ?? DBNull.Value) : DBNull.Value;
                }
                dataTable.Rows.Add(row);
            }

            // 4. Bulk Insert into Staging Table
            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulkCopy.DestinationTableName = stagingTableName;
                bulkCopy.BulkCopyTimeout = _commandTimeout;

                foreach (var col in columns)
                {
                    bulkCopy.ColumnMappings.Add(col, col);
                }

                await bulkCopy.WriteToServerAsync(dataTable);
            }

            // 5. Merge from Staging to Target
            // INSERT INTO Target (...) SELECT ... FROM Staging WHERE NOT EXISTS ...
            var columnList = string.Join(", ", columns.Select(c => $"[{c}]"));

            // Determine PKs in terms of Target Columns
            // primaryKeys arg contains Source Column Names.
            // targetToSourceMap contains Target -> Source.
            // We need Source -> Target to map the PKs.
            var sourceToTargetMap = targetToSourceMap.ToDictionary(
                kvp => kvp.Value,
                kvp => kvp.Key,
                StringComparer.OrdinalIgnoreCase);

            var pkTargetNames = primaryKeys.Select(pk =>
                sourceToTargetMap.TryGetValue(pk, out var targetName) ? targetName : pk
            ).ToList();

            // Build ON clause
            // source = staging table, target = real table
            var joinConditions = string.Join(" AND ", pkTargetNames.Select(pk =>
                $"target.[{pk}] = source.[{pk}]"
            ));

            var mergeSql = $@"
                INSERT INTO {tableName} ({columnList})
                SELECT {columnList}
                FROM {stagingTableName} AS source
                WHERE NOT EXISTS (
                    SELECT 1 FROM {tableName} AS target
                    WHERE {joinConditions}
                );
            ";

            // Handle Identity Insert
            // bool hasIdentity = await HasIdentityColumnAsync(connection, tableName); // Removed duplicate call inside transaction
            if (hasIdentity)
            {
                mergeSql = $"SET IDENTITY_INSERT {tableName} ON; {mergeSql} SET IDENTITY_INSERT {tableName} OFF;";
            }

            // Drop staging table
            mergeSql += $" DROP TABLE {stagingTableName};";

            int insertedCount = await connection.ExecuteAsync(mergeSql, transaction: transaction, commandTimeout: _commandTimeout);

            transaction.Commit();

            int skippedCount = sourceRecords.Count - insertedCount;

            // Log batch summary if needed
            // AnsiConsole.MarkupLine($"[grey]Batch processed: {insertedCount} inserted, {skippedCount} skipped.[/]");

            return (insertedCount, skippedCount);
        }
        catch (Exception ex)
        {
            try { transaction.Rollback(); } catch { }

            // Track schema errors if applicable
            if (ex is SqlException sqlEx && tableResult != null && IsSchemaError(sqlEx))
            {
                if (tableResult.SchemaErrors == null) tableResult.SchemaErrors = new SchemaErrors();
                var msg = sqlEx.Message;
                if (string.IsNullOrEmpty(tableResult.SchemaErrors.SchemaMismatchDetails))
                    tableResult.SchemaErrors.SchemaMismatchDetails = msg;
                else
                    tableResult.SchemaErrors.SchemaMismatchDetails += "; " + msg;
            }

            throw;
        }
    }

    private class TableInfo
    {
        public string SchemaName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
    }

    private class TableDependency
    {
        public string ReferencedTable { get; set; } = string.Empty; // Parent table
        public string ReferencingTable { get; set; } = string.Empty; // Child table
    }

    private async Task<Dictionary<string, HashSet<string>>> GetTableDependenciesAsync(List<TableInfo> tables)
    {
        using var connection = new SqlConnection(_targetConnectionString);
        await connection.OpenAsync();

        // Build a set of table names we care about
        var tableSet = new HashSet<string>(tables.Select(t => $"{t.SchemaName}.{t.TableName}"), StringComparer.OrdinalIgnoreCase);

        // Query foreign key relationships
        var dependencies = await connection.QueryAsync<TableDependency>(
            @"SELECT
                QUOTENAME(SCHEMA_NAME(ref_schema.schema_id)) + '.' + QUOTENAME(ref_tab.name) AS ReferencedTable,
                QUOTENAME(SCHEMA_NAME(fk_schema.schema_id)) + '.' + QUOTENAME(fk_tab.name) AS ReferencingTable
              FROM sys.foreign_keys AS fk
              INNER JOIN sys.tables AS fk_tab ON fk.parent_object_id = fk_tab.object_id
              INNER JOIN sys.schemas AS fk_schema ON fk_tab.schema_id = fk_schema.schema_id
              INNER JOIN sys.tables AS ref_tab ON fk.referenced_object_id = ref_tab.object_id
              INNER JOIN sys.schemas AS ref_schema ON ref_tab.schema_id = ref_schema.schema_id
              WHERE fk.is_disabled = 0", commandTimeout: _commandTimeout);

        // Build dependency graph: key = child table, value = set of parent tables it depends on
        var dependencyGraph = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

        // Initialize all tables with empty dependency sets
        foreach (var table in tables)
        {
            var fullName = $"{table.SchemaName}.{table.TableName}";
            dependencyGraph[fullName] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        // Add dependencies (only for tables we're syncing)
        foreach (var dep in dependencies)
        {
            if (tableSet.Contains(dep.ReferencingTable) && tableSet.Contains(dep.ReferencedTable))
            {
                if (!dependencyGraph.ContainsKey(dep.ReferencingTable))
                {
                    dependencyGraph[dep.ReferencingTable] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                }
                dependencyGraph[dep.ReferencingTable].Add(dep.ReferencedTable);
            }
        }

        return dependencyGraph;
    }

    private List<List<TableInfo>> TopologicalSort(List<TableInfo> tables, Dictionary<string, HashSet<string>> dependencies)
    {
        // Kahn's algorithm for topological sort
        var levels = new List<List<TableInfo>>();
        var remainingDeps = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        var tableMap = new Dictionary<string, TableInfo>(StringComparer.OrdinalIgnoreCase);

        // Initialize
        foreach (var table in tables)
        {
            var fullName = $"{table.SchemaName}.{table.TableName}";
            tableMap[fullName] = table;
            remainingDeps[fullName] = dependencies.ContainsKey(fullName)
                ? new HashSet<string>(dependencies[fullName], StringComparer.OrdinalIgnoreCase)
                : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        // Process levels
        while (remainingDeps.Any())
        {
            // Find all tables with no remaining dependencies (current level)
            var currentLevel = remainingDeps
                .Where(kvp => kvp.Value.Count == 0)
                .Select(kvp => tableMap[kvp.Key])
                .ToList();

            if (currentLevel.Count == 0)
            {
                // Circular dependency detected - add remaining tables to last level
                var remaining = remainingDeps.Keys.Select(k => tableMap[k]).ToList();
                if (remaining.Any())
                {
                    levels.Add(remaining);
                }
                break;
            }

            levels.Add(currentLevel);

            // Remove these tables from remaining dependencies
            foreach (var table in currentLevel)
            {
                var fullName = $"{table.SchemaName}.{table.TableName}";
                remainingDeps.Remove(fullName);

                // Remove this table from all other tables' dependency sets
                foreach (var kvp in remainingDeps.ToList())
                {
                    kvp.Value.Remove(fullName);
                }
            }
        }

        return levels;
    }

    private async Task<bool> VerifyConstraintsDisabledAsync()
    {
        using var connection = new SqlConnection(_targetConnectionString);
        await connection.OpenAsync();

        var enabledCount = await connection.ExecuteScalarAsync<int>(
            @"SELECT COUNT(*)
              FROM sys.foreign_keys
              WHERE is_disabled = 0", commandTimeout: _commandTimeout);

        if (enabledCount > 0)
        {
            AnsiConsole.MarkupLine($"[yellow]⚠ Warning: {enabledCount} foreign key constraints are still enabled[/]");
            return false;
        }

        return true;
    }

    /// <summary>
    /// Disables SYSTEM_VERSIONING on all temporal base tables in the list of tables to sync.
    /// Returns a dictionary mapping base table names to their history table names.
    /// </summary>
    private async Task<Dictionary<string, string>> DisableAllTemporalTablesAsync(List<TableInfo> tables)
    {
        var temporalTables = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        using var connection = new SqlConnection(_targetConnectionString);
        await connection.OpenAsync();

        foreach (var table in tables)
        {
            var tableName = $"{table.SchemaName}.{table.TableName}";

            // Check if it's a temporal BASE table
            var baseInfo = await GetTemporalBaseTableInfoAsync(connection, tableName);
            if (baseInfo.IsTemporalBase && baseInfo.HistoryTableFullName is not null)
            {
                var formattedTableName = FormatTableName(tableName);
                try
                {
                    await connection.ExecuteAsync($"ALTER TABLE {formattedTableName} SET (SYSTEM_VERSIONING = OFF)", commandTimeout: _commandTimeout);
                    temporalTables[formattedTableName] = baseInfo.HistoryTableFullName;
                    AnsiConsole.MarkupLine($"[gray]  → Disabled SYSTEM_VERSIONING for {Markup.Escape(formattedTableName)}[/]");
                }
                catch (Exception ex)
                {
                    AnsiConsole.MarkupLine($"[yellow]  ⚠ Failed to disable SYSTEM_VERSIONING for {Markup.Escape(formattedTableName)}: {Markup.Escape(ex.Message)}[/]");
                }
            }
            // Also check if it's a temporal HISTORY table (so we can disable its parent)
            else
            {
                var historyInfo = await GetTemporalParentTableInfoAsync(connection, tableName);
                if (historyInfo.IsHistoryTable && historyInfo.ParentTableName is not null)
                {
                    // Only disable if we haven't already disabled this base table
                    if (!temporalTables.ContainsKey(historyInfo.ParentTableName))
                    {
                        var formattedTableName = FormatTableName(tableName);
                        try
                        {
                            await connection.ExecuteAsync($"ALTER TABLE {historyInfo.ParentTableName} SET (SYSTEM_VERSIONING = OFF)", commandTimeout: _commandTimeout);
                            temporalTables[historyInfo.ParentTableName] = formattedTableName;
                            AnsiConsole.MarkupLine($"[gray]  → Disabled SYSTEM_VERSIONING for {Markup.Escape(historyInfo.ParentTableName)} (parent of {Markup.Escape(formattedTableName)})[/]");
                        }
                        catch (Exception ex)
                        {
                            AnsiConsole.MarkupLine($"[yellow]  ⚠ Failed to disable SYSTEM_VERSIONING for {Markup.Escape(historyInfo.ParentTableName)}: {Markup.Escape(ex.Message)}[/]");
                        }
                    }
                }
            }
        }

        return temporalTables;
    }

    /// <summary>
    /// Re-enables SYSTEM_VERSIONING on all temporal tables that were disabled.
    /// Returns a list of error messages if any occurred.
    /// </summary>
    private async Task<List<string>> EnableAllTemporalTablesAsync(Dictionary<string, string> temporalTables)
    {
        var errors = new List<string>();

        if (!temporalTables.Any())
            return errors;

        using var connection = new SqlConnection(_targetConnectionString);
        await connection.OpenAsync();

        foreach (var (baseTable, historyTable) in temporalTables)
        {
            try
            {
                await connection.ExecuteAsync(
                    $"ALTER TABLE {baseTable} SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = {historyTable}, DATA_CONSISTENCY_CHECK = OFF))",
                    commandTimeout: _commandTimeout);
                AnsiConsole.MarkupLine($"[gray]  → Re-enabled SYSTEM_VERSIONING for {Markup.Escape(baseTable)}[/]");
            }
            catch (Exception ex)
            {
                var errorMsg = $"Failed to re-enable SYSTEM_VERSIONING for {baseTable}: {ex.Message}";
                errors.Add(errorMsg);
                AnsiConsole.MarkupLine($"[yellow]  ⚠ {Markup.Escape(errorMsg)}[/]");
            }
        }

        return errors;
    }

    private async Task<List<string>> ToggleGlobalConstraintsAsync(bool enable)
    {
        var errors = new List<string>();

        using var connection = new SqlConnection(_targetConnectionString);
        await connection.OpenAsync();

        // Get list of tables
        var tables = await connection.QueryAsync<string>(
            @"SELECT QUOTENAME(s.name) + '.' + QUOTENAME(t.name)
              FROM sys.tables t
              INNER JOIN sys.schemas s ON t.schema_id = s.schema_id", commandTimeout: _commandTimeout);

        int successCount = 0;
        int failCount = 0;

        foreach (var table in tables)
        {
            try
            {
                var sql = $"ALTER TABLE {table} {(enable ? "WITH CHECK CHECK" : "NOCHECK")} CONSTRAINT ALL";
                await connection.ExecuteAsync(sql, commandTimeout: _commandTimeout);
                successCount++;
            }
            catch (Exception ex)
            {
                failCount++;
                errors.Add($"Failed to {(enable ? "enable" : "disable")} constraints on {table}: {ex.Message}");
            }
        }

        // Add summary message
        errors.Insert(0, $"{(enable ? "Enabled" : "Disabled")} constraints on {successCount} tables{(failCount > 0 ? $" ({failCount} failed)" : "")}");

        // Verify constraints are actually disabled (only when disabling)
        if (!enable)
        {
            var enabledCount = await connection.ExecuteScalarAsync<int>(
                @"SELECT COUNT(*)
                  FROM sys.foreign_keys
                  WHERE is_disabled = 0", commandTimeout: _commandTimeout);

            if (enabledCount > 0)
            {
                errors.Add($"Warning: {enabledCount} foreign key constraints are still enabled");
            }
        }

        return errors;
    }
}
