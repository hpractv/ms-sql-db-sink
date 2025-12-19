namespace MSSQLDBSink.Interfaces;

/// <summary>
/// Interface for database synchronization services.
/// </summary>
public interface IDatabaseSyncService
{
    /// <summary>
    /// Synchronizes tables from source to target database based on selections.
    /// </summary>
    /// <param name="selections">Table names or patterns to sync (e.g., "all", "Schema.Table", "TableName")</param>
    /// <param name="threadCount">Number of parallel threads to use for syncing</param>
    /// <param name="parameters">Additional sync parameters</param>
    /// <returns>Task representing the async operation</returns>
    Task SyncTablesAsync(IEnumerable<string> selections, int threadCount = 1, SyncParameters? parameters = null);

    /// <summary>
    /// Synchronizes a single table from source to target database.
    /// </summary>
    /// <param name="tableName">Fully qualified table name (Schema.Table)</param>
    /// <param name="progressTask">Optional progress task for tracking</param>
    /// <returns>Task representing the async operation</returns>
    Task SyncTableAsync(string tableName, Spectre.Console.ProgressTask? progressTask = null);

    /// <summary>
    /// Compares record counts and schema differences between source and target databases for selected tables.
    /// </summary>
    /// <param name="selections">Table names or patterns to compare (e.g., "all", "Schema.Table", "TableName")</param>
    /// <returns>Task representing the async operation</returns>
    Task CompareCountsAndSchemaAsync(IEnumerable<string> selections);
}
