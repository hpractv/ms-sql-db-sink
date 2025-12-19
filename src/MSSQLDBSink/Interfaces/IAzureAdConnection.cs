namespace MSSQLDBSink.Interfaces;

/// <summary>
/// Interface for Azure AD connection string generation.
/// </summary>
public interface IAzureAdConnection
{
    /// <summary>
    /// Gets the connection string for the specified server and database.
    /// </summary>
    string ConnectionString { get; }
}
