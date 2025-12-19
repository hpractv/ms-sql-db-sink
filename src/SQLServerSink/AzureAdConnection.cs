using Microsoft.Data.SqlClient;
using SQLServerSink.Interfaces;

namespace SQLServerSink;

public class AzureAdConnection : IAzureAdConnection
{
    private readonly string _serverName;
    private readonly string _databaseName;

    public AzureAdConnection(string serverName, string databaseName)
    {
        if (string.IsNullOrWhiteSpace(serverName))
            throw new ArgumentNullException(nameof(serverName));

        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentNullException(nameof(databaseName));

        _serverName = serverName;
        _databaseName = databaseName;
    }

    public SqlConnection CreateConnection()
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = _serverName,
            InitialCatalog = _databaseName,
            Authentication = SqlAuthenticationMethod.ActiveDirectoryInteractive
        };

        return new SqlConnection(builder.ConnectionString);
    }

    public string ConnectionString
    {
        get
        {
            var builder = new SqlConnectionStringBuilder
            {
                DataSource = _serverName,
                InitialCatalog = _databaseName,
                Authentication = SqlAuthenticationMethod.ActiveDirectoryInteractive
            };
            return builder.ConnectionString;
        }
    }
}
