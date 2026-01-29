using System.Collections.Generic;
using FluentAssertions;
using MSSQLDBSink;
using Xunit;

namespace MSSQLDBSink.Tests;

public class DatabaseSyncServiceOrderingTests
{
    [Fact]
    public void BuildFetchQuery_WhenOrderByPkIsTrue_AndPkExists_OrdersByPk()
    {
        // Arrange
        var tableName = "dbo.TestTable";
        var columns = new List<string> { "Id", "Name", "Value" };
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>(); // Identity mapping
        var offset = 0;
        var batchSize = 100;
        var orderByPk = true;

        // Act
        var query = DatabaseSyncService.BuildFetchQuery(
            tableName, columns, primaryKeys, targetToSourceMap, offset, batchSize, orderByPk);

        // Assert
        query.Should().Contain("ORDER BY [Id]");
        query.Should().NotContain("ORDER BY [Id] AS"); // Ensure format is correct
    }

    [Fact]
    public void BuildFetchQuery_WhenOrderByPkIsFalse_AndPkExists_OrdersByFirstColumn()
    {
        // Arrange
        var tableName = "dbo.TestTable";
        var columns = new List<string> { "Id", "Name", "Value" };
        var primaryKeys = new List<string> { "Id" }; // PK exists
        var targetToSourceMap = new Dictionary<string, string>();
        var offset = 0;
        var batchSize = 100;
        var orderByPk = false; // Flag disabled

        // Act
        var query = DatabaseSyncService.BuildFetchQuery(
            tableName, columns, primaryKeys, targetToSourceMap, offset, batchSize, orderByPk);

        // Assert
        // Should fallback to first available column logic
        // "Id" is the first column in the list
        query.Should().Contain("ORDER BY [Id]");
    }

    [Fact]
    public void BuildFetchQuery_WhenOrderByPkIsFalse_AndPkExists_AndFirstColumnDifferent_OrdersByFirstColumn()
    {
        // Arrange
        var tableName = "dbo.TestTable";
        var columns = new List<string> { "Name", "Id", "Value" }; // "Name" is first, "Id" is PK
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>();
        var offset = 0;
        var batchSize = 100;
        var orderByPk = false;

        // Act
        var query = DatabaseSyncService.BuildFetchQuery(
            tableName, columns, primaryKeys, targetToSourceMap, offset, batchSize, orderByPk);

        // Assert
        query.Should().Contain("ORDER BY [Name]");
        query.Should().NotContain("ORDER BY [Id]"); // Should NOT use PK
    }

    [Fact]
    public void BuildFetchQuery_WhenOrderByPkIsTrue_AndCompositePkExists_OrdersByCompositePk()
    {
        // Arrange
        var tableName = "dbo.TestTable";
        var columns = new List<string> { "Id1", "Id2", "Name" };
        var primaryKeys = new List<string> { "Id1", "Id2" };
        var targetToSourceMap = new Dictionary<string, string>();
        var offset = 0;
        var batchSize = 100;
        var orderByPk = true;

        // Act
        var query = DatabaseSyncService.BuildFetchQuery(
            tableName, columns, primaryKeys, targetToSourceMap, offset, batchSize, orderByPk);

        // Assert
        query.Should().Contain("ORDER BY [Id1], [Id2]");
    }
}
