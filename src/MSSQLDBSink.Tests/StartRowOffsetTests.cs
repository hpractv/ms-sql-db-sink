using System;
using System.Collections.Generic;
using System.Reflection;
using MSSQLDBSink;

namespace MSSQLDBSink.Tests;

public class StartRowOffsetTests
{
    [Fact]
    public void ParseStartRowOffsets_EmptyString_ReturnsEmptyList()
    {
        // Arrange
        string? startRow = null;

        // Act
        var result = InvokeParseStartRowOffsets(startRow);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeEmpty();
    }

    [Fact]
    public void ParseStartRowOffsets_SingleValue_ReturnsSingleItem()
    {
        // Arrange
        string startRow = "1000";

        // Act
        var result = InvokeParseStartRowOffsets(startRow);

        // Assert
        result.Should().HaveCount(1);
        result[0].Should().Be(1000);
    }

    [Fact]
    public void ParseStartRowOffsets_MultipleValues_ReturnsCorrectList()
    {
        // Arrange
        string startRow = "0,1000,500,2500";

        // Act
        var result = InvokeParseStartRowOffsets(startRow);

        // Assert
        result.Should().HaveCount(4);
        result[0].Should().Be(0);
        result[1].Should().Be(1000);
        result[2].Should().Be(500);
        result[3].Should().Be(2500);
    }

    [Fact]
    public void ParseStartRowOffsets_WithSpaces_TrimsAndParsesCorrectly()
    {
        // Arrange
        string startRow = " 100 , 200 , 300 ";

        // Act
        var result = InvokeParseStartRowOffsets(startRow);

        // Assert
        result.Should().HaveCount(3);
        result[0].Should().Be(100);
        result[1].Should().Be(200);
        result[2].Should().Be(300);
    }

    [Fact]
    public void ParseStartRowOffsets_NegativeValue_UsesZero()
    {
        // Arrange
        string startRow = "100,-500,200";

        // Act
        var result = InvokeParseStartRowOffsets(startRow);

        // Assert
        result.Should().HaveCount(3);
        result[0].Should().Be(100);
        result[1].Should().Be(0); // Negative becomes 0
        result[2].Should().Be(200);
    }

    [Fact]
    public void ParseStartRowOffsets_InvalidValue_UsesZero()
    {
        // Arrange
        string startRow = "100,abc,200";

        // Act
        var result = InvokeParseStartRowOffsets(startRow);

        // Assert
        result.Should().HaveCount(3);
        result[0].Should().Be(100);
        result[1].Should().Be(0); // Invalid becomes 0
        result[2].Should().Be(200);
    }

    [Fact]
    public void ParseStartRowOffsets_MixedValidAndInvalid_HandlesGracefully()
    {
        // Arrange
        string startRow = "0,100,invalid,-50,300";

        // Act
        var result = InvokeParseStartRowOffsets(startRow);

        // Assert
        result.Should().HaveCount(5);
        result[0].Should().Be(0);
        result[1].Should().Be(100);
        result[2].Should().Be(0); // Invalid becomes 0
        result[3].Should().Be(0); // Negative becomes 0
        result[4].Should().Be(300);
    }

    [Fact]
    public void StartRowOffset_AppliedToTableResult()
    {
        // Arrange
        var tableResult = new TableSyncResult
        {
            TableName = "dbo.TestTable",
            StartRowOffset = 1000
        };

        // Assert
        tableResult.StartRowOffset.Should().Be(1000);
    }

    [Fact]
    public void StartRowOffset_DefaultsToZero()
    {
        // Arrange
        var tableResult = new TableSyncResult
        {
            TableName = "dbo.TestTable"
        };

        // Assert
        tableResult.StartRowOffset.Should().Be(0);
    }

    [Fact]
    public void SyncParameters_StartRowOffsets_CanBeSet()
    {
        // Arrange
        var parameters = new SyncParameters
        {
            StartRowOffsets = new List<int> { 0, 1000, 500 }
        };

        // Assert
        parameters.StartRowOffsets.Should().HaveCount(3);
        parameters.StartRowOffsets[0].Should().Be(0);
        parameters.StartRowOffsets[1].Should().Be(1000);
        parameters.StartRowOffsets[2].Should().Be(500);
    }

    [Fact]
    public void SyncParameters_StartRowOffsets_DefaultsToEmptyList()
    {
        // Arrange
        var parameters = new SyncParameters();

        // Assert
        parameters.StartRowOffsets.Should().NotBeNull();
        parameters.StartRowOffsets.Should().BeEmpty();
    }

    // Helper method to call the public ParseStartRowOffsets method
    private static List<int> InvokeParseStartRowOffsets(string? startRow)
    {
        return Program.ParseStartRowOffsets(startRow);
    }
}

public class OrderByPrimaryKeyTests
{
    [Fact]
    public void SyncParameters_OrderByPrimaryKey_DefaultsToFalse()
    {
        // Arrange
        var parameters = new SyncParameters();

        // Assert
        parameters.OrderByPrimaryKey.Should().BeFalse("OrderByPrimaryKey should default to false until next major version");
    }

    [Fact]
    public void SyncParameters_OrderByPrimaryKey_CanBeSetToTrue()
    {
        // Arrange
        var parameters = new SyncParameters
        {
            OrderByPrimaryKey = true
        };

        // Assert
        parameters.OrderByPrimaryKey.Should().BeTrue("OrderByPrimaryKey should be settable to true");
    }

    [Fact]
    public void SyncParameters_OrderByPrimaryKey_CanBeSetToFalse()
    {
        // Arrange
        var parameters = new SyncParameters
        {
            OrderByPrimaryKey = false
        };

        // Assert
        parameters.OrderByPrimaryKey.Should().BeFalse("OrderByPrimaryKey should be settable to false");
    }

    [Fact]
    public void SyncParameters_OrderByPrimaryKey_WorksWithStartRowOffsets()
    {
        // Arrange
        var parameters = new SyncParameters
        {
            OrderByPrimaryKey = true,
            StartRowOffsets = new List<int> { 1000, 5000 }
        };

        // Assert
        parameters.OrderByPrimaryKey.Should().BeTrue("OrderByPrimaryKey should work with start row offsets");
        parameters.StartRowOffsets.Should().HaveCount(2);
    }
}
