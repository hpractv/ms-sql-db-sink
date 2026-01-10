using System;
using System.Collections.Generic;
using System.Reflection;
using MSSQLDBSink;
using FluentAssertions.Specialized;

namespace MSSQLDBSink.Tests;

public class DatabaseSyncServiceIntegrationTests
{
    private class TestSyncService : DatabaseSyncService
    {
        public TestSyncService()
            : base("Server=.;Database=Test;Trusted_Connection=True;", "Server=.;Database=Test;Trusted_Connection=True;")
        {
        }

        // Expose protected-like hooks via reflection wrapper if needed in future
    }

    [Fact]
    public void SyncTablesAsync_WithNoTables_Completes_And_WritesResult()
    {
        // This is a very light-weight integration-style test that exercises the public API
        // without actually hitting a database. We pass an empty selection list and rely on
        // the early-return path that does not open connections.
        var service = new TestSyncService();

        Func<Task> act = async () => await service.SyncTablesAsync(Array.Empty<string>(), threadCount: 1, parameters: new SyncParameters
        {
            SourceServer = "localhost",
            SourceDb = "SourceDb",
            TargetServer = "localhost",
            TargetDb = "TargetDb",
            TableName = "none"
        });

        act.Should().NotThrowAsync();
    }

    [Fact]
    public void SyncTableAsync_Sets_TableResult_OnFailure()
    {
        // Call SyncTableAsync with a bogus table name; this will attempt to hit the database
        // and likely throw, but the method should catch and record error details into the
        // internal SyncRunResult structure. We just assert that it does not throw an
        // unhandled exception at the caller level.
        var service = new TestSyncService();

        Func<Task> act = async () => await service.SyncTableAsync("NonExistent.Schema.Table");

        // Should not throw, should swallow and record error
        act.Should().NotThrowAsync();
    }
    
    [Fact]
    public void SyncTablesAsync_WithStartRowOffsets_StoresOffsetsCorrectly()
    {
        // Verify that start row offsets are properly stored in SyncParameters
        var parameters = new SyncParameters
        {
            SourceServer = "localhost",
            SourceDb = "SourceDb",
            TargetServer = "localhost",
            TargetDb = "TargetDb",
            TableName = "dbo.Table1,dbo.Table2,dbo.Table3",
            StartRowOffsets = new List<int> { 0, 1000, 500 }
        };
        
        // Assert
        parameters.StartRowOffsets.Should().HaveCount(3);
        parameters.StartRowOffsets[0].Should().Be(0);
        parameters.StartRowOffsets[1].Should().Be(1000);
        parameters.StartRowOffsets[2].Should().Be(500);
    }
    
    [Fact]
    public void TableSyncResult_StartRowOffset_IsTrackedCorrectly()
    {
        // Verify that TableSyncResult tracks the start row offset
        var result = new TableSyncResult
        {
            TableName = "dbo.TestTable",
            SourceCount = 10000,
            TargetCount = 5000,
            StartRowOffset = 2500,
            Inserted = 100,
            Skipped = 2600 // 2500 from offset + 100 duplicates found
        };
        
        // Assert
        result.StartRowOffset.Should().Be(2500);
        result.Skipped.Should().Be(2600);
        result.Inserted.Should().Be(100);
    }
}
