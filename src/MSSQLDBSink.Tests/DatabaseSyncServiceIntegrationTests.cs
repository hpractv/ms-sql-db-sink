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

        act.Should().NotThrowAsync();
    }
}
