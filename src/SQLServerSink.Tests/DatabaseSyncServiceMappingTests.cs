using System;
using System.Collections.Generic;
using System.Reflection;
using SQLServerSink;

namespace SQLServerSink.Tests;

public class DatabaseSyncServiceMappingTests
{
    private static DatabaseSyncService CreateService()
        => new DatabaseSyncService("Server=.;Database=Test;Trusted_Connection=True;", "Server=.;Database=Test;Trusted_Connection=True;");

    private static (List<string> cols, Dictionary<string,string> map) InvokeApplyColumnMappings(
        DatabaseSyncService service,
        List<string> source,
        List<string> target,
        Dictionary<string,string> mappings)
    {
        var method = typeof(DatabaseSyncService).GetMethod(
            "ApplyColumnMappings",
            BindingFlags.Instance | BindingFlags.NonPublic);
        method.Should().NotBeNull();

        var result = (ValueTuple<List<string>, Dictionary<string,string>>)method!.Invoke(service, new object[] { source, target, mappings })!;
        return (result.Item1, result.Item2);
    }

    [Fact]
    public void ApplyColumnMappings_NoMappings_UsesIntersection()
    {
        var service = CreateService();
        var source = new List<string> { "A", "B", "C" };
        var target = new List<string> { "B", "C", "D" };
        var mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        var (cols, map) = InvokeApplyColumnMappings(service, source, target, mappings);

        cols.Should().BeEquivalentTo(new[] { "B", "C" }, opts => opts.WithStrictOrdering());
        map.Should().BeEquivalentTo(new Dictionary<string,string>
        {
            ["B"] = "B",
            ["C"] = "C"
        });
    }

    [Fact]
    public void ApplyColumnMappings_UsesExplicitMappings_And_ExcludesMissingTargets()
    {
        var service = CreateService();
        var source = new List<string> { "SourceA", "SourceB", "SourceC" };
        var target = new List<string> { "ColA", "ColB" };
        var mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["SourceA"] = "ColA",
            ["SourceB"] = "ColB",
            ["SourceC"] = "NonExisting"
        };

        var (cols, map) = InvokeApplyColumnMappings(service, source, target, mappings);

        cols.Should().BeEquivalentTo(new[] { "ColA", "ColB" }, opts => opts.WithStrictOrdering());
        map.Should().BeEquivalentTo(new Dictionary<string,string>
        {
            ["ColA"] = "SourceA",
            ["ColB"] = "SourceB"
        });
    }

    [Fact]
    public void ApplyColumnMappings_Deduplicates_TargetColumns_CaseInsensitive()
    {
        var service = CreateService();
        var source = new List<string> { "A", "a" };
        var target = new List<string> { "A" };
        var mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        var (cols, map) = InvokeApplyColumnMappings(service, source, target, mappings);

        cols.Should().BeEquivalentTo(new[] { "A" });
        map.Should().HaveCount(1);
    }
}
