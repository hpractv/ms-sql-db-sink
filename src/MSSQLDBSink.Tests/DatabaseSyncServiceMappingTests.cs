using System;
using System.Collections.Generic;
using System.Reflection;
using MSSQLDBSink;

namespace MSSQLDBSink.Tests;

public class DatabaseSyncServiceMappingTests
{
    private static DatabaseSyncService CreateService()
        => new DatabaseSyncService("Server=.;Database=Test;Trusted_Connection=True;", "Server=.;Database=Test;Trusted_Connection=True;");

    private static (List<string> cols, Dictionary<string,string> map) InvokeApplyColumnMappings(
        DatabaseSyncService service,
        List<string> source,
        List<string> target,
        Dictionary<string,string> mappings,
        HashSet<string>? ignoredColumns = null)
    {
        var method = typeof(DatabaseSyncService).GetMethod(
            "ApplyColumnMappings",
            BindingFlags.Instance | BindingFlags.NonPublic);
        method.Should().NotBeNull();

        var ignored = ignoredColumns ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = (ValueTuple<List<string>, Dictionary<string,string>>)method!.Invoke(service, new object[] { source, target, mappings, ignored })!;
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

    [Fact]
    public void ApplyColumnMappings_IgnoresSpecifiedColumns()
    {
        var service = CreateService();
        var source = new List<string> { "A", "B", "C", "D" };
        var target = new List<string> { "A", "B", "C", "D" };
        var mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var ignoredColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "B", "D" };

        var (cols, map) = InvokeApplyColumnMappings(service, source, target, mappings, ignoredColumns);

        cols.Should().BeEquivalentTo(new[] { "A", "C" }, opts => opts.WithStrictOrdering());
        map.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["A"] = "A",
            ["C"] = "C"
        });
    }

    [Fact]
    public void ApplyColumnMappings_IgnoresColumns_CaseInsensitive()
    {
        var service = CreateService();
        var source = new List<string> { "ColumnA", "ColumnB", "ColumnC" };
        var target = new List<string> { "ColumnA", "ColumnB", "ColumnC" };
        var mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var ignoredColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "columnb" };

        var (cols, map) = InvokeApplyColumnMappings(service, source, target, mappings, ignoredColumns);

        cols.Should().BeEquivalentTo(new[] { "ColumnA", "ColumnC" }, opts => opts.WithStrictOrdering());
        map.Should().NotContainKey("ColumnB");
    }

    [Fact]
    public void ApplyColumnMappings_IgnoresColumns_WithMappings()
    {
        var service = CreateService();
        var source = new List<string> { "SourceA", "SourceB", "SourceC" };
        var target = new List<string> { "ColA", "ColB", "ColC" };
        var mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["SourceA"] = "ColA",
            ["SourceB"] = "ColB",
            ["SourceC"] = "ColC"
        };
        var ignoredColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "SourceB" };

        var (cols, map) = InvokeApplyColumnMappings(service, source, target, mappings, ignoredColumns);

        cols.Should().BeEquivalentTo(new[] { "ColA", "ColC" }, opts => opts.WithStrictOrdering());
        map.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["ColA"] = "SourceA",
            ["ColC"] = "SourceC"
        });
        map.Should().NotContainKey("ColB");
    }

    [Fact]
    public void ApplyColumnMappings_IgnoresAllColumns_ReturnsEmpty()
    {
        var service = CreateService();
        var source = new List<string> { "A", "B", "C" };
        var target = new List<string> { "A", "B", "C" };
        var mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var ignoredColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "A", "B", "C" };

        var (cols, map) = InvokeApplyColumnMappings(service, source, target, mappings, ignoredColumns);

        cols.Should().BeEmpty();
        map.Should().BeEmpty();
    }

    [Fact]
    public void ApplyColumnMappings_IgnoresNonExistentColumns_NoEffect()
    {
        var service = CreateService();
        var source = new List<string> { "A", "B", "C" };
        var target = new List<string> { "A", "B", "C" };
        var mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var ignoredColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "X", "Y", "Z" };

        var (cols, map) = InvokeApplyColumnMappings(service, source, target, mappings, ignoredColumns);

        cols.Should().BeEquivalentTo(new[] { "A", "B", "C" }, opts => opts.WithStrictOrdering());
        map.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["A"] = "A",
            ["B"] = "B",
            ["C"] = "C"
        });
    }
}
