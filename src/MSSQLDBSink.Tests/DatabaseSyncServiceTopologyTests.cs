using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using MSSQLDBSink;

namespace MSSQLDBSink.Tests;

public class DatabaseSyncServiceTopologyTests
{
    private static DatabaseSyncService CreateService()
        => new DatabaseSyncService("Server=.;Database=Test;Trusted_Connection=True;", "Server=.;Database=Test;Trusted_Connection=True;");

    private static IList CreateTableInfoList(params (string Schema, string Table)[] tables)
    {
        var tableInfoType = typeof(DatabaseSyncService).GetNestedType("TableInfo", BindingFlags.NonPublic | BindingFlags.Public);
        tableInfoType.Should().NotBeNull();

        var listType = typeof(List<>).MakeGenericType(tableInfoType!);
        var list = (IList)Activator.CreateInstance(listType)!;

        foreach (var (schema, table) in tables)
        {
            var instance = Activator.CreateInstance(tableInfoType!)!;
            tableInfoType!.GetProperty("SchemaName")!.SetValue(instance, schema);
            tableInfoType!.GetProperty("TableName")!.SetValue(instance, table);
            list.Add(instance);
        }

        return list;
    }

    private static IList InvokeTopologicalSort(
        DatabaseSyncService service,
        IList tables,
        Dictionary<string, HashSet<string>> deps)
    {
        var method = typeof(DatabaseSyncService).GetMethod(
            "TopologicalSort",
            BindingFlags.Instance | BindingFlags.NonPublic);
        method.Should().NotBeNull();

        // Invoke and cast to non-generic IList of levels
        var result = (IList)method!.Invoke(service, new object[] { tables, deps })!;
        return result;
    }

    [Fact]
    public void TopologicalSort_SimpleChain_ProducesLevels()
    {
        var service = CreateService();
        var tables = CreateTableInfoList(
            ("dbo", "A"),
            ("dbo", "B"),
            ("dbo", "C"));

        var deps = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["dbo.A"] = new(),
            ["dbo.B"] = new(new[] { "dbo.A" }),
            ["dbo.C"] = new(new[] { "dbo.B" })
        };

        var levels = InvokeTopologicalSort(service, tables, deps);

        levels.Count.Should().Be(3);
    }

    [Fact]
    public void TopologicalSort_CircularDependency_PutsAllInLastLevel()
    {
        var service = CreateService();
        var tables = CreateTableInfoList(
            ("dbo", "A"),
            ("dbo", "B"));

        var deps = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["dbo.A"] = new(new[] { "dbo.B" }),
            ["dbo.B"] = new(new[] { "dbo.A" })
        };

        var levels = InvokeTopologicalSort(service, tables, deps);

        levels.Count.Should().Be(1);
        ((IList)levels[0]!).Count.Should().Be(2);
    }
}
