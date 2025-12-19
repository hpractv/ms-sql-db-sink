using System;
using System.Reflection;
using MSSQLDBSink;

namespace MSSQLDBSink.Tests;

public class DatabaseSyncServiceUtilityTests
{
    private static DatabaseSyncService CreateService()
        => new DatabaseSyncService("Server=.;Database=Test;Trusted_Connection=True;", "Server=.;Database=Test;Trusted_Connection=True;");

    private static object InvokePrivate(object instance, string name, params object[] args)
    {
        var method = instance.GetType().GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Static);
        method.Should().NotBeNull($"Expected private method '{name}' to exist");
        return method!.Invoke(instance, args)!;
    }

    [Theory]
    [InlineData("dbo.Table", "[dbo].[Table]")]
    [InlineData("PlanFootprint.Pre65Plan", "[PlanFootprint].[Pre65Plan]")]
    [InlineData("JustTable", "[JustTable]")]
    public void FormatTableName_Formats_AsExpected(string input, string expected)
    {
        // FormatTableName is private static, so we can call it via reflection on any instance
        var service = CreateService();
        var result = (string)InvokePrivate(service, "FormatTableName", input);
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData("ShortName", 10, "ShortName")]
    [InlineData("ExactlyTen", 10, "ExactlyTen")]
    [InlineData("MoreThanTenChars", 10, "MoreTha...")]
    public void GetDisplayName_Truncates_Long_Names(string input, int maxLength, string expected)
    {
        var service = CreateService();
        var result = (string)InvokePrivate(service, "GetDisplayName", input, maxLength);
        result.Should().Be(expected);
    }

    [Fact]
    public void IsTableMatch_Matches_ByFullName_Schema_And_DboShortcut()
    {
        var service = CreateService();

        var table = new PrivateTableInfo("dbo", "Employees");
        var selections = new[] { "dbo.Employees", "dbo", "Employees" };

        foreach (var selection in selections)
        {
            var list = new System.Collections.Generic.List<string> { selection };
            var result = (bool)InvokePrivate(service, "IsTableMatch", table.ToTableInfo(), list);
            result.Should().BeTrue($"selection '{selection}' should match table");
        }

        var nonMatchList = new System.Collections.Generic.List<string> { "Other.Table" };
        var nonMatch = (bool)InvokePrivate(service, "IsTableMatch", table.ToTableInfo(), nonMatchList);
        nonMatch.Should().BeFalse();
    }

    [Fact]
    public void EnsureConnectionTimeout_Adds_Timeout_When_Missing()
    {
        var service = CreateService();
        var cs = "Server=.;Database=Foo;Trusted_Connection=True;";

        var result = (string)InvokePrivate(service, "EnsureConnectionTimeout", cs);

        result.Should().Contain("Connect Timeout", "connection timeout should be added when missing");
    }

    [Fact]
    public void EnsureConnectionTimeout_Keeps_Existing_Timeout()
    {
        var service = CreateService();
        var cs = "Server=.;Database=Foo;Trusted_Connection=True;Connect Timeout=30";

        var result = (string)InvokePrivate(service, "EnsureConnectionTimeout", cs);

        result.Should().Be(cs);
    }

    private readonly struct PrivateTableInfo
    {
        public string SchemaName { get; }
        public string TableName { get; }

        public PrivateTableInfo(string schema, string table)
        {
            SchemaName = schema;
            TableName = table;
        }

        public object ToTableInfo()
        {
            // DatabaseSyncService has an internal/private TableInfo type; we can create it via reflection
            var type = typeof(DatabaseSyncService).GetNestedType("TableInfo", BindingFlags.NonPublic | BindingFlags.Public);
            type.Should().NotBeNull("TableInfo nested type should exist");
            var instance = Activator.CreateInstance(type!);
            type!.GetProperty("SchemaName")!.SetValue(instance, SchemaName);
            type!.GetProperty("TableName")!.SetValue(instance, TableName);
            return instance!;
        }
    }
}
