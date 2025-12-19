using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;
using Microsoft.Data.SqlClient;
using SQLServerSink;

namespace SQLServerSink.Tests;

public class DatabaseSyncServiceTests
{
    private static DatabaseSyncService CreateService()
        => new DatabaseSyncService("Server=.;Database=Test;Trusted_Connection=True;", "Server=.;Database=Test;Trusted_Connection=True;");

    private static bool InvokeIsTransientError(DatabaseSyncService service, int errorNumber)
    {
        var method = typeof(DatabaseSyncService).GetMethod("IsTransientError", BindingFlags.Instance | BindingFlags.NonPublic);
        method.Should().NotBeNull();
        var ex = CreateSqlException(errorNumber, "test");
        return (bool)method!.Invoke(service, new object[] { ex })!;
    }

    private static bool InvokeIsSchemaError(DatabaseSyncService service, int errorNumber, string message)
    {
        var method = typeof(DatabaseSyncService).GetMethod("IsSchemaError", BindingFlags.Instance | BindingFlags.NonPublic);
        method.Should().NotBeNull();
        var ex = CreateSqlException(errorNumber, message);
        return (bool)method!.Invoke(service, new object[] { ex })!;
    }

    private static string InvokeFormatExceptionDetails(DatabaseSyncService service, Exception ex)
    {
        var method = typeof(DatabaseSyncService).GetMethod("FormatExceptionDetails", BindingFlags.Instance | BindingFlags.NonPublic);
        method.Should().NotBeNull();
        return (string)method!.Invoke(service, new object[] { ex })!;
    }

    // Helper to construct SqlException via reflection (since constructor is internal)
    private static SqlException CreateSqlException(int number, string message)
    {
        var errorCollection = (SqlErrorCollection)Activator.CreateInstance(
            typeof(SqlErrorCollection),
            BindingFlags.NonPublic | BindingFlags.Instance,
            binder: null,
            args: Array.Empty<object?>(),
            culture: null)!;

        var error = typeof(SqlError).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            binder: null,
            types: new[] { typeof(int), typeof(byte), typeof(byte), typeof(string), typeof(string), typeof(string), typeof(int), typeof(Exception) },
            modifiers: null)!
            .Invoke(new object?[] { number, (byte)0, (byte)0, "server", message, "proc", 0, null });

        typeof(SqlErrorCollection).GetMethod("Add", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(errorCollection, new[] { error });

        var exception = typeof(SqlException).GetMethod(
                "CreateException",
                BindingFlags.NonPublic | BindingFlags.Static,
                binder: null,
                types: new[] { typeof(SqlErrorCollection), typeof(string) },
                modifiers: null)!
            .Invoke(null, new object?[] { errorCollection, "11.0.0" });

        return (SqlException)exception!;
    }

    [Theory]
    [InlineData(40613, true)]
    [InlineData(40197, true)]
    [InlineData(99999, false)]
    public void IsTransientError_Detects_Known_Codes(int errorNumber, bool expected)
    {
        var service = CreateService();
        var result = InvokeIsTransientError(service, errorNumber);
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData(207, "Invalid column name 'Foo'", true)]
    [InlineData(999, "Invalid column name 'Foo'", true)]
    [InlineData(999, "Some other message", false)]
    public void IsSchemaError_Detects_Schema_Issues(int errorNumber, string message, bool expected)
    {
        var service = CreateService();
        var result = InvokeIsSchemaError(service, errorNumber, message);
        result.Should().Be(expected);
    }

    [Fact]
    public void FormatExceptionDetails_Includes_Error_Number_And_Message_For_SqlException()
    {
        var service = CreateService();
        var ex = CreateSqlException(13559, "Cannot insert rows in a temporal history table");

        var details = InvokeFormatExceptionDetails(service, ex);

        details.Should().Contain("SqlException");
        details.Should().Contain("13559");
        details.Should().Contain("Cannot insert rows in a temporal history table");
    }

    [Fact]
    public void FormatExceptionDetails_Includes_Type_And_Message_For_Generic_Exception()
    {
        var service = CreateService();
        var ex = new InvalidOperationException("Something went wrong");

        var details = InvokeFormatExceptionDetails(service, ex);

        details.Should().Contain(nameof(InvalidOperationException));
        details.Should().Contain("Something went wrong");
    }
}
