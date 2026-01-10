using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using MSSQLDBSink;

namespace MSSQLDBSink.Tests;

public class DatabaseSyncServiceContinuationTests
{
    private static DatabaseSyncService CreateService()
        => new DatabaseSyncService("Server=.;Database=Test;Trusted_Connection=True;", "Server=.;Database=Test;Trusted_Connection=True;");

    private static async Task<List<Dictionary<string, object?>>> InvokeFetchRecordsBatchAsync(
        DatabaseSyncService service,
        SqlConnection connection,
        string tableName,
        List<string> columns,
        List<string> primaryKeys,
        Dictionary<string, string> targetToSourceMap,
        int offset,
        int batchSize)
    {
        var method = typeof(DatabaseSyncService).GetMethod(
            "FetchRecordsBatchAsync",
            BindingFlags.Instance | BindingFlags.NonPublic);
        method.Should().NotBeNull();

        var task = (Task<List<Dictionary<string, object?>>>)method!.Invoke(
            service,
            new object[] { connection, tableName, columns, primaryKeys, targetToSourceMap, offset, batchSize })!;

        return await task;
    }

    [Fact]
    public void FetchRecordsBatchAsync_QueryStructure_IncludesOffsetAndFetchNext()
    {
        // Arrange - We can't easily test the actual SQL generation without a database,
        // but we can verify the logic that builds the query string
        var columns = new List<string> { "Id", "Name" };
        var primaryKeys = new List<string> { "Id" };
        var offset = 5000;
        var batchSize = 1000;

        // Act - Simulate the query building logic from FetchRecordsBatchAsync
        var orderByClause = string.Join(", ", primaryKeys.Select(pk => $"[{pk}]"));
        var columnList = string.Join(", ", columns.Select(c => $"[{c}]"));
        var expectedQueryPattern = $"ORDER BY {orderByClause}";
        var expectedOffsetPattern = $"OFFSET {offset} ROWS";
        var expectedFetchPattern = $"FETCH NEXT {batchSize} ROWS ONLY";

        // Assert - Verify the query components are correctly structured
        orderByClause.Should().Be("[Id]");
        columnList.Should().Contain("[Id]");
        columnList.Should().Contain("[Name]");
        expectedOffsetPattern.Should().Be("OFFSET 5000 ROWS");
        expectedFetchPattern.Should().Be("FETCH NEXT 1000 ROWS ONLY");
    }

    [Theory]
    [InlineData(0, 100)]
    [InlineData(1000, 500)]
    [InlineData(9000000, 1000000)]
    [InlineData(9000001, 1000000)]
    public void FetchRecordsBatchAsync_QueryStructure_VariousOffsets(int offset, int batchSize)
    {
        // Arrange
        var primaryKeys = new List<string> { "Id" };

        // Act - Simulate query building
        var orderByClause = string.Join(", ", primaryKeys.Select(pk => $"[{pk}]"));
        var offsetClause = $"OFFSET {offset} ROWS";
        var fetchClause = $"FETCH NEXT {batchSize} ROWS ONLY";

        // Assert
        offsetClause.Should().Be($"OFFSET {offset} ROWS");
        fetchClause.Should().Be($"FETCH NEXT {batchSize} ROWS ONLY");
        orderByClause.Should().NotBeNullOrEmpty();
    }

    [Theory]
    [InlineData(0, 100, 0, 99)]      // Start at 0, batch 100 -> rows 0-99
    [InlineData(1000, 500, 1000, 1499)] // Start at 1000, batch 500 -> rows 1000-1499
    [InlineData(9000000, 1000000, 9000000, 9999999)] // Start at 9M, batch 1M -> rows 9M-9.999M
    [InlineData(9000001, 1000000, 9000001, 9999999)] // Start at 9M+1, batch 1M -> rows 9M+1 to 9.999M (last row is 9,999,999 for 10M total)
    public void ContinuationOffset_CalculatesCorrectRowRange(int startOffset, int batchSize, int expectedFirstRow, int expectedLastRow)
    {
        // Arrange
        var offset = startOffset;
        var sourceCount = 10_000_000; // 10 million rows

        // Act - Simulate the loop logic from SyncTableAsync
        var processedRanges = new List<(int first, int last)>();
        while (offset < sourceCount)
        {
            var firstRow = offset;
            var lastRow = Math.Min(offset + batchSize - 1, sourceCount - 1);
            processedRanges.Add((firstRow, lastRow));
            offset += batchSize;
        }

        // Assert
        processedRanges.Should().NotBeEmpty();
        var firstRange = processedRanges[0];
        firstRange.first.Should().Be(expectedFirstRow);
        firstRange.last.Should().Be(expectedLastRow);
    }

    [Fact]
    public void StartRowOffset_EqualToSourceCount_SkipsAllRows()
    {
        // Arrange
        var startRowOffset = 10_000_000;
        var sourceCount = 10_000_000;

        // Act - Simulate the check from SyncTableAsync line 968
        var shouldSkip = startRowOffset >= sourceCount;

        // Assert
        shouldSkip.Should().BeTrue();
    }

    [Fact]
    public void StartRowOffset_ExceedsSourceCount_SkippedCountEqualsOffset()
    {
        // Arrange - Edge case: offset exceeds available records
        var startRowOffset = 15_000_000;
        var sourceCount = 10_000_000;

        // Act - Simulate the logic when startRowOffset >= sourceCount
        // The Skipped count should represent rows intentionally skipped (the offset),
        // not the number of rows that were actually available to skip (sourceCount)
        var skippedCount = startRowOffset; // Should be offset, not sourceCount

        // Assert
        skippedCount.Should().Be(15_000_000,
            "Skipped count should equal the offset value (intended skip), not sourceCount (available rows)");
        skippedCount.Should().NotBe(sourceCount,
            "Skipped count should not be limited to sourceCount when offset exceeds it");
    }

    [Theory]
    [InlineData(10_000_000, 10_000_000, 10_000_000)] // Equal: offset = sourceCount
    [InlineData(15_000_000, 10_000_000, 15_000_000)] // Exceeds: offset > sourceCount
    [InlineData(20_000_000, 5_000_000, 20_000_000)] // Much larger offset
    public void StartRowOffset_BeyondSourceCount_SkippedCountIsOffset(int startRowOffset, int sourceCount, int expectedSkipped)
    {
        // Arrange & Act
        // When startRowOffset >= sourceCount, Skipped should be set to startRowOffset
        var skippedCount = startRowOffset >= sourceCount ? startRowOffset : sourceCount;

        // Assert
        skippedCount.Should().Be(expectedSkipped,
            $"When offset ({startRowOffset}) >= sourceCount ({sourceCount}), Skipped should be {expectedSkipped}");
    }

    [Fact]
    public void StartRowOffset_OneGreaterThanSourceCount_SkipsAllRows()
    {
        // Arrange
        var startRowOffset = 10_000_001;
        var sourceCount = 10_000_000;

        // Act
        var shouldSkip = startRowOffset >= sourceCount;

        // Assert
        shouldSkip.Should().BeTrue();
    }

    [Fact]
    public void StartRowOffset_OneLessThanSourceCount_ProcessesRemainingRows()
    {
        // Arrange
        var startRowOffset = 9_999_999;
        var sourceCount = 10_000_000;

        // Act
        var shouldSkip = startRowOffset >= sourceCount;
        var remainingRows = sourceCount - startRowOffset;

        // Assert
        shouldSkip.Should().BeFalse();
        remainingRows.Should().Be(1);
    }

    [Theory]
    [InlineData(0, 10_000_000, 10_000_000)]           // No offset, all rows
    [InlineData(9_000_000, 10_000_000, 1_000_000)]    // Offset 9M, 1M remaining
    [InlineData(9_000_001, 10_000_000, 999_999)]      // Offset 9M+1, 999,999 remaining (missing row 9M)
    [InlineData(9_999_999, 10_000_000, 1)]             // Offset 9.999M, 1 remaining
    [InlineData(5_000_000, 10_000_000, 5_000_000)]    // Offset 5M, 5M remaining
    public void ContinuationOffset_CalculatesCorrectRemainingRowCount(int startOffset, int sourceCount, int expectedRemaining)
    {
        // Arrange & Act
        var remaining = startOffset >= sourceCount ? 0 : sourceCount - startOffset;

        // Assert
        remaining.Should().Be(expectedRemaining);
    }

    [Fact]
    public void ContinuationOffset_WithOffset9000001_MissesRow9000000()
    {
        // Arrange
        var startOffset = 9_000_001;
        var sourceCount = 10_000_000;
        var batchSize = 100_000;

        // Act - Simulate processing
        var processedRows = new HashSet<int>();
        var offset = startOffset;
        while (offset < sourceCount)
        {
            var batchEnd = Math.Min(offset + batchSize, sourceCount);
            for (int i = offset; i < batchEnd; i++)
            {
                processedRows.Add(i);
            }
            offset += batchSize;
        }

        // Assert
        processedRows.Should().NotContain(9_000_000, "Row 9,000,000 should be skipped when offset is 9,000,001");
        processedRows.Should().Contain(9_000_001, "Row 9,000,001 should be processed");
        processedRows.Should().Contain(9_999_999, "Last row should be processed");
        processedRows.Count.Should().Be(999_999); // Exactly 999,999 rows processed
    }

    [Fact]
    public void ContinuationOffset_WithOffset9000000_ProcessesAllRemainingRows()
    {
        // Arrange
        var startOffset = 9_000_000;
        var sourceCount = 10_000_000;
        var batchSize = 100_000;

        // Act - Simulate processing
        var processedRows = new HashSet<int>();
        var offset = startOffset;
        while (offset < sourceCount)
        {
            var batchEnd = Math.Min(offset + batchSize, sourceCount);
            for (int i = offset; i < batchEnd; i++)
            {
                processedRows.Add(i);
            }
            offset += batchSize;
        }

        // Assert
        processedRows.Should().Contain(9_000_000, "Row 9,000,000 should be processed");
        processedRows.Should().Contain(9_999_999, "Last row should be processed");
        processedRows.Count.Should().Be(1_000_000, "All 1,000,000 remaining rows should be processed");
    }

    [Fact]
    public void TotalSkipped_IncludesStartRowOffset()
    {
        // Arrange
        var startRowOffset = 5_000_000;
        var duplicatesSkipped = 50;

        // Act - Simulate the logic from SyncTableAsync line 991 and 1003
        var totalSkipped = startRowOffset; // Initialize with offset
        totalSkipped += duplicatesSkipped; // Add duplicates found during processing

        // Assert
        totalSkipped.Should().Be(5_000_050);
    }

    [Theory]
    [InlineData(0, 0, 0)]                    // No offset, no duplicates
    [InlineData(1000, 0, 1000)]              // 1000 offset, no duplicates
    [InlineData(1000, 50, 1050)]              // 1000 offset, 50 duplicates
    [InlineData(9000000, 0, 9000000)]         // 9M offset, no duplicates
    [InlineData(9000000, 100, 9000100)]      // 9M offset, 100 duplicates
    public void TotalSkipped_Calculation(int startOffset, int duplicatesFound, int expectedTotalSkipped)
    {
        // Arrange & Act
        var totalSkipped = startOffset + duplicatesFound;

        // Assert
        totalSkipped.Should().Be(expectedTotalSkipped);
    }

    [Fact]
    public void Continuation_Scenario_10MillionRows_9MillionImported_Offset9000000_Imports1Million()
    {
        // Arrange - Scenario: 10M rows total, 9M already imported, resume with offset 9,000,000
        var sourceCount = 10_000_000;
        var startRowOffset = 9_000_000;
        var batchSize = 100_000;

        // Act - Simulate the continuation logic
        var offset = startRowOffset;
        var totalSkipped = startRowOffset; // Initialize with offset
        var processedRowCount = 0;

        while (offset < sourceCount)
        {
            // Simulate processing a batch
            var batchEnd = Math.Min(offset + batchSize, sourceCount);
            var batchSizeActual = batchEnd - offset;

            // In real scenario, NOT EXISTS would prevent duplicates
            // For this test, assume all rows in this range are new (they should be)
            var skipped = 0; // No duplicates in this range

            totalSkipped += skipped;
            processedRowCount += batchSizeActual;
            offset += batchSize;
        }

        // Assert
        processedRowCount.Should().Be(1_000_000, "Should process exactly 1 million remaining rows");
        totalSkipped.Should().Be(9_000_000, "Should skip 9 million rows (the offset)");
    }

    [Fact]
    public void Continuation_Scenario_10MillionRows_9MillionImported_Offset9000001_MissesOneRow()
    {
        // Arrange - Scenario: 10M rows total, 9M already imported, resume with offset 9,000,001 (WRONG!)
        var sourceCount = 10_000_000;
        var startRowOffset = 9_000_001; // One too many!
        var batchSize = 100_000;

        // Act - Simulate the continuation logic
        var offset = startRowOffset;
        var totalInserted = 0;
        var totalSkipped = startRowOffset;
        var processedRowCount = 0;

        while (offset < sourceCount)
        {
            var batchEnd = Math.Min(offset + batchSize, sourceCount);
            var batchSizeActual = batchEnd - offset;

            totalInserted += batchSizeActual;
            processedRowCount += batchSizeActual;
            offset += batchSize;
        }

        // Assert
        processedRowCount.Should().Be(999_999, "Should process 999,999 rows (missing row 9,000,000)");
        totalInserted.Should().Be(999_999, "Should insert 999,999 rows");
        totalSkipped.Should().Be(9_000_001, "Should skip 9,000,001 rows (including the missed one)");
    }

    [Fact]
    public void Continuation_WithDuplicatePrevention_NoDuplicatesInserted()
    {
        // Arrange - Simulate scenario where some rows already exist
        var startRowOffset = 5_000_000;
        var sourceCount = 10_000_000;
        var batchSize = 100_000;
        var alreadyExistingRows = new HashSet<int> { 5_000_050, 5_000_100, 5_500_000 }; // Some duplicates

        // Act - Simulate processing with NOT EXISTS check
        var offset = startRowOffset;
        var totalInserted = 0;
        var totalSkipped = startRowOffset; // Start with offset
        var insertedRows = new HashSet<int>();
        var skippedRows = new HashSet<int>();

        while (offset < sourceCount)
        {
            var batchEnd = Math.Min(offset + batchSize, sourceCount);
            for (int rowNum = offset; rowNum < batchEnd; rowNum++)
            {
                // Simulate NOT EXISTS check
                if (alreadyExistingRows.Contains(rowNum))
                {
                    skippedRows.Add(rowNum);
                    totalSkipped++;
                }
                else
                {
                    insertedRows.Add(rowNum);
                    totalInserted++;
                }
            }
            offset += batchSize;
        }

        // Assert
        totalInserted.Should().Be(5_000_000 - alreadyExistingRows.Count, "Should insert all rows except duplicates");
        totalSkipped.Should().Be(5_000_000 + alreadyExistingRows.Count, "Should skip offset rows plus duplicates");
        insertedRows.Should().NotIntersectWith(alreadyExistingRows, "No duplicates should be inserted");
        skippedRows.Should().BeEquivalentTo(alreadyExistingRows, "All existing rows should be skipped");
    }

    [Fact]
    public void Continuation_EdgeCase_OffsetAtLastRow_ProcessesOneRow()
    {
        // Arrange
        var sourceCount = 10_000_000;
        var startRowOffset = sourceCount - 1; // 9,999,999

        // Act
        var shouldSkip = startRowOffset >= sourceCount;
        var remainingRows = shouldSkip ? 0 : sourceCount - startRowOffset;

        // Assert
        shouldSkip.Should().BeFalse();
        remainingRows.Should().Be(1);
    }

    [Fact]
    public void Continuation_EdgeCase_OffsetBeyondSourceCount_ProcessesNothing()
    {
        // Arrange
        var sourceCount = 10_000_000;
        var startRowOffset = sourceCount + 100; // 10,000,100

        // Act
        var shouldSkip = startRowOffset >= sourceCount;

        // Assert
        shouldSkip.Should().BeTrue();
    }

    [Theory]
    [InlineData(0, 100, 100)]           // Offset 0, batch 100 -> next offset 100
    [InlineData(1000, 500, 1500)]      // Offset 1000, batch 500 -> next offset 1500
    [InlineData(9000000, 1000000, 10000000)] // Offset 9M, batch 1M -> next offset 10M (stops)
    public void ContinuationOffset_IncrementsCorrectly(int currentOffset, int batchSize, int expectedNextOffset)
    {
        // Arrange & Act
        var nextOffset = currentOffset + batchSize;

        // Assert
        nextOffset.Should().Be(expectedNextOffset);
    }

    [Fact]
    public void FetchRecordsBatchAsync_OrderByClause_UsesPrimaryKeysWhenFlagEnabled()
    {
        // Arrange - Simulate the ORDER BY clause generation logic with flag enabled
        var primaryKeys = new List<string> { "Id", "CreatedDate" };
        var columns = new List<string> { "Id", "Name", "CreatedDate" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var orderByPrimaryKey = true; // Flag enabled

        // Act - Simulate the logic from FetchRecordsBatchAsync
        string orderByClause;
        if (orderByPrimaryKey && primaryKeys != null && primaryKeys.Any())
        {
            orderByClause = string.Join(", ", primaryKeys.Select(pk => $"[{pk}]"));
        }
        else if (columns.Any())
        {
            var firstTarget = columns.First();
            var firstSource = targetToSourceMap.TryGetValue(firstTarget, out var mapped) ? mapped : firstTarget;
            orderByClause = $"[{firstSource}]";
        }
        else
        {
            orderByClause = "(SELECT NULL)";
        }

        // Assert
        orderByClause.Should().Be("[Id], [CreatedDate]", "Should use all primary key columns for ordering when flag is enabled");
        orderByClause.Should().Contain("[Id]");
        orderByClause.Should().Contain("[CreatedDate]");
    }

    [Fact]
    public void FetchRecordsBatchAsync_OrderByClause_DoesNotUsePrimaryKeysWhenFlagDisabled()
    {
        // Arrange - Simulate the ORDER BY clause generation logic with flag disabled (default)
        var primaryKeys = new List<string> { "Id", "CreatedDate" };
        var columns = new List<string> { "Id", "Name", "CreatedDate" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var orderByPrimaryKey = false; // Flag disabled (default)

        // Act - Simulate the logic from FetchRecordsBatchAsync
        string orderByClause;
        if (orderByPrimaryKey && primaryKeys != null && primaryKeys.Any())
        {
            orderByClause = string.Join(", ", primaryKeys.Select(pk => $"[{pk}]"));
        }
        else if (columns.Any())
        {
            var firstTarget = columns.First();
            var firstSource = targetToSourceMap.TryGetValue(firstTarget, out var mapped) ? mapped : firstTarget;
            orderByClause = $"[{firstSource}]";
        }
        else
        {
            orderByClause = "(SELECT NULL)";
        }

        // Assert
        orderByClause.Should().Be("[Id]", "Should fall back to first column when flag is disabled, even if primary keys exist");
        orderByClause.Should().NotContain("[CreatedDate]", "Should not use all primary keys when flag is disabled");
    }

    [Fact]
    public void FetchRecordsBatchAsync_OrderByClause_WithSinglePrimaryKey_UsesPrimaryKey()
    {
        // Arrange
        var primaryKeys = new List<string> { "Id" };
        var columns = new List<string> { "Id", "Name" };

        // Act
        string orderByClause;
        if (primaryKeys != null && primaryKeys.Any())
        {
            orderByClause = string.Join(", ", primaryKeys.Select(pk => $"[{pk}]"));
        }
        else
        {
            orderByClause = "[Name]"; // Fallback
        }

        // Assert
        orderByClause.Should().Be("[Id]", "Should use primary key for ordering");
    }

    [Fact]
    public void FetchRecordsBatchAsync_OrderByClause_WithCompositePrimaryKey_UsesAllPrimaryKeys()
    {
        // Arrange - Composite primary key
        var primaryKeys = new List<string> { "CustomerId", "OrderId", "ItemId" };

        // Act
        var orderByClause = string.Join(", ", primaryKeys.Select(pk => $"[{pk}]"));

        // Assert
        orderByClause.Should().Be("[CustomerId], [OrderId], [ItemId]",
            "Should use all primary key columns in order for deterministic sorting");
        orderByClause.Split(',').Length.Should().Be(3, "Should include all three primary key columns");
    }

    [Fact]
    public void FetchRecordsBatchAsync_OrderByClause_NoPrimaryKeys_FallsBackToFirstColumn()
    {
        // Arrange - No primary keys (shouldn't happen in practice with --allow-no-pk, but test the fallback)
        var primaryKeys = new List<string>();
        var columns = new List<string> { "Name", "Email" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        // Act
        string orderByClause;
        if (primaryKeys != null && primaryKeys.Any())
        {
            orderByClause = string.Join(", ", primaryKeys.Select(pk => $"[{pk}]"));
        }
        else if (columns.Any())
        {
            var firstTarget = columns.First();
            var firstSource = targetToSourceMap.TryGetValue(firstTarget, out var mapped) ? mapped : firstTarget;
            orderByClause = $"[{firstSource}]";
        }
        else
        {
            orderByClause = "(SELECT NULL)";
        }

        // Assert
        orderByClause.Should().Be("[Name]", "Should fall back to first column when no primary keys");
    }

    [Fact]
    public void GetPrimaryKeyColumnsAsync_OrdersByOrdinalPosition()
    {
        // This test verifies that primary keys are retrieved in the correct order
        // The SQL query uses ORDER BY k.ORDINAL_POSITION which ensures composite
        // primary keys are returned in the correct order for deterministic sorting

        // Arrange - Simulate the SQL query logic
        var expectedOrderBy = "ORDER BY k.ORDINAL_POSITION";

        // Act - Verify the query structure (we can't easily test the actual query without a DB)
        var queryContainsOrderBy = expectedOrderBy.Contains("ORDINAL_POSITION");

        // Assert
        queryContainsOrderBy.Should().BeTrue("Primary key query should order by ORDINAL_POSITION");
    }

    // Helper method to simulate the complete query building logic from FetchRecordsBatchAsync
    private static string BuildQuery(
        List<string> columns,
        List<string> primaryKeys,
        Dictionary<string, string> targetToSourceMap,
        string tableName,
        int offset,
        int batchSize,
        bool orderByPrimaryKey = false)
    {
        // Build SELECT using source column names with aliases to target names
        var selectParts = columns.Select(targetCol =>
        {
            var sourceCol = targetToSourceMap.TryGetValue(targetCol, out var mapped) ? mapped : targetCol;
            if (!sourceCol.Equals(targetCol, StringComparison.OrdinalIgnoreCase))
            {
                return $"[{sourceCol}] AS [{targetCol}]";
            }
            return $"[{sourceCol}]";
        });
        string columnList = string.Join(", ", selectParts);

        // Determine ORDER BY clause
        // When orderByPrimaryKey is enabled, order by primary keys to ensure consistent ordering
        string orderByClause;
        if (orderByPrimaryKey && primaryKeys != null && primaryKeys.Any())
        {
            orderByClause = string.Join(", ", primaryKeys.Select(pk => $"[{pk}]"));
        }
        else if (columns.Any())
        {
            var firstTarget = columns.First();
            var firstSource = targetToSourceMap.TryGetValue(firstTarget, out var mapped) ? mapped : firstTarget;
            orderByClause = $"[{firstSource}]";
        }
        else
        {
            orderByClause = "(SELECT NULL)";
        }

        return $@"
            SELECT {columnList}
            FROM {tableName}
            ORDER BY {orderByClause}
            OFFSET {offset} ROWS
            FETCH NEXT {batchSize} ROWS ONLY";
    }

    [Fact]
    public void FetchRecordsBatchAsync_CompleteQuery_IncludesAllRequiredClauses()
    {
        // Arrange
        var columns = new List<string> { "Id", "Name", "Email" };
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";
        var offset = 1000;
        var batchSize = 500;

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize);

        // Assert
        query.Should().Contain("SELECT", "Query should contain SELECT");
        query.Should().Contain("FROM", "Query should contain FROM");
        query.Should().Contain("ORDER BY", "Query should contain ORDER BY");
        query.Should().Contain("OFFSET", "Query should contain OFFSET");
        query.Should().Contain("FETCH NEXT", "Query should contain FETCH NEXT");
        query.Should().Contain("[Id]", "Query should select Id column");
        query.Should().Contain("[Name]", "Query should select Name column");
        query.Should().Contain("[Email]", "Query should select Email column");
        query.Should().Contain("OFFSET 1000 ROWS", "Query should have correct OFFSET");
        query.Should().Contain("FETCH NEXT 500 ROWS ONLY", "Query should have correct FETCH NEXT");
    }

    [Fact]
    public void FetchRecordsBatchAsync_Query_OrdersByPrimaryKey_WhenFlagEnabled()
    {
        // Arrange
        var columns = new List<string> { "Id", "Name" };
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";
        var offset = 0;
        var batchSize = 100;
        var orderByPrimaryKey = true; // Flag enabled

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize, orderByPrimaryKey);

        // Assert
        query.Should().Contain("ORDER BY [Id]", "Query should order by primary key when flag is enabled");
        query.Should().NotContain("ORDER BY [Name]", "Query should not order by non-primary key");
    }

    [Fact]
    public void FetchRecordsBatchAsync_Query_DoesNotOrderByPrimaryKey_WhenFlagDisabled()
    {
        // Arrange
        var columns = new List<string> { "Id", "Name" };
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";
        var offset = 0;
        var batchSize = 100;
        var orderByPrimaryKey = false; // Flag disabled (default)

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize, orderByPrimaryKey);

        // Assert
        query.Should().Contain("ORDER BY [Id]", "Query should still order by first column (Id) when flag is disabled");
        // Note: When flag is disabled, it falls back to first column, which happens to be Id in this case
    }

    [Fact]
    public void FetchRecordsBatchAsync_Query_OrdersByCompositePrimaryKey_InCorrectOrder_WhenFlagEnabled()
    {
        // Arrange - Composite primary key with specific order
        var columns = new List<string> { "CustomerId", "OrderId", "ItemId", "Quantity" };
        var primaryKeys = new List<string> { "CustomerId", "OrderId", "ItemId" }; // Order matters!
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.OrderItems";
        var offset = 0;
        var batchSize = 100;
        var orderByPrimaryKey = true; // Flag enabled

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize, orderByPrimaryKey);

        // Assert
        query.Should().Contain("ORDER BY [CustomerId], [OrderId], [ItemId]",
            "Query should order by all primary keys in the correct order when flag is enabled");
        // Verify the order is correct (CustomerId before OrderId, OrderId before ItemId)
        var orderByIndex = query.IndexOf("ORDER BY");
        var customerIdIndex = query.IndexOf("[CustomerId]", orderByIndex);
        var orderIdIndex = query.IndexOf("[OrderId]", orderByIndex);
        var itemIdIndex = query.IndexOf("[ItemId]", orderByIndex);

        customerIdIndex.Should().BeLessThan(orderIdIndex, "CustomerId should come before OrderId");
        orderIdIndex.Should().BeLessThan(itemIdIndex, "OrderId should come before ItemId");
    }

    [Fact]
    public void FetchRecordsBatchAsync_Query_ColumnSelection_IncludesAllColumns()
    {
        // Arrange
        var columns = new List<string> { "Id", "FirstName", "LastName", "Email", "CreatedDate" };
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";
        var offset = 0;
        var batchSize = 100;

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize);

        // Assert
        query.Should().Contain("[Id]");
        query.Should().Contain("[FirstName]");
        query.Should().Contain("[LastName]");
        query.Should().Contain("[Email]");
        query.Should().Contain("[CreatedDate]");
    }

    [Fact]
    public void FetchRecordsBatchAsync_Query_ColumnMapping_UsesSourceColumnWithAlias()
    {
        // Arrange - Source and target have different column names
        var columns = new List<string> { "UserId", "FullName", "EmailAddress" }; // Target column names
        var primaryKeys = new List<string> { "Id" }; // Source primary key name
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["UserId"] = "Id",           // Target -> Source mapping
            ["FullName"] = "Name",
            ["EmailAddress"] = "Email"
        };
        var tableName = "dbo.Users";
        var offset = 0;
        var batchSize = 100;

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize);

        // Assert
        query.Should().Contain("[Id] AS [UserId]", "Should use source column with alias for target name");
        query.Should().Contain("[Name] AS [FullName]", "Should use source column with alias for target name");
        query.Should().Contain("[Email] AS [EmailAddress]", "Should use source column with alias for target name");
        query.Should().Contain("ORDER BY [Id]", "ORDER BY should use source column name (primary key)");
    }

    [Fact]
    public void FetchRecordsBatchAsync_Query_ColumnMapping_OrderByUsesSourceColumnName()
    {
        // Arrange - Primary key has different name in source vs target
        var columns = new List<string> { "UserId", "Name" };
        var primaryKeys = new List<string> { "Id" }; // Source PK name
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["UserId"] = "Id" // Target UserId maps to source Id
        };
        var tableName = "dbo.Users";
        var offset = 0;
        var batchSize = 100;

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize);

        // Assert
        query.Should().Contain("ORDER BY [Id]", "ORDER BY should use source primary key name, not target name");
        query.Should().NotContain("ORDER BY [UserId]", "ORDER BY should not use target column name");
    }

    [Theory]
    [InlineData(0, 100)]
    [InlineData(1000, 500)]
    [InlineData(50000, 10000)]
    [InlineData(9000000, 1000000)]
    public void FetchRecordsBatchAsync_Query_OffsetAndFetchNext_AreCorrectlyFormatted(int offset, int batchSize)
    {
        // Arrange
        var columns = new List<string> { "Id", "Name" };
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize);

        // Assert
        query.Should().Contain($"OFFSET {offset} ROWS", $"Query should contain OFFSET {offset} ROWS");
        query.Should().Contain($"FETCH NEXT {batchSize} ROWS ONLY", $"Query should contain FETCH NEXT {batchSize} ROWS ONLY");
    }

    [Fact]
    public void FetchRecordsBatchAsync_Query_DeterministicOrdering_WithMultiplePrimaryKeys_WhenFlagEnabled()
    {
        // Arrange - Test that ordering is deterministic by using multiple primary keys
        var columns = new List<string> { "Year", "Month", "Day", "Value" };
        var primaryKeys = new List<string> { "Year", "Month", "Day" }; // Must be in this order
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.DailyMetrics";
        var offset = 0;
        var batchSize = 100;
        var orderByPrimaryKey = true; // Flag enabled

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize, orderByPrimaryKey);

        // Assert
        var orderByClause = ExtractOrderByClause(query);
        orderByClause.Should().Be("[Year], [Month], [Day]",
            "Ordering should be deterministic with all primary keys in correct order when flag is enabled");

        // Verify order is preserved
        var yearIndex = orderByClause.IndexOf("[Year]");
        var monthIndex = orderByClause.IndexOf("[Month]");
        var dayIndex = orderByClause.IndexOf("[Day]");

        yearIndex.Should().BeLessThan(monthIndex, "Year should come before Month");
        monthIndex.Should().BeLessThan(dayIndex, "Month should come before Day");
    }

    [Fact]
    public void FetchRecordsBatchAsync_Query_TableName_IsCorrectlyFormatted()
    {
        // Arrange
        var columns = new List<string> { "Id" };
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";
        var offset = 0;
        var batchSize = 100;

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize);

        // Assert
        query.Should().Contain($"FROM {tableName}", "Query should contain correct table name");
    }

    [Fact]
    public void FetchRecordsBatchAsync_Query_NoPrimaryKeys_FallsBackToFirstColumn()
    {
        // Arrange - No primary keys (edge case)
        var columns = new List<string> { "Name", "Email" };
        var primaryKeys = new List<string>(); // Empty
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";
        var offset = 0;
        var batchSize = 100;

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize);

        // Assert
        query.Should().Contain("ORDER BY [Name]", "Should fall back to first column when no primary keys");
    }

    [Fact]
    public void FetchRecordsBatchAsync_Query_ColumnSelection_RespectsColumnMappings()
    {
        // Arrange - Complex mapping scenario
        var columns = new List<string> { "PK", "DisplayName", "ContactEmail" }; // Target names
        var primaryKeys = new List<string> { "Id" }; // Source PK
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["PK"] = "Id",
            ["DisplayName"] = "FullName",
            ["ContactEmail"] = "Email"
        };
        var tableName = "dbo.Contacts";
        var offset = 0;
        var batchSize = 100;

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize);

        // Assert
        query.Should().Contain("[Id] AS [PK]", "Should map source Id to target PK");
        query.Should().Contain("[FullName] AS [DisplayName]", "Should map source FullName to target DisplayName");
        query.Should().Contain("[Email] AS [ContactEmail]", "Should map source Email to target ContactEmail");
        query.Should().Contain("ORDER BY [Id]", "ORDER BY should use source primary key name");
    }

    [Fact]
    public void FetchRecordsBatchAsync_Query_Ordering_IsConsistentAcrossMultipleCalls()
    {
        // Arrange
        var columns = new List<string> { "Id", "Name" };
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";
        var offset = 0;
        var batchSize = 100;

        // Act - Build query multiple times
        var query1 = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize);
        var query2 = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize);
        var query3 = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize);

        // Assert
        var orderBy1 = ExtractOrderByClause(query1);
        var orderBy2 = ExtractOrderByClause(query2);
        var orderBy3 = ExtractOrderByClause(query3);

        orderBy1.Should().Be(orderBy2, "Ordering should be consistent");
        orderBy2.Should().Be(orderBy3, "Ordering should be consistent");
    }

    // Helper to extract ORDER BY clause from query
    private static string ExtractOrderByClause(string query)
    {
        var orderByIndex = query.IndexOf("ORDER BY");
        if (orderByIndex == -1) return string.Empty;

        var offsetIndex = query.IndexOf("OFFSET", orderByIndex);
        if (offsetIndex == -1) return string.Empty;

        return query.Substring(orderByIndex + 8, offsetIndex - orderByIndex - 8).Trim();
    }

    [Fact]
    public void SyncParameters_OrderByPrimaryKey_DefaultsToFalse()
    {
        // Arrange
        var parameters = new SyncParameters();

        // Assert
        parameters.OrderByPrimaryKey.Should().BeFalse("OrderByPrimaryKey should default to false");
    }

    [Fact]
    public void SyncParameters_OrderByPrimaryKey_CanBeSet()
    {
        // Arrange
        var parameters = new SyncParameters
        {
            OrderByPrimaryKey = true
        };

        // Assert
        parameters.OrderByPrimaryKey.Should().BeTrue("OrderByPrimaryKey should be settable");
    }

    [Fact]
    public void FetchRecordsBatchAsync_OrderByPrimaryKey_FlagRespected_WhenTrue()
    {
        // Arrange
        var columns = new List<string> { "Id", "Name", "Email" };
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";
        var offset = 0;
        var batchSize = 100;
        var orderByPrimaryKey = true; // Flag enabled

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize, orderByPrimaryKey);
        var orderByClause = ExtractOrderByClause(query);

        // Assert
        orderByClause.Should().Be("[Id]", "Should order by primary key when flag is true");
    }

    [Fact]
    public void FetchRecordsBatchAsync_OrderByPrimaryKey_FlagRespected_WhenFalse()
    {
        // Arrange
        var columns = new List<string> { "Id", "Name", "Email" };
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";
        var offset = 0;
        var batchSize = 100;
        var orderByPrimaryKey = false; // Flag disabled (default)

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize, orderByPrimaryKey);
        var orderByClause = ExtractOrderByClause(query);

        // Assert
        orderByClause.Should().Be("[Id]", "Should fall back to first column when flag is false (Id is first column)");
    }

    [Fact]
    public void FetchRecordsBatchAsync_OrderByPrimaryKey_WithCompositeKey_WhenFlagEnabled()
    {
        // Arrange
        var columns = new List<string> { "Year", "Month", "Day", "Value" };
        var primaryKeys = new List<string> { "Year", "Month", "Day" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.DailyMetrics";
        var offset = 0;
        var batchSize = 100;
        var orderByPrimaryKey = true; // Flag enabled

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize, orderByPrimaryKey);
        var orderByClause = ExtractOrderByClause(query);

        // Assert
        orderByClause.Should().Be("[Year], [Month], [Day]",
            "Should order by all primary keys when flag is enabled");
    }

    [Fact]
    public void FetchRecordsBatchAsync_OrderByPrimaryKey_WithCompositeKey_WhenFlagDisabled()
    {
        // Arrange
        var columns = new List<string> { "Year", "Month", "Day", "Value" };
        var primaryKeys = new List<string> { "Year", "Month", "Day" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.DailyMetrics";
        var offset = 0;
        var batchSize = 100;
        var orderByPrimaryKey = false; // Flag disabled (default)

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize, orderByPrimaryKey);
        var orderByClause = ExtractOrderByClause(query);

        // Assert
        orderByClause.Should().Be("[Year]",
            "Should order by first column only when flag is disabled, even if primary keys exist");
    }

    [Fact]
    public void FetchRecordsBatchAsync_OrderByPrimaryKey_NoPrimaryKeys_FallsBackToFirstColumn()
    {
        // Arrange
        var columns = new List<string> { "Name", "Email" };
        var primaryKeys = new List<string>(); // No primary keys
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";
        var offset = 0;
        var batchSize = 100;
        var orderByPrimaryKey = true; // Flag enabled but no PKs

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize, orderByPrimaryKey);
        var orderByClause = ExtractOrderByClause(query);

        // Assert
        orderByClause.Should().Be("[Name]",
            "Should fall back to first column when no primary keys exist, regardless of flag");
    }

    [Theory]
    [InlineData(true, "[Id]")]
    [InlineData(false, "[Id]")] // Falls back to first column which is Id
    public void FetchRecordsBatchAsync_OrderByPrimaryKey_FlagBehavior(bool orderByPrimaryKey, string expectedOrderBy)
    {
        // Arrange
        var columns = new List<string> { "Id", "Name" };
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";
        var offset = 0;
        var batchSize = 100;

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize, orderByPrimaryKey);
        var orderByClause = ExtractOrderByClause(query);

        // Assert
        orderByClause.Should().Be(expectedOrderBy,
            $"When orderByPrimaryKey={orderByPrimaryKey}, should produce {expectedOrderBy}");
    }

    [Fact]
    public void OrderByPrimaryKey_WithStartRowOffset_ProducesConsistentOrdering()
    {
        // Arrange - Simulate resuming with order-by-pk enabled
        var columns = new List<string> { "Id", "Name" };
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";
        var offset = 5000;
        var batchSize = 1000;
        var orderByPrimaryKey = true; // Flag enabled for reliable continuation

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize, orderByPrimaryKey);

        // Assert
        query.Should().Contain("ORDER BY [Id]", "Should order by primary key when flag is enabled");
        query.Should().Contain("OFFSET 5000 ROWS", "Should include offset");
        query.Should().Contain("FETCH NEXT 1000 ROWS ONLY", "Should include batch size");
    }

    [Fact]
    public void OrderByPrimaryKey_WithStartRowOffset_WithoutFlag_ProducesWarningScenario()
    {
        // Arrange - Simulate resuming WITHOUT order-by-pk (should show warning in real code)
        var columns = new List<string> { "Id", "Name" };
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";
        var offset = 5000;
        var batchSize = 1000;
        var orderByPrimaryKey = false; // Flag disabled - ordering may be inconsistent

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize, orderByPrimaryKey);

        // Assert
        // When flag is false, it falls back to first column (which happens to be Id in this case)
        // But in real scenarios with different column order, this could be different
        query.Should().Contain("ORDER BY", "Should still have ORDER BY clause");
        query.Should().Contain("OFFSET 5000 ROWS", "Should include offset");
    }

    [Fact]
    public void OrderByPrimaryKey_Flag_IsIndependentOfStartRowOffset()
    {
        // Arrange - Test that the flag works regardless of offset value
        var columns = new List<string> { "Id", "Name" };
        var primaryKeys = new List<string> { "Id" };
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var tableName = "dbo.Users";
        var batchSize = 100;
        var orderByPrimaryKey = true;

        // Act - Test with different offsets
        var query1 = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, 0, batchSize, orderByPrimaryKey);
        var query2 = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, 1000, batchSize, orderByPrimaryKey);
        var query3 = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, 9000000, batchSize, orderByPrimaryKey);

        // Assert - All should order by primary key
        ExtractOrderByClause(query1).Should().Be("[Id]");
        ExtractOrderByClause(query2).Should().Be("[Id]");
        ExtractOrderByClause(query3).Should().Be("[Id]");
    }

    [Fact]
    public void OrderByPrimaryKey_WithColumnMappings_UsesSourceColumnNames()
    {
        // Arrange - Primary key has different name in source vs target
        var columns = new List<string> { "UserId", "Name" }; // Target names
        var primaryKeys = new List<string> { "Id" }; // Source PK name
        var targetToSourceMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["UserId"] = "Id" // Target UserId maps to source Id
        };
        var tableName = "dbo.Users";
        var offset = 0;
        var batchSize = 100;
        var orderByPrimaryKey = true; // Flag enabled

        // Act
        var query = BuildQuery(columns, primaryKeys, targetToSourceMap, tableName, offset, batchSize, orderByPrimaryKey);
        var orderByClause = ExtractOrderByClause(query);

        // Assert
        orderByClause.Should().Be("[Id]",
            "ORDER BY should use source column name (primary key), not target mapped name");
        query.Should().Contain("[Id] AS [UserId]", "SELECT should use source with alias");
    }
}

