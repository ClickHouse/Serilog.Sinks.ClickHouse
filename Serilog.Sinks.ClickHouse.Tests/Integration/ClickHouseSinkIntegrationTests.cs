using ClickHouse.Driver;
using Serilog.Events;
using Serilog.Sinks.ClickHouse.Client;
using Serilog.Sinks.ClickHouse.ColumnWriters;
using Serilog.Sinks.ClickHouse.Configuration;
using Serilog.Sinks.ClickHouse.Schema;
using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Integration;

[TestFixture]
[Category("Integration")]
public class ClickHouseSinkIntegrationTests
{
    private string ConnectionString => IntegrationTestFixture.ConnectionString;

    private ClickHouseSinkOptions CreateOptions(string tableName, TableCreationMode mode = TableCreationMode.CreateIfNotExists)
        => new()
        {
            ConnectionString = ConnectionString,
            Schema = DefaultSchema.Create(tableName),
            TableCreation = new TableCreationOptions { Mode = mode },
        };

    private static string UniqueTable(string prefix = "test") => $"{prefix}_{Guid.NewGuid():N}";

    [Test]
    public async Task EmitBatchAsync_WritesCorrectRowCount()
    {
        var table = UniqueTable();
        var options = CreateOptions(table);

        using var sink = new ClickHouseSink(options);

        var logEvents = new[]
        {
            new LogEventBuilder().WithMessage("User {UserId} logged in").WithProperty("UserId", 123).Build(),
            new LogEventBuilder().WithMessage("High memory: {Pct}%").WithProperty("Pct", 85.5).Build(),
        };

        await sink.EmitBatchAsync(logEvents);

        using var client = new ClickHouseClient(ConnectionString);
        var count = await client.ExecuteScalarAsync($"SELECT count() FROM {SqlGenerator.EscapeTableName(table)}");
        Assert.That(Convert.ToInt64(count), Is.EqualTo(2));
    }

    [Test]
    public async Task EmitBatchAsync_CreatesTable_OnFirstBatch()
    {
        var table = UniqueTable("auto_create");
        var options = CreateOptions(table);

        using var client = new ClickHouseClient(ConnectionString);
        var before = await client.ExecuteScalarAsync($"EXISTS {SqlGenerator.EscapeTableName(table)}");
        Assert.That(before is (byte)1, Is.False);

        using var sink = new ClickHouseSink(options);
        await sink.EmitBatchAsync(new[] { new LogEventBuilder().WithMessage("Test").Build() });

        var after = await client.ExecuteScalarAsync($"EXISTS {SqlGenerator.EscapeTableName(table)}");
        Assert.That(after is (byte)1, Is.True);
    }

    [Test]
    public async Task EmitBatchAsync_SkipsTableCreation_WhenModeIsNone()
    {
        var table = UniqueTable("no_create");

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = DefaultSchema.Create(table),
            TableCreation = new TableCreationOptions
            {
                Mode = TableCreationMode.None,
                ValidateOnStartup = false,
            },
        };

        using var sink = new ClickHouseSink(options);

        // Should throw because table doesn't exist and we're trying to insert
        Assert.ThrowsAsync<ClickHouseServerException>(
            () => sink.EmitBatchAsync(new[] { new LogEventBuilder().WithMessage("Test").Build() }));
    }

    [Test]
    public async Task EmitBatchAsync_TransformsAllColumns_FromLogEvent()
    {
        var table = UniqueTable("columns");
        var options = CreateOptions(table);

        using var sink = new ClickHouseSink(options);

        var timestamp = new DateTimeOffset(2024, 6, 15, 14, 30, 45, TimeSpan.Zero);
        var logEvent = new LogEventBuilder()
            .WithTimestamp(timestamp)
            .WithLevel(LogEventLevel.Warning)
            .WithMessage("Test message {Value}")
            .WithProperty("Value", 42)
            .Build();

        await sink.EmitBatchAsync(new[] { logEvent });

        // Query the data back
        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT timestamp, level, message, message_template, exception FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);

        // Timestamp
        var storedTimestamp = reader.GetDateTime(0);
        Assert.That(storedTimestamp, Is.EqualTo(timestamp.UtcDateTime));

        // Level
        Assert.That(reader.GetString(1), Is.EqualTo("Warning"));

        // Rendered message
        Assert.That(reader.GetString(2), Is.EqualTo("Test message 42"));

        // Message template
        Assert.That(reader.GetString(3), Is.EqualTo("Test message {Value}"));

        // Exception (null or empty for Nullable(String) with no exception)
        var exception = reader.GetValue(4);
        Assert.That(exception is null or DBNull || (exception is string s && s == ""), Is.True);
    }

    [Test]
    public async Task EmitBatchAsync_HandlesExceptions_InLogEvents()
    {
        var table = UniqueTable("exception");
        var options = CreateOptions(table);

        using var sink = new ClickHouseSink(options);

        var exception = new InvalidOperationException("Test exception");
        var logEvent = new LogEventBuilder()
            .WithLevel(LogEventLevel.Error)
            .WithMessage("An error occurred")
            .WithException(exception)
            .Build();

        await sink.EmitBatchAsync(new[] { logEvent });

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT exception FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);
        var stored = reader.GetString(0);
        Assert.That(stored, Does.Contain("Test exception"));
        Assert.That(stored, Does.Contain("InvalidOperationException"));
    }

    [Test]
    public async Task EmitBatchAsync_HandlesMultipleBatches()
    {
        var table = UniqueTable("multi_batch");
        var options = CreateOptions(table);

        using var sink = new ClickHouseSink(options);

        for (int i = 0; i < 3; i++)
        {
            var logEvents = Enumerable.Range(0, 10)
                .Select(j => new LogEventBuilder().WithMessage($"Batch {i}, Event {j}").Build())
                .ToList();

            await sink.EmitBatchAsync(logEvents);
        }

        using var client = new ClickHouseClient(ConnectionString);
        var count = await client.ExecuteScalarAsync($"SELECT count() FROM {SqlGenerator.EscapeTableName(table)}");
        Assert.That(Convert.ToInt64(count), Is.EqualTo(30));
    }

    [Test]
    public async Task EmitBatchAsync_WithCustomSchema_WorksCorrectly()
    {
        var table = UniqueTable("custom_schema");
        var schema = new SchemaBuilder()
            .WithTableName(table)
            .AddTimestampColumn("event_time", precision: 6)
            .AddLevelColumn("severity", asString: true)
            .AddMessageColumn("log_message")
            .WithEngine(new CustomEngine("ENGINE = MergeTree() ORDER BY (event_time)"))
            .Build();

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = schema,
        };

        using var sink = new ClickHouseSink(options);

        var logEvent = new LogEventBuilder()
            .WithLevel(LogEventLevel.Warning)
            .WithMessage("Custom schema test")
            .Build();

        await sink.EmitBatchAsync(new[] { logEvent });

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT severity, log_message FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("Warning"));
        Assert.That(reader.GetString(1), Is.EqualTo("Custom schema test"));
    }

    [Test]
    public async Task EmitBatchAsync_InvokesOnBatchWritten_OnSuccess()
    {
        var table = UniqueTable("callback");
        int? capturedCount = null;
        TimeSpan? capturedDuration = null;

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = DefaultSchema.Create(table),
            OnBatchWritten = (count, duration) =>
            {
                capturedCount = count;
                capturedDuration = duration;
            },
        };

        using var sink = new ClickHouseSink(options);

        var logEvents = new[]
        {
            new LogEventBuilder().WithMessage("Test 1").Build(),
            new LogEventBuilder().WithMessage("Test 2").Build(),
            new LogEventBuilder().WithMessage("Test 3").Build(),
        };

        await sink.EmitBatchAsync(logEvents);

        Assert.That(capturedCount, Is.EqualTo(3));
        Assert.That(capturedDuration, Is.Not.Null);
        Assert.That(capturedDuration!.Value, Is.GreaterThanOrEqualTo(TimeSpan.Zero));
    }

    [Test]
    public async Task EmitBatchAsync_DoesNotThrow_WhenCallbackIsNull()
    {
        var table = UniqueTable("no_callback");
        var options = CreateOptions(table);

        using var sink = new ClickHouseSink(options);

        var logEvents = new[] { new LogEventBuilder().WithMessage("Test").Build() };

        // Should not throw even though no callbacks are configured
        await sink.EmitBatchAsync(logEvents);
    }

    [Test]
    public async Task EmitBatchAsync_HandlesMissingProperty_ForNonNullableColumn()
    {
        var table = UniqueTable("missing_prop");
        var schema = new SchemaBuilder()
            .WithTableName(table)
            .AddTimestampColumn()
            .AddMessageColumn()
            .AddPropertyColumn("UserId", "Int64", writeMethod: PropertyWriteMethod.Raw)
            .Build();

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = schema,
        };

        using var sink = new ClickHouseSink(options);

        // Log event WITHOUT the UserId property — writer returns DBDefault.Value
        var logEvent = new LogEventBuilder()
            .WithMessage("No user id here")
            .Build();

        await sink.EmitBatchAsync(new[] { logEvent });

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT UserId, message FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);
        // ClickHouse should apply the column default (0 for Int64)
        Assert.That(Convert.ToInt64(reader.GetValue(0)), Is.EqualTo(0));
        Assert.That(reader.GetString(1), Is.EqualTo("No user id here"));
    }

    [Test]
    public async Task EmitBatchAsync_PropertiesStoredAsJson()
    {
        var table = UniqueTable("props_json");
        var options = CreateOptions(table);

        using var sink = new ClickHouseSink(options);

        var logEvent = new LogEventBuilder()
            .WithMessage("Test {Application} {UserId}")
            .WithProperty("Application", "MyApp")
            .WithProperty("UserId", 42)
            .Build();

        await sink.EmitBatchAsync(new[] { logEvent });

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT properties FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);
        var json = reader.GetString(0);
        Assert.That(json, Does.Contain("Application"));
        Assert.That(json, Does.Contain("MyApp"));
        Assert.That(json, Does.Contain("UserId"));
    }

    [Test]
    public async Task EmitBatchAsync_PropertiesWithMixedTypes_StoredAsValidJson()
    {
        var table = UniqueTable("props_mixed");
        var schema = new SchemaBuilder()
            .WithTableName(table)
            .AddTimestampColumn()
            .AddLevelColumn()
            .AddMessageColumn()
            .AddMessageTemplateColumn()
            .AddExceptionColumn()
            .AddPropertiesColumn("properties",
                "JSON(StringProp String, IntProp Int32, DoubleProp Float64, BoolProp Bool, NullProp Nullable(String))")
            .Build();
        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = schema,
            TableCreation = new TableCreationOptions { Mode = TableCreationMode.CreateIfNotExists },
        };

        using var sink = new ClickHouseSink(options);

        var logEvent = new LogEventBuilder()
            .WithMessage("Mixed types test")
            .WithProperty("StringProp", "hello world")
            .WithProperty("IntProp", 42)
            .WithProperty("DoubleProp", 3.14)
            .WithProperty("BoolProp", true)
            .WithProperty("NullProp", null)
            .Build();

        await sink.EmitBatchAsync(new[] { logEvent });

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT properties FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);
        var json = reader.GetString(0);

        var parsed = System.Text.Json.JsonDocument.Parse(json);
        var root = parsed.RootElement;

        Assert.That(root.GetProperty("StringProp").GetString(), Is.EqualTo("hello world"));
        Assert.That(root.GetProperty("IntProp").GetInt32(), Is.EqualTo(42));
        Assert.That(root.GetProperty("DoubleProp").GetDouble(), Is.EqualTo(3.14));
        Assert.That(root.GetProperty("BoolProp").GetBoolean(), Is.True);
        // nulls are ignored
    }

    [Test]
    public async Task EmitBatchAsync_PropertiesWithSpecialCharacters_StoredAsValidJson()
    {
        var table = UniqueTable("props_special");
        var options = CreateOptions(table);

        using var sink = new ClickHouseSink(options);

        var logEvent = new LogEventBuilder()
            .WithMessage("Special chars test")
            .WithProperty("Query", "SELECT * FROM \"users\" WHERE name = 'O\\'Brien'")
            .WithProperty("Path", "C:\\Users\\test\\file.txt")
            .WithProperty("Multiline", "line1\nline2\ttab")
            .Build();

        await sink.EmitBatchAsync(new[] { logEvent });

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT properties FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);
        var json = reader.GetString(0);

        // Must be valid JSON — the real test is that parsing doesn't throw
        var parsed = System.Text.Json.JsonDocument.Parse(json);
        var root = parsed.RootElement;

        Assert.That(root.GetProperty("Query").GetString(), Does.Contain("SELECT"));
        Assert.That(root.GetProperty("Path").GetString(), Does.Contain("C:\\"));
        Assert.That(root.GetProperty("Multiline").GetString(), Does.Contain("line1"));
    }

    [Test]
    public async Task EmitBatchAsync_EmptyProperties_StoredAsEmptyJson()
    {
        var table = UniqueTable("props_empty");
        var options = CreateOptions(table);

        using var sink = new ClickHouseSink(options);

        var logEvent = new LogEventBuilder()
            .WithMessage("No properties here")
            .Build();

        await sink.EmitBatchAsync(new[] { logEvent });

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT properties FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);
        var json = reader.GetString(0);

        Assert.DoesNotThrow(() => System.Text.Json.JsonDocument.Parse(json));
    }

    [Test]
    public async Task EmitBatchAsync_PropertiesWithUnicodeValues_StoredCorrectly()
    {
        var table = UniqueTable("props_unicode");
        var options = CreateOptions(table);

        using var sink = new ClickHouseSink(options);

        var logEvent = new LogEventBuilder()
            .WithMessage("Unicode test")
            .WithProperty("Japanese", "\u65e5\u672c\u8a9e")
            .WithProperty("Emoji", "\ud83d\ude80\ud83c\udf1f")
            .WithProperty("Mixed", "Hello \u4e16\u754c World")
            .Build();

        await sink.EmitBatchAsync(new[] { logEvent });

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT properties FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);
        var json = reader.GetString(0);

        var parsed = System.Text.Json.JsonDocument.Parse(json);
        var root = parsed.RootElement;

        Assert.That(root.GetProperty("Japanese").GetString(), Is.EqualTo("\u65e5\u672c\u8a9e"));
        Assert.That(root.GetProperty("Emoji").GetString(), Is.EqualTo("\ud83d\ude80\ud83c\udf1f"));
        Assert.That(root.GetProperty("Mixed").GetString(), Is.EqualTo("Hello \u4e16\u754c World"));
    }

    [Test]
    public async Task EmitBatchAsync_PropertiesPreservedAcrossMultipleEvents()
    {
        var table = UniqueTable("props_batch");
        var schema = new SchemaBuilder()
            .WithTableName(table)
            .AddTimestampColumn()
            .AddLevelColumn()
            .AddMessageColumn()
            .AddMessageTemplateColumn()
            .AddExceptionColumn()
            .AddPropertiesColumn("properties",
                "JSON(RequestId String, StatusCode Int32, ErrorDetail Nullable(String))")
            .Build();
        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = schema,
            TableCreation = new TableCreationOptions { Mode = TableCreationMode.CreateIfNotExists },
        };

        using var sink = new ClickHouseSink(options);

        var events = new[]
        {
            new LogEventBuilder()
                .WithMessage("Event 1")
                .WithProperty("RequestId", "aaa-111")
                .WithProperty("StatusCode", 200)
                .Build(),
            new LogEventBuilder()
                .WithMessage("Event 2")
                .WithProperty("RequestId", "bbb-222")
                .WithProperty("StatusCode", 404)
                .Build(),
            new LogEventBuilder()
                .WithMessage("Event 3")
                .WithProperty("RequestId", "ccc-333")
                .WithProperty("StatusCode", 500)
                .WithProperty("ErrorDetail", "Internal server error")
                .Build(),
        };

        await sink.EmitBatchAsync(events);

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT properties FROM {SqlGenerator.EscapeTableName(table)} ORDER BY properties.RequestId");

        var rows = new List<System.Text.Json.JsonElement>();
        while (reader.Read())
        {
            var json = reader.GetString(0);
            rows.Add(System.Text.Json.JsonDocument.Parse(json).RootElement);
        }

        Assert.That(rows, Has.Count.EqualTo(3));
        Assert.That(rows[0].GetProperty("RequestId").GetString(), Is.EqualTo("aaa-111"));
        Assert.That(rows[0].GetProperty("StatusCode").GetInt32(), Is.EqualTo(200));
        Assert.That(rows[1].GetProperty("RequestId").GetString(), Is.EqualTo("bbb-222"));
        Assert.That(rows[1].GetProperty("StatusCode").GetInt32(), Is.EqualTo(404));
        Assert.That(rows[2].GetProperty("ErrorDetail").GetString(), Is.EqualTo("Internal server error"));
    }
}
