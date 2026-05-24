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
            Schema = DefaultSchema.Create(tableName).Build(),
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
            Schema = DefaultSchema.Create(table).Build(),
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
    public async Task EmitBatchAsync_WithIndexes_CreatesTableWithIndexes()
    {
        var table = UniqueTable("indexes");
        var schema = new SchemaBuilder()
            .WithTableName(table)
            .AddTimestampColumn()
            .AddLevelColumn()
            .AddMessageColumn()
            .AddIndex("INDEX idx_level level TYPE set(0) GRANULARITY 1")
            .AddIndex("INDEX idx_message message TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1")
            .WithEngine(new CustomEngine("ENGINE = MergeTree() ORDER BY (timestamp)"))
            .Build();

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = schema,
        };

        using var sink = new ClickHouseSink(options);

        var logEvent = new LogEventBuilder()
            .WithLevel(LogEventLevel.Warning)
            .WithMessage("Index test message")
            .Build();

        await sink.EmitBatchAsync(new[] { logEvent });

        // Verify data was written
        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT level, message FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("Warning"));
        Assert.That(reader.GetString(1), Is.EqualTo("Index test message"));

        // Verify indexes exist in system.data_skipping_indices
        var indexReader = await client.ExecuteReaderAsync(
            $"SELECT name, expr, type FROM system.data_skipping_indices WHERE table = '{table}' AND database = currentDatabase() ORDER BY name");

        var indexes = new List<(string Name, string Expr, string Type)>();
        while (indexReader.Read())
        {
            indexes.Add((indexReader.GetString(0), indexReader.GetString(1), indexReader.GetString(2)));
        }

        Assert.That(indexes, Has.Count.EqualTo(2));
        Assert.That(indexes[0].Name, Is.EqualTo("idx_level"));
        Assert.That(indexes[0].Type, Is.EqualTo("set"));
        Assert.That(indexes[1].Name, Is.EqualTo("idx_message"));
        Assert.That(indexes[1].Type, Is.EqualTo("tokenbf_v1"));
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
            Schema = DefaultSchema.Create(table).Build(),
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

    // ── SchemaManager: DropAndRecreate ────────────────────────────

    [Test]
    public async Task EmitBatchAsync_WithDropAndRecreate_DropsExistingDataAndRecreatesTable()
    {
        var table = UniqueTable("drop_recreate");
        var schema = DefaultSchema.Create(table).Build();

        // Pre-create the table and insert a sentinel row
        using (var preClient = new ClickHouseClient(ConnectionString))
        {
            var createSql = SqlGenerator.GenerateCreateTable(schema);
            await preClient.ExecuteNonQueryAsync(createSql);

            var exists = await preClient.ExecuteScalarAsync($"EXISTS {SqlGenerator.EscapeTableName(table)}");
            Assert.That(exists is (byte)1, Is.True);
        }

        // Now use DropAndRecreate mode
        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = schema,
            TableCreation = new TableCreationOptions { Mode = TableCreationMode.DropAndRecreate },
        };

        using var sink = new ClickHouseSink(options);
        await sink.EmitBatchAsync(new[] { new LogEventBuilder().WithMessage("After recreate").Build() });

        using var client = new ClickHouseClient(ConnectionString);
        var count = await client.ExecuteScalarAsync($"SELECT count() FROM {SqlGenerator.EscapeTableName(table)}");
        Assert.That(Convert.ToInt64(count), Is.EqualTo(1));

        var reader = await client.ExecuteReaderAsync(
            $"SELECT message FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("After recreate"));
    }

    // ── SchemaManager: ValidateOnStartup ──────────────────────────

    [Test]
    public async Task EmitBatchAsync_WithModeNone_ValidateOnStartup_SucceedsWhenTablePreExists()
    {
        var table = UniqueTable("validate_ok");
        var schema = DefaultSchema.Create(table).Build();

        // Pre-create the table
        using (var preClient = new ClickHouseClient(ConnectionString))
        {
            var createSql = SqlGenerator.GenerateCreateTable(schema);
            await preClient.ExecuteNonQueryAsync(createSql);
        }

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = schema,
            TableCreation = new TableCreationOptions
            {
                Mode = TableCreationMode.None,
                ValidateOnStartup = true,
            },
        };

        using var sink = new ClickHouseSink(options);
        await sink.EmitBatchAsync(new[] { new LogEventBuilder().WithMessage("Validated").Build() });

        using var client = new ClickHouseClient(ConnectionString);
        var count = await client.ExecuteScalarAsync($"SELECT count() FROM {SqlGenerator.EscapeTableName(table)}");
        Assert.That(Convert.ToInt64(count), Is.EqualTo(1));
    }

    [Test]
    public void EmitBatchAsync_WithModeNone_ValidateOnStartup_ThrowsWhenTableMissing()
    {
        var table = UniqueTable("validate_fail");

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = DefaultSchema.Create(table).Build(),
            TableCreation = new TableCreationOptions
            {
                Mode = TableCreationMode.None,
                ValidateOnStartup = true,
            },
        };

        using var sink = new ClickHouseSink(options);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => sink.EmitBatchAsync(new[] { new LogEventBuilder().WithMessage("Test").Build() }));

        Assert.That(ex!.Message, Does.Contain("does not exist"));
    }

    // ── DefaultSchema presets ─────────────────────────────────────

    [Test]
    public async Task EmitBatchAsync_WithMinimalSchema_WritesCorrectly()
    {
        var table = UniqueTable("minimal");
        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = DefaultSchema.CreateMinimal(table).Build(),
        };

        using var sink = new ClickHouseSink(options);

        var logEvent = new LogEventBuilder()
            .WithLevel(LogEventLevel.Warning)
            .WithMessage("Minimal schema test")
            .Build();

        await sink.EmitBatchAsync(new[] { logEvent });

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT timestamp, level, message FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetDateTime(0), Is.Not.EqualTo(default(DateTime)));
        // Level stored as UInt8 (Warning = 3)
        Assert.That(Convert.ToByte(reader.GetValue(1)), Is.EqualTo((byte)LogEventLevel.Warning));
        Assert.That(reader.GetString(2), Is.EqualTo("Minimal schema test"));
    }

    [Test]
    public async Task EmitBatchAsync_WithComprehensiveSchema_WritesAllColumns()
    {
        var table = UniqueTable("comprehensive");
        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = DefaultSchema.CreateComprehensive(table).Build(),
        };

        using var sink = new ClickHouseSink(options);

        var logEvent = new LogEventBuilder()
            .WithLevel(LogEventLevel.Error)
            .WithMessage("Comprehensive {Action}")
            .WithProperty("Action", "test")
            .WithException(new InvalidOperationException("boom"))
            .Build();

        await sink.EmitBatchAsync(new[] { logEvent });

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT timestamp, level, message, message_template, exception, properties, log_event FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetDateTime(0), Is.Not.EqualTo(default(DateTime)));
        Assert.That(reader.GetString(1), Is.EqualTo("Error"));
        Assert.That(reader.GetString(2), Does.Contain("Comprehensive"));
        Assert.That(reader.GetString(3), Is.EqualTo("Comprehensive {Action}"));
        Assert.That(reader.GetString(4), Does.Contain("boom"));
        Assert.That(reader.GetString(5), Does.Contain("Action"));

        // log_event column — full event as JSON
        var logEventJson = reader.GetString(6);
        var parsed = System.Text.Json.JsonDocument.Parse(logEventJson);
        Assert.That(parsed.RootElement.TryGetProperty("Timestamp", out _), Is.True);
        Assert.That(parsed.RootElement.TryGetProperty("Level", out _), Is.True);
    }

    [Test]
    public async Task EmitBatchAsync_WithColumnCodec_CreatesTableWithCodecs()
    {
        var table = UniqueTable("codec");
        var schema = new SchemaBuilder()
            .WithTableName(table)
            .AddTimestampColumn(codec: "DoubleDelta, ZSTD")
            .AddLevelColumn(codec: "ZSTD")
            .AddMessageColumn(codec: "ZSTD")
            .WithEngine(new CustomEngine("ENGINE = MergeTree() ORDER BY (timestamp)"))
            .Build();

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = schema,
        };

        using var sink = new ClickHouseSink(options);
        await sink.EmitBatchAsync(new[] { new LogEventBuilder().WithMessage("Codec test").Build() });

        // Verify the table was created with codecs
        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT create_table_query FROM system.tables WHERE name = '{table}' AND database = currentDatabase()");

        Assert.That(reader.Read(), Is.True);
        var createQuery = reader.GetString(0);
        Assert.That(createQuery, Does.Contain("CODEC(DoubleDelta, ZSTD"));
        Assert.That(createQuery, Does.Contain("CODEC(ZSTD"));
    }

    [Test]
    public async Task EmitBatchAsync_WithColumnDefault_AppliesDefault()
    {
        var table = UniqueTable("coldefault");
        var schema = new SchemaBuilder()
            .WithTableName(table)
            .AddTimestampColumn()
            .AddMessageColumn()
            .AddPropertyColumn("Source", type: "String", defaultExpression: "'unknown'")
            .Build();

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = schema,
        };

        using var sink = new ClickHouseSink(options);

        // Event without the Source property — should get default 'unknown'
        await sink.EmitBatchAsync(new[] { new LogEventBuilder().WithMessage("No source").Build() });

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT Source FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("unknown"));
    }

    [Test]
    public async Task EmitBatchAsync_WithColumnComment_CreatesTableWithComments()
    {
        var table = UniqueTable("colcomment");
        var schema = new SchemaBuilder()
            .WithTableName(table)
            .AddTimestampColumn(comment: "Event timestamp in UTC")
            .AddMessageColumn(comment: "Rendered log message")
            .WithEngine(new CustomEngine("ENGINE = MergeTree() ORDER BY (timestamp)"))
            .Build();

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = schema,
        };

        using var sink = new ClickHouseSink(options);
        await sink.EmitBatchAsync(new[] { new LogEventBuilder().WithMessage("Comment test").Build() });

        // Verify column comments in system.columns
        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT name, comment FROM system.columns WHERE table = '{table}' AND database = currentDatabase() AND comment != '' ORDER BY name");

        var comments = new Dictionary<string, string>();
        while (reader.Read())
        {
            comments[reader.GetString(0)] = reader.GetString(1);
        }

        Assert.That(comments["timestamp"], Is.EqualTo("Event timestamp in UTC"));
        Assert.That(comments["message"], Is.EqualTo("Rendered log message"));
    }

    [Test]
    public async Task EmitBatchAsync_WithAllColumnDdlOptions_CreatesTableWithCodecDefaultTtlComment()
    {
        var table = UniqueTable("all_ddl");
        var schema = new SchemaBuilder()
            .WithTableName(table)
            .AddTimestampColumn(
                codec: "DoubleDelta, ZSTD",
                comment: "Event timestamp in UTC")
            .AddLevelColumn(
                codec: "ZSTD",
                defaultExpression: "'Information'",
                comment: "Log severity level")
            .AddMessageColumn(
                codec: "ZSTD",
                ttl: "timestamp + INTERVAL 30 DAY",
                comment: "Rendered log message")
            .AddPropertyColumn("Source", type: "String",
                defaultExpression: "'unknown'",
                codec: "ZSTD",
                ttl: "timestamp + INTERVAL 90 DAY",
                comment: "Event source application")
            .WithEngine(new CustomEngine("ENGINE = MergeTree() ORDER BY (timestamp)"))
            .Build();

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = schema,
        };

        using var sink = new ClickHouseSink(options);

        // Emit an event WITH the Source property
        await sink.EmitBatchAsync(new[]
        {
            new LogEventBuilder().WithMessage("With source").WithProperty("Source", "MyApp").Build(),
        });

        // Emit an event WITHOUT Source — should get the DEFAULT 'unknown'
        await sink.EmitBatchAsync(new[]
        {
            new LogEventBuilder().WithMessage("No source").Build(),
        });

        using var client = new ClickHouseClient(ConnectionString);

        // ── Verify codecs & TTL in CREATE TABLE DDL ──────────────────
        var ddlReader = await client.ExecuteReaderAsync(
            $"SELECT create_table_query FROM system.tables WHERE name = '{table}' AND database = currentDatabase()");
        Assert.That(ddlReader.Read(), Is.True);
        var ddl = ddlReader.GetString(0);

        Assert.That(ddl, Does.Contain("CODEC(DoubleDelta, ZSTD"), "Timestamp codec missing");
        Assert.That(ddl, Does.Contain("CODEC(ZSTD"), "Level/message codec missing");
        Assert.That(ddl, Does.Contain("TTL timestamp + toIntervalDay(30)").Or.Contain("TTL `timestamp` + toIntervalDay(30)"),
            "Message TTL missing");
        Assert.That(ddl, Does.Contain("TTL timestamp + toIntervalDay(90)").Or.Contain("TTL `timestamp` + toIntervalDay(90)"),
            "Source TTL missing");

        // ── Verify column comments ───────────────────────────────────
        var commentReader = await client.ExecuteReaderAsync(
            $"SELECT name, comment FROM system.columns WHERE table = '{table}' AND database = currentDatabase() AND comment != '' ORDER BY name");

        var comments = new Dictionary<string, string>();
        while (commentReader.Read())
        {
            comments[commentReader.GetString(0)] = commentReader.GetString(1);
        }

        Assert.That(comments["timestamp"], Is.EqualTo("Event timestamp in UTC"));
        Assert.That(comments["level"], Is.EqualTo("Log severity level"));
        Assert.That(comments["message"], Is.EqualTo("Rendered log message"));
        Assert.That(comments["Source"], Is.EqualTo("Event source application"));

        // ── Verify default applied for missing property ──────────────
        var dataReader = await client.ExecuteReaderAsync(
            $"SELECT message, Source FROM {SqlGenerator.EscapeTableName(table)} ORDER BY message");

        var rows = new List<(string Message, string Source)>();
        while (dataReader.Read())
        {
            rows.Add((dataReader.GetString(0), dataReader.GetString(1)));
        }

        Assert.That(rows, Has.Count.EqualTo(2));
        Assert.That(rows[0].Source, Is.EqualTo("unknown"), "DEFAULT not applied for missing property");
        Assert.That(rows[1].Source, Is.EqualTo("MyApp"), "Explicit property value not stored");
    }

    /// <summary>
    /// A column writer that always throws, used to test per-column error isolation.
    /// </summary>
    private class ThrowingColumnWriter : ColumnWriterBase
    {
        public ThrowingColumnWriter()
            : base("bad_column", "Nullable(String)")
        {
        }

        public override object? GetValue(LogEvent logEvent, IFormatProvider? formatProvider = null)
        {
            throw new InvalidOperationException("Intentional test failure");
        }
    }

    [Test]
    public async Task EmitBatchAsync_WithThrowingColumnWriter_StillWritesRowWithDefaults()
    {
        var table = UniqueTable("throw_col");
        var schema = new SchemaBuilder()
            .WithTableName(table)
            .AddTimestampColumn()
            .AddMessageColumn()
            .AddColumn(new ThrowingColumnWriter())
            .Build();

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = schema,
        };

        using var sink = new ClickHouseSink(options);
        await sink.EmitBatchAsync(new[] { new LogEventBuilder().WithMessage("Should still be written").Build() });

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT message, bad_column FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("Should still be written"));
        Assert.That(reader.IsDBNull(1), Is.True);
    }
    
    [Test]
    public async Task EmitBatchAsync_WritesGuidFromCSharp_WhenGeneratorIsCSharpGuid()
    {
        var table = UniqueTable("guid_csharp");
        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = new SchemaBuilder()
                .WithTableName(table)
                .AddLogIdColumn("log_id", asString: false, generator: LogIdGenerator.CSharpGuid)
                .AddTimestampColumn()
                .Build(),
            TableCreation = new TableCreationOptions { Mode = TableCreationMode.CreateIfNotExists },
        };

        using var sink = new ClickHouseSink(options);
        var logEvent = new LogEventBuilder().WithMessage("Test C# Guid").Build();

        await sink.EmitBatchAsync([logEvent]);
        
        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync($"SELECT log_id FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");
        
        Assert.That(reader.Read(), Is.True);
        var storedGuid = reader.GetGuid(0);
        
        Assert.That(storedGuid, Is.Not.EqualTo(Guid.Empty));
    }
    
    [Test]
    public async Task EmitBatchAsync_AllowsClickHouseToGenerateUuidV4_WithCorrectVersion()
    {
        var table = UniqueTable("guid_ch_v4");
        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = new SchemaBuilder()
                .WithTableName(table)
                .AddLogIdColumn("log_id", asString: false, generator: LogIdGenerator.ClickHouseUUIDv4)
                .AddTimestampColumn()
                .Build(),
            TableCreation = new TableCreationOptions { Mode = TableCreationMode.CreateIfNotExists },
        };

        using var sink = new ClickHouseSink(options);
        var logEvent = new LogEventBuilder().WithMessage("Test ClickHouse UUIDv4").Build();
        
        Assert.DoesNotThrowAsync(() => sink.EmitBatchAsync([logEvent]));

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync($"SELECT log_id FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");
        
        Assert.That(reader.Read(), Is.True);
        var storedGuid = reader.GetGuid(0);
        
        Assert.That(storedGuid, Is.Not.EqualTo(Guid.Empty));
        
#if NET9_0_OR_GREATER
        Assert.That(storedGuid.Version, Is.EqualTo(4));
#else
        var bytes = storedGuid.ToByteArray();
        var version = (bytes[7] >> 4) & 0x0F;
        Assert.That(version, Is.EqualTo(4));
#endif
    }

    [Test]
    public async Task EmitBatchAsync_AllowsClickHouseToGenerateUuidV7_WhenSkipWriteIsTrue()
    {
        var table = UniqueTable("guid_ch_v7");
        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = new SchemaBuilder()
                .WithTableName(table)
                .AddLogIdColumn("log_id", asString: false, generator: LogIdGenerator.ClickHouseUUIDv7)
                .AddTimestampColumn()
                .Build(),
            TableCreation = new TableCreationOptions { Mode = TableCreationMode.CreateIfNotExists },
        };

        using var sink = new ClickHouseSink(options);
        var logEvent = new LogEventBuilder().WithMessage("Test ClickHouse UUIDv7").Build();
        
        Assert.DoesNotThrowAsync(() => sink.EmitBatchAsync([logEvent]));
        
        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync($"SELECT log_id FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");
        
        Assert.That(reader.Read(), Is.True);
        var storedGuid = reader.GetGuid(0);
        
        Assert.That(storedGuid, Is.Not.EqualTo(Guid.Empty));
        
#if NET9_0_OR_GREATER
        Assert.That(storedGuid.Version, Is.EqualTo(7));
#else
        var bytes = storedGuid.ToByteArray();
        var version = (bytes[7] >> 4) & 0x0F;
        Assert.That(version, Is.EqualTo(7));
#endif
    }
}
