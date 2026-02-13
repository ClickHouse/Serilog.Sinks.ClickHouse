using ClickHouse.Driver;
using ClickHouse.Driver.ADO;
using Serilog.Events;
using Serilog.Sinks.ClickHouse.Client;
using Serilog.Sinks.ClickHouse.Configuration;
using Serilog.Sinks.ClickHouse.Schema;
using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Integration;

/// <summary>
/// Integration tests for the extension-method overloads that accept
/// ClickHouseClientSettings, IClickHouseClient, and ClickHouseDataSource.
/// Each test writes events through the Serilog pipeline and verifies
/// they arrive in ClickHouse.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ExtensionOverloadsIntegrationTests
{
    private string ConnectionString => IntegrationTestFixture.ConnectionString;

    private static string UniqueTable(string prefix = "ext") => $"{prefix}_{Guid.NewGuid():N}";

    private async Task<long> CountRows(string table)
    {
        using var client = new ClickHouseClient(ConnectionString);
        var result = await client.ExecuteScalarAsync($"SELECT count() FROM {SqlGenerator.EscapeTableName(table)}");
        return Convert.ToInt64(result);
    }

    // ── ConnectionString overload ─────────────────────────────────

    [Test]
    public async Task ConnectionString_Overload_WritesEvents()
    {
        var table = UniqueTable("connstr");

        using var logger = new LoggerConfiguration()
            .WriteTo.ClickHouse(
                connectionString: ConnectionString,
                tableName: table,
                batchSizeLimit: 10)
            .CreateLogger();

        logger.Information("Hello from {Source}", "ConnectionString overload");
        await logger.DisposeAsync();

        Assert.That(await CountRows(table), Is.GreaterThanOrEqualTo(1));
    }

    // ── IClickHouseClient simple overload ─────────────────────────

    [Test]
    public async Task Client_SimpleOverload_WritesEvents()
    {
        var table = UniqueTable("client_simple");
        using var client = new ClickHouseClient(ConnectionString);

        using var logger = new LoggerConfiguration()
            .WriteTo.ClickHouse(
                client: client,
                tableName: table,
                batchSizeLimit: 10)
            .CreateLogger();

        logger.Information("Hello from {Source}", "IClickHouseClient simple overload");
        await logger.DisposeAsync();

        // Read with a separate client since the first was handed off to the sink
        Assert.That(await CountRows(table), Is.GreaterThanOrEqualTo(1));
    }

    // ── IClickHouseClient full-options overload ───────────────────

    [Test]
    public async Task Client_FullOptionsOverload_WritesEvents()
    {
        var table = UniqueTable("client_full");
        using var client = new ClickHouseClient(ConnectionString);

        var options = new ClickHouseSinkOptions
        {
            Schema = DefaultSchema.Create(table).Build(),
        };

        using var logger = new LoggerConfiguration()
            .WriteTo.ClickHouse(options, client)
            .CreateLogger();

        logger.Warning("Hello from {Source}", "IClickHouseClient full-options overload");
        await logger.DisposeAsync();

        Assert.That(await CountRows(table), Is.GreaterThanOrEqualTo(1));
    }

    // ── ClickHouseClientSettings overload ─────────────────────────

    [Test]
    public async Task Settings_Overload_WritesEvents()
    {
        var table = UniqueTable("settings");
        var settings = new ClickHouseClientSettings(ConnectionString);

        using var logger = new LoggerConfiguration()
            .WriteTo.ClickHouse(
                settings: settings,
                tableName: table,
                batchSizeLimit: 10)
            .CreateLogger();

        logger.Information("Hello from {Source}", "ClickHouseClientSettings overload");
        await logger.DisposeAsync();

        Assert.That(await CountRows(table), Is.GreaterThanOrEqualTo(1));
    }

#if NET7_0_OR_GREATER
    // ── ClickHouseDataSource simple overload ──────────────────────

    [Test]
    public async Task DataSource_SimpleOverload_WritesEvents()
    {
        var table = UniqueTable("ds_simple");
        await using var dataSource = new ClickHouseDataSource(ConnectionString);

        using var logger = new LoggerConfiguration()
            .WriteTo.ClickHouse(
                dataSource: dataSource,
                tableName: table,
                batchSizeLimit: 10)
            .CreateLogger();

        logger.Information("Hello from {Source}", "ClickHouseDataSource simple overload");
        await logger.DisposeAsync();

        Assert.That(await CountRows(table), Is.GreaterThanOrEqualTo(1));
    }

    // ── ClickHouseDataSource full-options overload ────────────────

    [Test]
    public async Task DataSource_FullOptionsOverload_WritesEvents()
    {
        var table = UniqueTable("ds_full");
        await using var dataSource = new ClickHouseDataSource(ConnectionString);

        var options = new ClickHouseSinkOptions
        {
            Schema = DefaultSchema.Create(table).Build(),
        };

        using var logger = new LoggerConfiguration()
            .WriteTo.ClickHouse(options, dataSource)
            .CreateLogger();

        logger.Warning("Hello from {Source}", "ClickHouseDataSource full-options overload");
        await logger.DisposeAsync();

        Assert.That(await CountRows(table), Is.GreaterThanOrEqualTo(1));
    }
#endif

    // ── Optional parameters propagated correctly ──────────────────

    [Test]
    public async Task Client_SimpleOverload_RespectsDatabase()
    {
        var database = $"db_{Guid.NewGuid():N}";
        var table = UniqueTable("client_db");

        // Create the database first
        using (var adminClient = new ClickHouseClient(ConnectionString))
        {
            await adminClient.ExecuteNonQueryAsync($"CREATE DATABASE IF NOT EXISTS {database}");
        }

        try
        {
            using var client = new ClickHouseClient(ConnectionString);

            using var logger = new LoggerConfiguration()
                .WriteTo.ClickHouse(
                    client: client,
                    tableName: table,
                    database: database,
                    batchSizeLimit: 10)
                .CreateLogger();

            logger.Information("Hello from {Source}", "database test");
            await logger.DisposeAsync();

            // Verify the table was created in the correct database
            using var verifyClient = new ClickHouseClient(ConnectionString);
            var count = await verifyClient.ExecuteScalarAsync(
                $"SELECT count() FROM {database}.{SqlGenerator.EscapeTableName(table)}");
            Assert.That(Convert.ToInt64(count), Is.GreaterThanOrEqualTo(1));
        }
        finally
        {
            using var cleanup = new ClickHouseClient(ConnectionString);
            await cleanup.ExecuteNonQueryAsync($"DROP DATABASE IF EXISTS {database}");
        }
    }

    [Test]
    public async Task ConnectionString_Overload_RespectsMinimumLevel()
    {
        var table = UniqueTable("minlevel");

        using var logger = new LoggerConfiguration()
            .WriteTo.ClickHouse(
                connectionString: ConnectionString,
                tableName: table,
                batchSizeLimit: 10,
                minimumLevel: LogEventLevel.Warning)
            .CreateLogger();

        // These should be filtered out
        logger.Debug("Should be filtered");
        logger.Information("Should also be filtered");

        // This should pass through
        logger.Warning("Should be written");

        await logger.DisposeAsync();

        Assert.That(await CountRows(table), Is.EqualTo(1));
    }

    // ── Action<SchemaBuilder> overload ────────────────────────────

    [Test]
    public async Task SchemaBuilderAction_Overload_WritesEvents()
    {
        var table = UniqueTable("schema_action");

        using var logger = new LoggerConfiguration()
            .WriteTo.ClickHouse(
                connectionString: ConnectionString,
                configureSchema: schema => schema
                    .WithTableName(table)
                    .AddTimestampColumn()
                    .AddLevelColumn()
                    .AddMessageColumn(),
                batchSizeLimit: 10)
            .CreateLogger();

        logger.Information("Hello from {Source}", "SchemaBuilder action overload");
        await logger.DisposeAsync();

        Assert.That(await CountRows(table), Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task SchemaBuilderAction_Overload_WithCustomColumns_WritesCorrectData()
    {
        var table = UniqueTable("schema_action_custom");

        using var logger = new LoggerConfiguration()
            .WriteTo.ClickHouse(
                connectionString: ConnectionString,
                configureSchema: schema => schema
                    .WithTableName(table)
                    .AddTimestampColumn("event_time", precision: 6)
                    .AddLevelColumn("severity", asString: true)
                    .AddMessageColumn("log_message")
                    .AddExceptionColumn(),
                batchSizeLimit: 10)
            .CreateLogger();

        logger.Warning("Custom schema builder {Detail}", "works");
        await logger.DisposeAsync();

        using var client = new ClickHouseClient(ConnectionString);
        var reader = await client.ExecuteReaderAsync(
            $"SELECT severity, log_message FROM {SqlGenerator.EscapeTableName(table)} LIMIT 1");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(0), Is.EqualTo("Warning"));
        Assert.That(reader.GetString(1), Does.Contain("Custom schema builder"));
    }
}
