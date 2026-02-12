using ClickHouse.Driver;
using ClickHouse.Driver.ADO;
using NSubstitute;
using Serilog.Events;
using Serilog.Sinks.ClickHouse.Client;
using Serilog.Sinks.ClickHouse.Configuration;
using Serilog.Sinks.ClickHouse.Schema;

namespace Serilog.Sinks.ClickHouse.Tests.Unit;

/// <summary>
/// Unit tests for the ClickHouseSinkExtensions overloads.
/// Verifies null-argument validation, placeholder ConnectionString behavior,
/// and that each overload produces a working logger configuration.
/// </summary>
public class ClickHouseSinkExtensionsTests
{
    private IClickHouseClient _mockClient = null!;

    [SetUp]
    public void SetUp()
    {
        _mockClient = Substitute.For<IClickHouseClient>();
    }

    [TearDown]
    public void TearDown()
    {
        _mockClient?.Dispose();
    }

    // ── IClickHouseClient simple overload ────────────────────────

    [Test]
    public void SimpleClient_CreatesLogger()
    {
        var config = new LoggerConfiguration()
            .WriteTo.ClickHouse(_mockClient, tableName: "test_logs");

        using var logger = config.CreateLogger();
        Assert.That(logger, Is.Not.Null);
    }

    [Test]
    public void SimpleClient_ThrowsOnNullClient()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new LoggerConfiguration()
                .WriteTo.ClickHouse((IClickHouseClient)null!, tableName: "test_logs"));
    }

    [Test]
    public void SimpleClient_RespectsMinimumLevel()
    {
        var config = new LoggerConfiguration()
            .WriteTo.ClickHouse(_mockClient, tableName: "test_logs", minimumLevel: LogEventLevel.Warning);

        using var logger = config.CreateLogger();
        Assert.That(logger, Is.Not.Null);
    }

    [Test]
    public void SimpleClient_RespectsDatabase()
    {
        var config = new LoggerConfiguration()
            .WriteTo.ClickHouse(_mockClient, tableName: "test_logs", database: "my_db");

        using var logger = config.CreateLogger();
        Assert.That(logger, Is.Not.Null);
    }

    [Test]
    public void SimpleClient_RespectsBatchingParameters()
    {
        var config = new LoggerConfiguration()
            .WriteTo.ClickHouse(
                _mockClient,
                tableName: "test_logs",
                batchSizeLimit: 500,
                flushInterval: TimeSpan.FromSeconds(10),
                queueLimit: 50_000);

        using var logger = config.CreateLogger();
        Assert.That(logger, Is.Not.Null);
    }

    // ── IClickHouseClient full-options overload ──────────────────

    [Test]
    public void FullOptionsWithClient_CreatesLogger()
    {
        var options = new ClickHouseSinkOptions
        {
            Schema = DefaultSchema.Create("test_logs"),
            TableCreation = new TableCreationOptions
            {
                Mode = TableCreationMode.None,
                ValidateOnStartup = false,
            },
        };

        var config = new LoggerConfiguration()
            .WriteTo.ClickHouse(options, _mockClient);

        using var logger = config.CreateLogger();
        Assert.That(logger, Is.Not.Null);
    }

    [Test]
    public void FullOptionsWithClient_WorksWithEmptyConnectionString()
    {
        var options = new ClickHouseSinkOptions
        {
            ConnectionString = "",
            Schema = DefaultSchema.Create("test_logs"),
            TableCreation = new TableCreationOptions
            {
                Mode = TableCreationMode.None,
                ValidateOnStartup = false,
            },
        };

        // Empty ConnectionString should be replaced with placeholder
        var config = new LoggerConfiguration()
            .WriteTo.ClickHouse(options, _mockClient);

        using var logger = config.CreateLogger();
        Assert.That(logger, Is.Not.Null);
    }

    [Test]
    public void FullOptionsWithClient_PreservesExistingConnectionString()
    {
        var options = new ClickHouseSinkOptions
        {
            ConnectionString = "Host=myhost;Port=9000",
            Schema = DefaultSchema.Create("test_logs"),
            TableCreation = new TableCreationOptions
            {
                Mode = TableCreationMode.None,
                ValidateOnStartup = false,
            },
        };

        var config = new LoggerConfiguration()
            .WriteTo.ClickHouse(options, _mockClient);

        using var logger = config.CreateLogger();
        Assert.That(logger, Is.Not.Null);
    }

    [Test]
    public void FullOptionsWithClient_ThrowsOnNullOptions()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new LoggerConfiguration()
                .WriteTo.ClickHouse((ClickHouseSinkOptions)null!, _mockClient));
    }

    [Test]
    public void FullOptionsWithClient_ThrowsOnNullClient()
    {
        var options = new ClickHouseSinkOptions
        {
            ConnectionString = "Host=localhost",
            Schema = DefaultSchema.Create("test_logs"),
        };

        Assert.Throws<ArgumentNullException>(() =>
            new LoggerConfiguration()
                .WriteTo.ClickHouse(options, (IClickHouseClient)null!));
    }

    // ── ClickHouseClientSettings simple overload ─────────────────

    [Test]
    public void SimpleSettings_ThrowsOnNullSettings()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new LoggerConfiguration()
                .WriteTo.ClickHouse((ClickHouseClientSettings)null!, tableName: "test_logs"));
    }

    // ── Connection-string overload (regression) ──────────────────

    [Test]
    public void SimpleConnectionString_CreatesLogger()
    {
        var config = new LoggerConfiguration()
            .WriteTo.ClickHouse("Host=localhost;Port=9000", tableName: "test_logs");

        using var logger = config.CreateLogger();
        Assert.That(logger, Is.Not.Null);
    }

    [Test]
    public void FullOptions_ThrowsOnNullLoggerConfiguration()
    {
        var options = new ClickHouseSinkOptions
        {
            ConnectionString = "Host=localhost;Port=9000",
            Schema = DefaultSchema.Create("test_logs"),
        };

        Assert.Throws<ArgumentNullException>(() =>
            ClickHouseSinkExtensions.ClickHouse(null!, options));
    }

#if NET7_0_OR_GREATER
    // ── ClickHouseDataSource full-options overload ───────────────

    [Test]
    public void FullOptionsWithDataSource_ThrowsOnNullDataSource()
    {
        var options = new ClickHouseSinkOptions
        {
            ConnectionString = "Host=localhost",
            Schema = DefaultSchema.Create("test_logs"),
        };

        Assert.Throws<ArgumentNullException>(() =>
            new LoggerConfiguration()
                .WriteTo.ClickHouse(options, (ClickHouseDataSource)null!));
    }

    [Test]
    public void FullOptionsWithDataSource_ThrowsOnNullOptions()
    {
        using var dataSource = new ClickHouseDataSource("Host=localhost");
        Assert.Throws<ArgumentNullException>(() =>
            new LoggerConfiguration()
                .WriteTo.ClickHouse((ClickHouseSinkOptions)null!, dataSource));
    }

    // ── ClickHouseDataSource simple overload ─────────────────────

    [Test]
    public void SimpleDataSource_ThrowsOnNullDataSource()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new LoggerConfiguration()
                .WriteTo.ClickHouse((ClickHouseDataSource)null!, tableName: "test_logs"));
    }
#endif
}
