using Microsoft.Extensions.Configuration;
using Serilog;

namespace Serilog.Sinks.ClickHouse.Tests.Unit;

/// <summary>
/// Verifies that Serilog.Settings.Configuration can discover and configure
/// the ClickHouse sink from an IConfiguration (e.g. appsettings.json).
/// </summary>
public class ConfigurationBindingTests
{
    [Test]
    public void ReadFromConfiguration_BasicBinding_CreatesLogger()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Serilog:Using:0"] = "Serilog.Sinks.ClickHouse",
                ["Serilog:WriteTo:0:Name"] = "ClickHouse",
                ["Serilog:WriteTo:0:Args:connectionString"] = "Host=localhost;Port=9000",
                ["Serilog:WriteTo:0:Args:tableName"] = "test_logs",
            })
            .Build();

        using var logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .CreateLogger();

        Assert.That(logger, Is.Not.Null);
    }

    [Test]
    public void ReadFromConfiguration_WithOptionalParams_BindsCorrectly()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Serilog:Using:0"] = "Serilog.Sinks.ClickHouse",
                ["Serilog:WriteTo:0:Name"] = "ClickHouse",
                ["Serilog:WriteTo:0:Args:connectionString"] = "Host=localhost;Port=9000",
                ["Serilog:WriteTo:0:Args:tableName"] = "test_logs",
                ["Serilog:WriteTo:0:Args:database"] = "my_database",
                ["Serilog:WriteTo:0:Args:batchSizeLimit"] = "500",
                ["Serilog:WriteTo:0:Args:minimumLevel"] = "Warning",
            })
            .Build();

        using var logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .CreateLogger();

        Assert.That(logger, Is.Not.Null);
    }
}
