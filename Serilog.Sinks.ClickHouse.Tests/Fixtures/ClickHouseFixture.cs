using Testcontainers.ClickHouse;

namespace Serilog.Sinks.ClickHouse.Tests.Fixtures;

/// <summary>
/// Fixture that provides a ClickHouse database running in a Docker container
/// for integration testing.
/// </summary>
public class ClickHouseFixture
{
    private readonly ClickHouseContainer _container;

    public ClickHouseFixture()
    {
        _container = new ClickHouseBuilder("clickhouse/clickhouse-server:latest")
            .Build();
    }

    public string ConnectionString => _container.GetConnectionString();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}
