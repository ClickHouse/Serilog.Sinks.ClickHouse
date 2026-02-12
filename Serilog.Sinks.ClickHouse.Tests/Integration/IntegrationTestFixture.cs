using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Integration;

/// <summary>
/// Shared setup fixture that starts a single ClickHouse container
/// for all integration tests in this namespace.
/// </summary>
[SetUpFixture]
public class IntegrationTestFixture
{
    private static ClickHouseFixture? _fixture;

    public static string ConnectionString => _fixture?.ConnectionString
        ?? throw new InvalidOperationException("Integration test fixture not initialized.");

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClickHouseFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_fixture != null)
            await _fixture.DisposeAsync();
    }
}
