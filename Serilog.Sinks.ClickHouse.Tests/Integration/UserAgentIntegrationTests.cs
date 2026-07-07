using ClickHouse.Driver;
using Serilog.Sinks.ClickHouse.Configuration;
using Serilog.Sinks.ClickHouse.Schema;
using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Integration;

/// <summary>
/// Verifies end-to-end that the sink tags its ClickHouse HTTP requests with the
/// <c>lib</c> User-Agent token, by reading it back from <c>system.query_log</c>.
/// </summary>
[TestFixture]
[Category("Integration")]
public class UserAgentIntegrationTests
{
    private string ConnectionString => IntegrationTestFixture.ConnectionString;

    [Test]
    public async Task SinkQueries_AreTaggedWithLibInUserAgent()
    {
        var table = $"user_agent_{Guid.NewGuid():N}";
        var options = new ClickHouseSinkOptions
        {
            ConnectionString = ConnectionString,
            Schema = DefaultSchema.Create(table).Build(),
        };

        using (var sink = new ClickHouseSink(options))
        {
            await sink.EmitBatchAsync([new LogEventBuilder().WithMessage("user-agent probe").Build()]);
        }

        // Use a separate, untagged client for verification so the lib tag we assert on
        // can only have come from the sink's own client.
        using var client = new ClickHouseClient(ConnectionString);
        await client.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");

        var taggedQueryCount = await client.ExecuteScalarAsync(
            "SELECT count() FROM system.query_log " +
            $"WHERE query LIKE '%{table}%' " +
            "AND http_user_agent LIKE '%lib:Serilog.Sinks.ClickHouse%' " +
            "AND event_time > now() - INTERVAL 5 MINUTE");

        Assert.That(Convert.ToInt64(taggedQueryCount), Is.GreaterThan(0),
            "Expected the sink's queries to carry lib:Serilog.Sinks.ClickHouse in the HTTP User-Agent.");
    }
}
