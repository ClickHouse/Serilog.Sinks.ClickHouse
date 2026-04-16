using ClickHouse.Driver;

namespace Serilog.Sinks.ClickHouse.Tests.Integration.Cluster;

/// <summary>
/// Shared setup fixture for cluster integration tests.
/// Expects a 2-node ClickHouse cluster started via docker-compose.cluster.yml
/// with node1 on port 9000 and node2 on port 9001.
/// </summary>
[SetUpFixture]
[Category("Cluster")]
public class ClusterTestFixture
{
    /// <summary>
    /// Connection string for node 1 of the test cluster.
    /// </summary>
    public static string Node1ConnectionString => "Host=localhost;Port=9000";

    /// <summary>
    /// Connection string for node 2 of the test cluster.
    /// </summary>
    public static string Node2ConnectionString => "Host=localhost;Port=9001";

    /// <summary>
    /// The cluster name matching the docker-compose configuration.
    /// </summary>
    public const string ClusterName = "test_cluster";

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        // Verify both nodes are reachable
        using var client1 = new ClickHouseClient(Node1ConnectionString);
        var result = await client1.ExecuteScalarAsync("SELECT 1");
        Assert.That(Convert.ToInt32(result), Is.EqualTo(1), "Node 1 is not reachable");

        using var client2 = new ClickHouseClient(Node2ConnectionString);
        result = await client2.ExecuteScalarAsync("SELECT 1");
        Assert.That(Convert.ToInt32(result), Is.EqualTo(1), "Node 2 is not reachable");
    }
}
