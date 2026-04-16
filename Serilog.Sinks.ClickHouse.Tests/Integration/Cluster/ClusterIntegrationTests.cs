using ClickHouse.Driver;
using Serilog.Sinks.ClickHouse.Client;
using Serilog.Sinks.ClickHouse.Configuration;
using Serilog.Sinks.ClickHouse.Schema;
using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Integration.Cluster;

[TestFixture]
[Category("Cluster")]
public class ClusterIntegrationTests
{
    private string Node1 => ClusterTestFixture.Node1ConnectionString;
    private string Node2 => ClusterTestFixture.Node2ConnectionString;

    private static string UniqueTable(string prefix = "cluster") => $"{prefix}_{Guid.NewGuid():N}";

    [Test]
    public async Task CreateTable_OnCluster_CreatesTableOnBothNodes()
    {
        var table = UniqueTable("on_cluster");

        var schema = new SchemaBuilder()
            .WithTableName(table)
            .OnCluster(ClusterTestFixture.ClusterName)
            .AddTimestampColumn()
            .AddLevelColumn()
            .AddMessageColumn()
            .WithEngine(new CustomEngine(
                $"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{table}', '{{replica}}')\nORDER BY (timestamp)"))
            .Build();

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = Node1,
            Schema = schema,
            TableCreation = new TableCreationOptions { Mode = TableCreationMode.CreateIfNotExists },
        };

        try
        {
            using var sink = new ClickHouseSink(options);
            await sink.EmitBatchAsync(new[] { new LogEventBuilder().WithMessage("Cluster test").Build() });

            // Verify the table exists on node 1
            using var client1 = new ClickHouseClient(Node1);
            var exists1 = await client1.ExecuteScalarAsync($"EXISTS {table}");
            Assert.That(exists1 is (byte)1, Is.True, "Table should exist on node 1");

            // Verify the table exists on node 2 (created via ON CLUSTER)
            using var client2 = new ClickHouseClient(Node2);
            var exists2 = await client2.ExecuteScalarAsync($"EXISTS {table}");
            Assert.That(exists2 is (byte)1, Is.True, "Table should exist on node 2 via ON CLUSTER DDL");
        }
        finally
        {
            // Cleanup on both nodes
            using var cleanup = new ClickHouseClient(Node1);
            await cleanup.ExecuteNonQueryAsync(
                $"DROP TABLE IF EXISTS {table} ON CLUSTER {ClusterTestFixture.ClusterName}");
        }
    }

    [Test]
    public async Task CreateTable_OnCluster_DataReplicatedAcrossNodes()
    {
        var table = UniqueTable("replicated");

        var schema = new SchemaBuilder()
            .WithTableName(table)
            .OnCluster(ClusterTestFixture.ClusterName)
            .AddTimestampColumn()
            .AddMessageColumn()
            .WithEngine(new CustomEngine(
                $"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{table}', '{{replica}}')\nORDER BY (timestamp)"))
            .Build();

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = Node1,
            Schema = schema,
            TableCreation = new TableCreationOptions { Mode = TableCreationMode.CreateIfNotExists },
        };

        try
        {
            using var sink = new ClickHouseSink(options);
            await sink.EmitBatchAsync(new[]
            {
                new LogEventBuilder().WithMessage("Replicated event 1").Build(),
                new LogEventBuilder().WithMessage("Replicated event 2").Build(),
            });

            // Verify data on node 1
            using var client1 = new ClickHouseClient(Node1);
            var count1 = await client1.ExecuteScalarAsync($"SELECT count() FROM {table}");
            Assert.That(Convert.ToInt64(count1), Is.EqualTo(2), "Node 1 should have 2 rows");

            // Wait briefly for replication, then verify data on node 2
            await Task.Delay(TimeSpan.FromSeconds(2));

            using var client2 = new ClickHouseClient(Node2);
            var count2 = await client2.ExecuteScalarAsync($"SELECT count() FROM {table}");
            Assert.That(Convert.ToInt64(count2), Is.EqualTo(2), "Node 2 should have 2 replicated rows");
        }
        finally
        {
            using var cleanup = new ClickHouseClient(Node1);
            await cleanup.ExecuteNonQueryAsync(
                $"DROP TABLE IF EXISTS {table} ON CLUSTER {ClusterTestFixture.ClusterName}");
        }
    }

    [Test]
    public async Task CreateTable_OnCluster_WithColumnCodec_CreatesCorrectSchema()
    {
        var table = UniqueTable("codec_cluster");

        var schema = new SchemaBuilder()
            .WithTableName(table)
            .OnCluster(ClusterTestFixture.ClusterName)
            .AddTimestampColumn(codec: "DoubleDelta, ZSTD")
            .AddLevelColumn(codec: "ZSTD")
            .AddMessageColumn(codec: "ZSTD")
            .WithEngine(new CustomEngine(
                $"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{table}', '{{replica}}')\nORDER BY (timestamp)"))
            .Build();

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = Node1,
            Schema = schema,
            TableCreation = new TableCreationOptions { Mode = TableCreationMode.CreateIfNotExists },
        };

        try
        {
            using var sink = new ClickHouseSink(options);
            await sink.EmitBatchAsync(new[] { new LogEventBuilder().WithMessage("Codec test").Build() });

            // Verify the CREATE TABLE statement includes codecs
            using var client = new ClickHouseClient(Node1);
            var reader = await client.ExecuteReaderAsync(
                $"SELECT create_table_query FROM system.tables WHERE name = '{table}' AND database = currentDatabase()");

            Assert.That(reader.Read(), Is.True);
            var createQuery = reader.GetString(0);
            Assert.That(createQuery, Does.Contain("CODEC(DoubleDelta, ZSTD"));
            Assert.That(createQuery, Does.Contain("CODEC(ZSTD"));
        }
        finally
        {
            using var cleanup = new ClickHouseClient(Node1);
            await cleanup.ExecuteNonQueryAsync(
                $"DROP TABLE IF EXISTS {table} ON CLUSTER {ClusterTestFixture.ClusterName}");
        }
    }

    [Test]
    public async Task DropAndRecreate_OnCluster_WorksOnBothNodes()
    {
        var table = UniqueTable("drop_cluster");

        var schema = new SchemaBuilder()
            .WithTableName(table)
            .OnCluster(ClusterTestFixture.ClusterName)
            .AddTimestampColumn()
            .AddMessageColumn()
            .WithEngine(new CustomEngine(
                $"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{table}', '{{replica}}')\nORDER BY (timestamp)"))
            .Build();

        try
        {
            // First, create the table via ON CLUSTER
            using var preClient = new ClickHouseClient(Node1);
            await preClient.ExecuteNonQueryAsync(SqlGenerator.GenerateCreateTable(schema));

            // Now use DropAndRecreate — the drop should also be ON CLUSTER
            var options = new ClickHouseSinkOptions
            {
                ConnectionString = Node1,
                Schema = schema,
                TableCreation = new TableCreationOptions { Mode = TableCreationMode.DropAndRecreate },
            };

            using var sink = new ClickHouseSink(options);
            await sink.EmitBatchAsync(new[] { new LogEventBuilder().WithMessage("After drop recreate").Build() });

            // Both nodes should still have the table
            using var client1 = new ClickHouseClient(Node1);
            var exists1 = await client1.ExecuteScalarAsync($"EXISTS {table}");
            Assert.That(exists1 is (byte)1, Is.True, "Table should exist on node 1 after drop/recreate");

            using var client2 = new ClickHouseClient(Node2);
            var exists2 = await client2.ExecuteScalarAsync($"EXISTS {table}");
            Assert.That(exists2 is (byte)1, Is.True, "Table should exist on node 2 after drop/recreate");
        }
        finally
        {
            using var cleanup = new ClickHouseClient(Node1);
            await cleanup.ExecuteNonQueryAsync(
                $"DROP TABLE IF EXISTS {table} ON CLUSTER {ClusterTestFixture.ClusterName}");
        }
    }
}
