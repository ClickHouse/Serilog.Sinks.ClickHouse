using Serilog.Sinks.ClickHouse.ColumnWriters;
using Serilog.Sinks.ClickHouse.Schema;

namespace Serilog.Sinks.ClickHouse.Tests.Unit.Schema;

public class SchemaBuilderTests
{
    [Test]
    public void Build_CreatesValidSchema_WithMinimalConfiguration()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn()
            .Build();

        Assert.That(schema.TableName, Is.EqualTo("logs"));
        Assert.That(schema.Columns, Has.Count.EqualTo(1));
        Assert.That(schema.Engine, Is.InstanceOf<DefaultEngine>());
    }

    [Test]
    public void Build_ThrowsException_WhenNoColumns()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new SchemaBuilder()
            .WithTableName("logs")
            .Build());

        Assert.That(ex!.Message, Does.Contain("At least one column"));
    }

    [Test]
    public void Build_SetsDatabase_WhenSpecified()
    {
        var schema = new SchemaBuilder()
            .WithDatabase("mydb")
            .WithTableName("logs")
            .AddTimestampColumn()
            .Build();

        Assert.That(schema.Database, Is.EqualTo("mydb"));
        Assert.That(schema.FullTableName, Is.EqualTo("mydb.logs"));
    }

    [Test]
    public void AddTimestampColumn_AddsTimestampWriter()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn("event_time", precision: 6, useUtc: false)
            .Build();

        Assert.That(schema.Columns, Has.Count.EqualTo(1));
        var column = schema.Columns.First();
        Assert.That(column, Is.InstanceOf<TimestampColumnWriter>());
        Assert.That(column.ColumnName, Is.EqualTo("event_time"));
    }

    [Test]
    public void AddTimestampColumn_ThrowsException_WhenPrecisionIsNegative()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SchemaBuilder()
            .AddTimestampColumn(precision: -1));
    }

    [Test]
    public void AddTimestampColumn_ThrowsException_WhenPrecisionExceedsNine()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SchemaBuilder()
            .AddTimestampColumn(precision: 10));
    }

    [Test]
    public void AddLevelColumn_AsString_UsesLowCardinalityString()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddLevelColumn("severity", asString: true)
            .Build();

        var column = schema.Columns.First();
        Assert.That(column.ColumnType, Is.EqualTo("LowCardinality(String)"));
    }

    [Test]
    public void AddPropertyColumn_AddsTypedColumn()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddPropertyColumn("UserId", "Int64", "user_id")
            .Build();

        var column = schema.Columns.First();
        Assert.That(column, Is.InstanceOf<SinglePropertyColumnWriter>());
        Assert.That(column.ColumnName, Is.EqualTo("user_id"));
        Assert.That(column.ColumnType, Is.EqualTo("Int64"));
    }

    [Test]
    public void WithEngine_ConfiguresCustomEngine()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn()
            .WithEngine(new CustomEngine("ENGINE = MergeTree() ORDER BY (timestamp, level) PARTITION BY toYYYYMM(timestamp) TTL timestamp + INTERVAL 30 DAY"))
            .Build();

        var engine = schema.Engine as CustomEngine;
        Assert.That(engine, Is.Not.Null);
        Assert.That(engine!.ToSql(), Does.Contain("ORDER BY (timestamp, level)"));
        Assert.That(engine.ToSql(), Does.Contain("PARTITION BY toYYYYMM(timestamp)"));
        Assert.That(engine.ToSql(), Does.Contain("TTL timestamp + INTERVAL 30 DAY"));
    }

    [Test]
    public void WithComment_SetsTableComment()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn()
            .WithComment("Application logs table")
            .Build();

        Assert.That(schema.Comment, Is.EqualTo("Application logs table"));
    }

    [Test]
    public void Build_AllowsMultipleColumns()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn()
            .AddLevelColumn()
            .AddMessageColumn()
            .AddExceptionColumn()
            .AddPropertiesColumn()
            .Build();

        Assert.That(schema.Columns, Has.Count.EqualTo(5));
        Assert.That(schema.Columns[0], Is.InstanceOf<TimestampColumnWriter>());
        Assert.That(schema.Columns[1], Is.InstanceOf<LevelColumnWriter>());
        Assert.That(schema.Columns[2], Is.InstanceOf<RenderedMessageColumnWriter>());
        Assert.That(schema.Columns[3], Is.InstanceOf<ExceptionColumnWriter>());
        Assert.That(schema.Columns[4], Is.InstanceOf<PropertiesColumnWriter>());
    }

    [Test]
    public void AddColumn_AllowsCustomColumnWriter()
    {
        var customWriter = new TimestampColumnWriter("custom_time");

        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddColumn(customWriter)
            .Build();

        Assert.That(schema.Columns, Does.Contain(customWriter));
    }

    [Test]
    public void AddPropertyColumn_WithoutType_SetsColumnTypeToNull()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddPropertyColumn("UserId")
            .Build();

        var column = schema.Columns.First();
        Assert.That(column, Is.InstanceOf<SinglePropertyColumnWriter>());
        Assert.That(column.ColumnType, Is.Null);
        Assert.That(column.ColumnName, Is.EqualTo("UserId"));
    }

    [Test]
    public void AddPropertyColumn_WithType_SetsColumnType()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddPropertyColumn("UserId", type: "Int64")
            .Build();

        var column = schema.Columns.First();
        Assert.That(column.ColumnType, Is.EqualTo("Int64"));
    }

    [Test]
    public void AddIndex_AddsSingleIndex()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn()
            .AddIndex("INDEX idx_level level TYPE set(0) GRANULARITY 1")
            .Build();

        Assert.That(schema.Indexes, Has.Count.EqualTo(1));
        Assert.That(schema.Indexes[0], Is.EqualTo("INDEX idx_level level TYPE set(0) GRANULARITY 1"));
    }

    [Test]
    public void AddIndex_AddsMultipleIndexes()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn()
            .AddLevelColumn()
            .AddMessageColumn()
            .AddIndex("INDEX idx_level level TYPE set(0) GRANULARITY 1")
            .AddIndex("INDEX idx_message message TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1")
            .Build();

        Assert.That(schema.Indexes, Has.Count.EqualTo(2));
    }

    [Test]
    public void AddIndex_ThrowsException_WhenDefinitionIsEmpty()
    {
        Assert.Throws<ArgumentException>(() => new SchemaBuilder()
            .AddIndex(""));
    }

    [Test]
    public void AddIndex_ThrowsException_WhenDefinitionIsWhitespace()
    {
        Assert.Throws<ArgumentException>(() => new SchemaBuilder()
            .AddIndex("   "));
    }

    [Test]
    public void OnCluster_SetsClusterName()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .OnCluster("my_cluster")
            .AddTimestampColumn()
            .Build();

        Assert.That(schema.ClusterName, Is.EqualTo("my_cluster"));
    }

    [Test]
    public void OnCluster_ThrowsException_WhenEmpty()
    {
        Assert.Throws<ArgumentException>(() => new SchemaBuilder()
            .OnCluster(""));
    }

    [Test]
    public void OnCluster_ThrowsException_WhenWhitespace()
    {
        Assert.Throws<ArgumentException>(() => new SchemaBuilder()
            .OnCluster("   "));
    }

    [Test]
    public void Build_ClusterNameIsNull_WhenNotSet()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn()
            .Build();

        Assert.That(schema.ClusterName, Is.Null);
    }

    [Test]
    public void AddTimestampColumn_SetsCodec()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn(codec: "DoubleDelta, ZSTD")
            .Build();

        Assert.That(schema.Columns.First().Codec, Is.EqualTo("DoubleDelta, ZSTD"));
    }

    [Test]
    public void AddTimestampColumn_SetsAllColumnOptions()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn(codec: "ZSTD", defaultExpression: "now()", ttl: "timestamp + INTERVAL 30 DAY", comment: "Event time")
            .Build();

        var column = schema.Columns.First();
        Assert.That(column.Codec, Is.EqualTo("ZSTD"));
        Assert.That(column.Default, Is.EqualTo("now()"));
        Assert.That(column.Ttl, Is.EqualTo("timestamp + INTERVAL 30 DAY"));
        Assert.That(column.Comment, Is.EqualTo("Event time"));
    }

    [Test]
    public void AddMessageColumn_SetsCodec()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddMessageColumn(codec: "ZSTD")
            .Build();

        Assert.That(schema.Columns.First().Codec, Is.EqualTo("ZSTD"));
    }

    [Test]
    public void AddPropertyColumn_SetsCodec()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddPropertyColumn("RequestPath", type: "String", codec: "FSST, ZSTD")
            .Build();

        Assert.That(schema.Columns.First().Codec, Is.EqualTo("FSST, ZSTD"));
    }

    [Test]
    public void AddPropertiesColumn_WithColumnType_SetsCodec()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddPropertiesColumn("properties", "JSON(UserId Int64)", codec: "ZSTD")
            .Build();

        var column = schema.Columns.First();
        Assert.That(column.ColumnType, Is.EqualTo("JSON(UserId Int64)"));
        Assert.That(column.Codec, Is.EqualTo("ZSTD"));
    }

    [Test]
    public void AddColumn_PreservesColumnOptions()
    {
        var writer = new TimestampColumnWriter("ts")
        {
            Codec = "ZSTD",
            Default = "now()",
            Ttl = "ts + INTERVAL 7 DAY",
            Comment = "Timestamp",
        };

        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddColumn(writer)
            .Build();

        var column = schema.Columns.First();
        Assert.That(column.Codec, Is.EqualTo("ZSTD"));
        Assert.That(column.Default, Is.EqualTo("now()"));
        Assert.That(column.Ttl, Is.EqualTo("ts + INTERVAL 7 DAY"));
        Assert.That(column.Comment, Is.EqualTo("Timestamp"));
    }
    
    [Test]
    public void AddLogIdColumn_WithCSharpGuid_AddsLogIdWriterWithCorrectDefaults()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddLogIdColumn("log_id", asString: false, generator: LogIdGenerator.CSharpGuid)
            .Build();

        Assert.That(schema.Columns, Has.Count.EqualTo(1));
        var column = schema.Columns.First();
        
        Assert.That(column, Is.InstanceOf<LogIdColumnWriter>());
        Assert.That(column.ColumnName, Is.EqualTo("log_id"));
        Assert.That(column.ColumnType, Is.EqualTo("UUID"));
        Assert.That(column.Default, Is.Null);
    }

    [Test]
    public void AddLogIdColumn_AsString_SetsColumnTypeToString()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddLogIdColumn("log_id", asString: true, generator: LogIdGenerator.CSharpGuid)
            .Build();

        var column = schema.Columns.First();
        Assert.That(column.ColumnType, Is.EqualTo("String"));
    }

    [Test]
    public void AddLogIdColumn_WithClickHouseUUIDv4_SetsDefaultExpression()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddLogIdColumn("log_id", asString: false, generator: LogIdGenerator.ClickHouseUUIDv4)
            .Build();

        var column = schema.Columns.First();
        Assert.That(column.ColumnType, Is.EqualTo("UUID"));
        Assert.That(column.Default, Is.EqualTo("generateUUIDv4()"));
    }

    [Test]
    public void AddLogIdColumn_WithClickHouseUUIDv4AndAsString_SetsStringDefaultExpression()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddLogIdColumn("log_id", asString: true, generator: LogIdGenerator.ClickHouseUUIDv4)
            .Build();

        var column = schema.Columns.First();
        Assert.That(column.ColumnType, Is.EqualTo("String"));
        Assert.That(column.Default, Is.EqualTo("toString(generateUUIDv4())"));
    }

    [Test]
    public void AddLogIdColumn_WithClickHouseUUIDv7_SetsDefaultExpression()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddLogIdColumn("log_id", asString: false, generator: LogIdGenerator.ClickHouseUUIDv7)
            .Build();

        var column = schema.Columns.First();
        Assert.That(column.ColumnType, Is.EqualTo("UUID"));
        Assert.That(column.Default, Is.EqualTo("generateUUIDv7()"));
    }

    [Test]
    public void AddLogIdColumn_SetsOptionalParameters()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddLogIdColumn("log_id", codec: "ZSTD(1)", ttl: "timestamp + INTERVAL 1 DAY", comment: "Unique log id")
            .Build();

        var column = schema.Columns.First();
        Assert.That(column.Codec, Is.EqualTo("ZSTD(1)"));
        Assert.That(column.Ttl, Is.EqualTo("timestamp + INTERVAL 1 DAY"));
        Assert.That(column.Comment, Is.EqualTo("Unique log id"));
    }
}
