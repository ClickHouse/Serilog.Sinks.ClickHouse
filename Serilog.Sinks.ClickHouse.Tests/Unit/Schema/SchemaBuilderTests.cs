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
}
