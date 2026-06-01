using Serilog.Sinks.ClickHouse.ColumnWriters;
using Serilog.Sinks.ClickHouse.Schema;
using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Unit.ColumnWriters;

[TestFixture]
public class LogIdColumnWriterTests
{
    [Test]
    public void Constructor_DefaultColumnNameIsLogId()
    {
        var writer = new LogIdColumnWriter();
        Assert.That(writer.ColumnName, Is.EqualTo("log_id"));
    }

    [Test]
    public void Constructor_UsesUuidType_WhenAsStringIsFalse()
    {
        var writer = new LogIdColumnWriter(asString: false);
        Assert.That(writer.ColumnType, Is.EqualTo("UUID"));
    }

    [Test]
    public void Constructor_UsesStringType_WhenAsStringIsTrue()
    {
        var writer = new LogIdColumnWriter(asString: true);
        Assert.That(writer.ColumnType, Is.EqualTo("String"));
    }

    [Test]
    public void Constructor_AcceptsCustomColumnType()
    {
        var writer = new LogIdColumnWriter("my_id", asString: false, columnType: "Nullable(UUID)");
        Assert.That(writer.ColumnType, Is.EqualTo("Nullable(UUID)"));
    }

    [Test]
    public void GetValue_ReturnsGuidStructure_WhenAsStringIsFalse()
    {
        var logEvent = new LogEventBuilder().Build();
        var writer = new LogIdColumnWriter(asString: false);

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.InstanceOf<Guid>());
        Assert.That(result, Is.Not.EqualTo(Guid.Empty));
    }

    [Test]
    public void GetValue_ReturnsValidGuidString_WhenAsStringIsTrue()
    {
        var logEvent = new LogEventBuilder().Build();
        var writer = new LogIdColumnWriter(asString: true);

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.InstanceOf<string>());
        Assert.DoesNotThrow(() => Guid.Parse((string)result!));
    }

    [Test]
    public void GetValue_GeneratesUniqueValues_EachTimeCalled()
    {
        var logEvent = new LogEventBuilder().Build();
        var writer = new LogIdColumnWriter(asString: false);

        var result1 = writer.GetValue(logEvent);
        var result2 = writer.GetValue(logEvent);

        Assert.That(result1, Is.Not.EqualTo(result2));
    }
    
    [Test]
    public void AddLogIdColumn_WithCSharpGuid_SetsSkipWriteToFalse()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddLogIdColumn(generator: LogIdGenerator.CSharpGuid)
            .Build();

        var column = schema.Columns.First();
        Assert.That(column.SkipWrite, Is.False);
    }

    [Test]
    [TestCase(LogIdGenerator.ClickHouseUUIDv4)]
    [TestCase(LogIdGenerator.ClickHouseUUIDv7)]
    public void AddLogIdColumn_WithDbGenerator_SetsSkipWriteToTrue(LogIdGenerator generator)
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddLogIdColumn(generator: generator)
            .Build();

        var column = schema.Columns.First();
        Assert.That(column.SkipWrite, Is.True);
    }
}