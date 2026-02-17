using Serilog.Events;
using Serilog.Sinks.ClickHouse.ColumnWriters;
using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Unit.ColumnWriters;

public class TimestampColumnWriterTests
{
    [Test]
    public void GetValue_ReturnsUtcDateTime_WhenUseUtcIsTrue()
    {
        var timestamp = new DateTimeOffset(2024, 6, 15, 14, 30, 45, TimeSpan.FromHours(5));
        var logEvent = new LogEventBuilder()
            .WithTimestamp(timestamp)
            .Build();

        var writer = new TimestampColumnWriter(useUtc: true);

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo(timestamp.UtcDateTime));
        Assert.That(((DateTime)result!).Kind, Is.EqualTo(DateTimeKind.Utc));
    }

    [Test]
    public void GetValue_ReturnsLocalDateTime_WhenUseUtcIsFalse()
    {
        var timestamp = new DateTimeOffset(2024, 6, 15, 14, 30, 45, TimeSpan.FromHours(5));
        var logEvent = new LogEventBuilder()
            .WithTimestamp(timestamp)
            .Build();

        var writer = new TimestampColumnWriter(useUtc: false);

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo(timestamp.DateTime));
    }

    [Test]
    public void Constructor_UsesDateTime64Type_ByDefault()
    {
        var writer = new TimestampColumnWriter();

        Assert.That(writer.ColumnType, Is.EqualTo("DateTime64(6)"));
        Assert.That(writer.ColumnName, Is.EqualTo("timestamp"));
    }

    [Test]
    public void Constructor_AllowsCustomColumnName()
    {
        var writer = new TimestampColumnWriter(columnName: "event_time");
        Assert.That(writer.ColumnName, Is.EqualTo("event_time"));
    }

    [Test]
    public void Constructor_AllowsCustomColumnType()
    {
        var writer = new TimestampColumnWriter(columnType: "DateTime");
        Assert.That(writer.ColumnType, Is.EqualTo("DateTime"));
    }
}
