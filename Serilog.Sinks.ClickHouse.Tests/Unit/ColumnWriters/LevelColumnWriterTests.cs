using Serilog.Events;
using Serilog.Sinks.ClickHouse.ColumnWriters;
using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Unit.ColumnWriters;

public class LevelColumnWriterTests
{
    [TestCase(LogEventLevel.Verbose, (byte)0)]
    [TestCase(LogEventLevel.Debug, (byte)1)]
    [TestCase(LogEventLevel.Information, (byte)2)]
    [TestCase(LogEventLevel.Warning, (byte)3)]
    [TestCase(LogEventLevel.Error, (byte)4)]
    [TestCase(LogEventLevel.Fatal, (byte)5)]
    public void GetValue_ReturnsNumericLevel_WhenAsStringIsFalse(LogEventLevel level, byte expectedValue)
    {
        var logEvent = new LogEventBuilder()
            .WithLevel(level)
            .Build();

        var writer = new LevelColumnWriter(asString: false);

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo(expectedValue));
    }

    [TestCase(LogEventLevel.Verbose, "Verbose")]
    [TestCase(LogEventLevel.Debug, "Debug")]
    [TestCase(LogEventLevel.Information, "Information")]
    [TestCase(LogEventLevel.Warning, "Warning")]
    [TestCase(LogEventLevel.Error, "Error")]
    [TestCase(LogEventLevel.Fatal, "Fatal")]
    public void GetValue_ReturnsStringLevel_WhenAsStringIsTrue(LogEventLevel level, string expectedValue)
    {
        var logEvent = new LogEventBuilder()
            .WithLevel(level)
            .Build();

        var writer = new LevelColumnWriter(asString: true);

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo(expectedValue));
    }

    [Test]
    public void Constructor_UsesUInt8Type_WhenAsStringIsFalse()
    {
        var writer = new LevelColumnWriter(asString: false);
        Assert.That(writer.ColumnType, Is.EqualTo("UInt8"));
    }

    [Test]
    public void Constructor_UsesLowCardinalityStringType_WhenAsStringIsTrue()
    {
        var writer = new LevelColumnWriter(asString: true);
        Assert.That(writer.ColumnType, Is.EqualTo("LowCardinality(String)"));
    }

    [Test]
    public void Constructor_DefaultColumnNameIsLevel()
    {
        var writer = new LevelColumnWriter();
        Assert.That(writer.ColumnName, Is.EqualTo("level"));
    }
}
