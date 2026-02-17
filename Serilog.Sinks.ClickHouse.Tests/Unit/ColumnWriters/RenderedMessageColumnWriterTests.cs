using System.Globalization;
using Serilog.Events;
using Serilog.Sinks.ClickHouse.ColumnWriters;
using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Unit.ColumnWriters;

public class RenderedMessageColumnWriterTests
{
    [Test]
    public void GetValue_ReturnsRenderedMessage_WithPropertySubstitution()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("User {UserId} logged in from {IpAddress}")
            .WithProperty("UserId", 123)
            .WithProperty("IpAddress", "192.168.1.1")
            .Build();

        var writer = new RenderedMessageColumnWriter();

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo("User 123 logged in from \"192.168.1.1\""));
    }

    [Test]
    public void GetValue_ReturnsPlainMessage_WhenNoProperties()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Simple message without properties")
            .Build();

        var writer = new RenderedMessageColumnWriter();

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo("Simple message without properties"));
    }

    [Test]
    public void GetValue_UsesFormatProvider_WhenProvided()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Value is {Amount}")
            .WithProperty("Amount", 1234.56)
            .Build();

        var frenchCulture = new CultureInfo("fr-FR");
        var writer = new RenderedMessageColumnWriter();

        var result = writer.GetValue(logEvent, frenchCulture);

        // French format uses comma as decimal separator
        Assert.That(result, Is.EqualTo("Value is 1234,56"));
    }

    [Test]
    public void Constructor_DefaultColumnNameIsMessage()
    {
        var writer = new RenderedMessageColumnWriter();
        Assert.That(writer.ColumnName, Is.EqualTo("message"));
    }
}
