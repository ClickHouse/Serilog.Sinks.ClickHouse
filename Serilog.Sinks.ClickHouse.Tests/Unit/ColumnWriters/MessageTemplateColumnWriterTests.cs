using Serilog.Sinks.ClickHouse.ColumnWriters;
using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Unit.ColumnWriters;

public class MessageTemplateColumnWriterTests
{
    [Test]
    public void GetValue_ReturnsRawTemplate_WithoutPropertySubstitution()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("User {UserId} logged in from {IpAddress}")
            .WithProperty("UserId", 123)
            .WithProperty("IpAddress", "192.168.1.1")
            .Build();

        var writer = new MessageTemplateColumnWriter();

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo("User {UserId} logged in from {IpAddress}"));
    }

    [Test]
    public void GetValue_ReturnsPlainMessage_WhenNoPlaceholders()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Simple message without placeholders")
            .Build();

        var writer = new MessageTemplateColumnWriter();

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo("Simple message without placeholders"));
    }

    [Test]
    public void Constructor_DefaultColumnNameIsMessageTemplate()
    {
        var writer = new MessageTemplateColumnWriter();
        Assert.That(writer.ColumnName, Is.EqualTo("message_template"));
    }
}
