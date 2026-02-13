using System.Text.Json;
using Serilog.Events;
using Serilog.Sinks.ClickHouse.ColumnWriters;
using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Unit.ColumnWriters;

public class LogEventColumnWriterTests
{
    [Test]
    public void GetValue_ReturnsValidJson()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Test message")
            .WithLevel(LogEventLevel.Information)
            .Build();

        var writer = new LogEventColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        Assert.DoesNotThrow(() => JsonDocument.Parse(result));
    }

    [Test]
    public void GetValue_IncludesTimestamp()
    {
        var timestamp = new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.Zero);
        var logEvent = new LogEventBuilder()
            .WithTimestamp(timestamp)
            .WithMessage("Test")
            .Build();

        var writer = new LogEventColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        Assert.That(result, Does.Contain("2024-01-15"));
    }

    [Test]
    public void GetValue_IncludesLevel()
    {
        var logEvent = new LogEventBuilder()
            .WithLevel(LogEventLevel.Warning)
            .WithMessage("Test")
            .Build();

        var writer = new LogEventColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        Assert.That(result, Does.Contain("Warning"));
    }

    [Test]
    public void GetValue_IncludesMessageTemplate()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("User {UserId} logged in")
            .WithProperty("UserId", 123)
            .Build();

        var writer = new LogEventColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        Assert.That(result, Does.Contain("User {UserId} logged in"));
    }

    [Test]
    public void GetValue_IncludesProperties()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .WithProperty("UserId", 123)
            .WithProperty("Action", "Login")
            .Build();

        var writer = new LogEventColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        Assert.That(result, Does.Contain("UserId"));
        Assert.That(result, Does.Contain("123"));
        Assert.That(result, Does.Contain("Action"));
        Assert.That(result, Does.Contain("Login"));
    }

    [Test]
    public void GetValue_IncludesException_WhenPresent()
    {
        var exception = new InvalidOperationException("Something went wrong");
        var logEvent = new LogEventBuilder()
            .WithMessage("Error occurred")
            .WithException(exception)
            .Build();

        var writer = new LogEventColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        Assert.That(result, Does.Contain("InvalidOperationException"));
        Assert.That(result, Does.Contain("Something went wrong"));
    }

    [Test]
    public void GetValue_HandlesEventWithoutException()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Normal message")
            .Build();

        var writer = new LogEventColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        Assert.That(result, Is.Not.Null.And.Not.Empty);
        Assert.DoesNotThrow(() => JsonDocument.Parse(result));
    }

    [Test]
    public void Constructor_DefaultColumnNameIsLogEvent()
    {
        var writer = new LogEventColumnWriter();
        Assert.That(writer.ColumnName, Is.EqualTo("log_event"));
    }

    [Test]
    public void Constructor_DefaultColumnTypeIsString()
    {
        var writer = new LogEventColumnWriter();
        Assert.That(writer.ColumnType, Is.EqualTo("String"));
    }

    [Test]
    public void Constructor_AcceptsCustomColumnName()
    {
        var writer = new LogEventColumnWriter("custom_event");
        Assert.That(writer.ColumnName, Is.EqualTo("custom_event"));
    }

    [Test]
    public void Constructor_AcceptsCustomColumnType()
    {
        var writer = new LogEventColumnWriter("event", "Nullable(String)");
        Assert.That(writer.ColumnType, Is.EqualTo("Nullable(String)"));
    }

    [Test]
    public void GetValue_ProducesConsistentOutput_ForSameEvent()
    {
        var timestamp = new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.Zero);
        var logEvent = new LogEventBuilder()
            .WithTimestamp(timestamp)
            .WithLevel(LogEventLevel.Information)
            .WithMessage("Consistent test")
            .WithProperty("Key", "Value")
            .Build();

        var writer = new LogEventColumnWriter();

        var result1 = (string)writer.GetValue(logEvent)!;
        var result2 = (string)writer.GetValue(logEvent)!;

        Assert.That(result1, Is.EqualTo(result2));
    }

    [Test]
    public void GetValue_HandlesSpecialCharactersInMessage()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Message with \"quotes\" and \\ backslash")
            .Build();

        var writer = new LogEventColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        Assert.DoesNotThrow(() => JsonDocument.Parse(result));
    }

    [Test]
    public void GetValue_HandlesUnicodeCharacters()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Unicode: 日本語 emoji 🎉")
            .Build();

        var writer = new LogEventColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        Assert.DoesNotThrow(() => JsonDocument.Parse(result));
    }
}
