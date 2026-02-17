using System.Text.Json;
using Serilog.Sinks.ClickHouse.ColumnWriters;
using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Unit.ColumnWriters;

public class PropertiesColumnWriterTests
{
    [Test]
    public void GetValue_ReturnsEmptyJsonObject_WhenNoProperties()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("No properties")
            .Build();

        var writer = new PropertiesColumnWriter();

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo("{}"));
    }

    [Test]
    public void GetValue_ReturnsSingleProperty_AsJson()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .WithProperty("UserId", 123)
            .Build();

        var writer = new PropertiesColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        Assert.That(result, Does.Contain("\"UserId\""));
        Assert.That(result, Does.Contain("123"));

        // Verify it's valid JSON
        var parsed = JsonDocument.Parse(result);
        Assert.That(parsed.RootElement.GetProperty("UserId").GetInt32(), Is.EqualTo(123));
    }

    [Test]
    public void GetValue_ReturnsMultipleProperties_AsJson()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .WithProperty("UserId", 123)
            .WithProperty("RequestPath", "/api/users")
            .WithProperty("Duration", 45.5)
            .Build();

        var writer = new PropertiesColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        var parsed = JsonDocument.Parse(result);
        Assert.That(parsed.RootElement.GetProperty("UserId").GetInt32(), Is.EqualTo(123));
        Assert.That(parsed.RootElement.GetProperty("RequestPath").GetString(), Is.EqualTo("/api/users"));
        Assert.That(parsed.RootElement.GetProperty("Duration").GetDouble(), Is.EqualTo(45.5));
    }

    [Test]
    public void GetValue_HandlesStringProperty_WithQuotes()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .WithProperty("Query", "SELECT * FROM \"users\"")
            .Build();

        var writer = new PropertiesColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        // Verify it's valid JSON (quotes should be escaped)
        var parsed = JsonDocument.Parse(result);
        Assert.That(parsed.RootElement.GetProperty("Query").GetString(), Is.EqualTo("SELECT * FROM \"users\""));
    }

    [Test]
    public void GetValue_HandlesNullProperty()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .WithProperty("NullValue", null)
            .Build();

        var writer = new PropertiesColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        var parsed = JsonDocument.Parse(result);
        Assert.That(parsed.RootElement.GetProperty("NullValue").ValueKind, Is.EqualTo(JsonValueKind.Null));
    }

    [Test]
    public void GetValue_HandlesBooleanProperty()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .WithProperty("IsEnabled", true)
            .Build();

        var writer = new PropertiesColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        var parsed = JsonDocument.Parse(result);
        Assert.That(parsed.RootElement.GetProperty("IsEnabled").GetBoolean(), Is.True);
    }

    [Test]
    public void GetValue_HandlesPropertyNameWithSpecialCharacters()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .WithProperty("has\nnewline", "value")
            .Build();

        var writer = new PropertiesColumnWriter();

        var result = (string)writer.GetValue(logEvent)!;

        // Should be valid JSON with escaped newline in property name
        Assert.DoesNotThrow(() => JsonDocument.Parse(result));
    }

    [Test]
    public void Constructor_DefaultColumnNameIsProperties()
    {
        var writer = new PropertiesColumnWriter();
        Assert.That(writer.ColumnName, Is.EqualTo("properties"));
    }
}
