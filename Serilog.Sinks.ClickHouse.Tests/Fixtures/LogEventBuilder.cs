using Serilog.Events;
using Serilog.Parsing;

namespace Serilog.Sinks.ClickHouse.Tests.Fixtures;

/// <summary>
/// Fluent builder for creating test LogEvent instances.
/// </summary>
public class LogEventBuilder
{
    private DateTimeOffset _timestamp = DateTimeOffset.UtcNow;
    private LogEventLevel _level = LogEventLevel.Information;
    private string _messageTemplate = "Test message";
    private readonly Dictionary<string, LogEventPropertyValue> _properties = new();
    private Exception? _exception;

    public LogEventBuilder WithTimestamp(DateTimeOffset timestamp)
    {
        _timestamp = timestamp;
        return this;
    }

    public LogEventBuilder WithLevel(LogEventLevel level)
    {
        _level = level;
        return this;
    }

    public LogEventBuilder WithMessage(string messageTemplate)
    {
        _messageTemplate = messageTemplate;
        return this;
    }

    public LogEventBuilder WithProperty(string name, object? value)
    {
        _properties[name] = CreatePropertyValue(value);
        return this;
    }

    public LogEventBuilder WithException(Exception exception)
    {
        _exception = exception;
        return this;
    }

    public LogEvent Build()
    {
        var parser = new MessageTemplateParser();
        var template = parser.Parse(_messageTemplate);

        var properties = _properties.Select(kvp => new LogEventProperty(kvp.Key, kvp.Value));

        return new LogEvent(_timestamp, _level, _exception, template, properties);
    }

    private static LogEventPropertyValue CreatePropertyValue(object? value)
    {
        return value switch
        {
            null => new ScalarValue(null),
            string s => new ScalarValue(s),
            int i => new ScalarValue(i),
            long l => new ScalarValue(l),
            double d => new ScalarValue(d),
            bool b => new ScalarValue(b),
            DateTime dt => new ScalarValue(dt),
            Guid g => new ScalarValue(g),
            _ => new ScalarValue(value.ToString()),
        };
    }
}
