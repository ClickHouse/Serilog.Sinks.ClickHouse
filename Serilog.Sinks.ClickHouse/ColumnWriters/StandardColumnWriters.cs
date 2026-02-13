using Serilog.Events;

namespace Serilog.Sinks.ClickHouse.ColumnWriters;

/// <summary>
/// Writes the timestamp from the log event.
/// Default type: DateTime64(3).
/// </summary>
public class TimestampColumnWriter : ColumnWriterBase
{
    private readonly bool _useUtc;

    public TimestampColumnWriter(string columnName = "timestamp", string? columnType = null, bool useUtc = true)
        : base(columnName, columnType ?? "DateTime64(3)")
    {
        _useUtc = useUtc;
    }

    public override object? GetValue(LogEvent logEvent, IFormatProvider? formatProvider = null)
    {
        return _useUtc ? logEvent.Timestamp.UtcDateTime : logEvent.Timestamp.DateTime;
    }
}

/// <summary>
/// Writes the log level.
/// If asString is true (default), uses LowCardinality(String). Otherwise uses UInt8.
/// If columnType is passed, it overrides the above.
/// </summary>
public class LevelColumnWriter : ColumnWriterBase
{
    private readonly bool _asString;

    public LevelColumnWriter(string columnName = "level", bool asString = true, string? columnType = null)
        : base(columnName, columnType ?? (asString ? "LowCardinality(String)" : "UInt8"))
    {
        _asString = asString;
    }

    public override object? GetValue(LogEvent logEvent, IFormatProvider? formatProvider = null)
    {
        return _asString ? logEvent.Level.ToString() : (byte)logEvent.Level;
    }
}

/// <summary>
/// Writes the rendered message (with property values substituted).
/// Default type: String.
/// </summary>
public class RenderedMessageColumnWriter : ColumnWriterBase
{
    public RenderedMessageColumnWriter(string columnName = "message", string? columnType = null)
        : base(columnName, columnType ?? "String")
    {
    }

    public override object? GetValue(LogEvent logEvent, IFormatProvider? formatProvider = null)
    {
        return logEvent.RenderMessage(formatProvider);
    }
}

/// <summary>
/// Writes the raw message template (without property substitution).
/// Default type: String.
/// </summary>
public class MessageTemplateColumnWriter : ColumnWriterBase
{
    public MessageTemplateColumnWriter(string columnName = "message_template", string? columnType = null)
        : base(columnName, columnType ?? "String")
    {
    }

    public override object? GetValue(LogEvent logEvent, IFormatProvider? formatProvider = null)
    {
        return logEvent.MessageTemplate.Text;
    }
}

/// <summary>
/// Writes the exception details (ToString()) or null if no exception.
/// Default type: Nullable(String)
/// </summary>
public class ExceptionColumnWriter : ColumnWriterBase
{
    public ExceptionColumnWriter(string columnName = "exception", string? columnType = null)
        : base(columnName, columnType ?? "Nullable(String)")
    {
    }

    public override object? GetValue(LogEvent logEvent, IFormatProvider? formatProvider = null)
    {
        return logEvent.Exception?.ToString();
    }
}
