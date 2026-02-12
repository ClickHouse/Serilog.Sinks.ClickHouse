using Serilog.Events;
using Serilog.Formatting.Json;

namespace Serilog.Sinks.ClickHouse.ColumnWriters;

/// <summary>
/// Writes the entire log event as a JSON object.
/// Useful for debugging or when you need full event fidelity.
/// </summary>
public class LogEventColumnWriter : ColumnWriterBase
{
    private readonly JsonFormatter _formatter;

    public LogEventColumnWriter(string columnName = "log_event", string? columnType = null, IFormatProvider? formatProvider = null)
        : base(columnName, columnType ?? "String")
    {
        _formatter = new JsonFormatter(formatProvider: formatProvider);
    }

    public override object? GetValue(LogEvent logEvent, IFormatProvider? formatProvider = null)
    {
        var sb = StringBuilderPool.Get();
        try
        {
            using var writer = new StringWriter(sb);
            _formatter.Format(logEvent, writer);
            return sb.ToString();
        }
        finally
        {
            StringBuilderPool.Return(sb);
        }
    }
}
