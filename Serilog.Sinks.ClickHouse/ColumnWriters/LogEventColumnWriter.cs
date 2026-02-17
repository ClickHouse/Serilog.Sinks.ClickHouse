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

    /// <inheritdoc />
    public LogEventColumnWriter(string columnName = "log_event", string? columnType = null, IFormatProvider? formatProvider = null)
        : base(columnName, columnType ?? "String")
    {
        _formatter = new JsonFormatter(formatProvider: formatProvider);
    }

    /// <inheritdoc />
    public override object? GetValue(LogEvent logEvent, IFormatProvider? formatProvider = null)
    {
        var sb = StringBuilderPool.Get();
        try
        {
            var formatter = formatProvider == null? _formatter : new JsonFormatter(formatProvider: formatProvider);

            using var writer = new StringWriter(sb);
            formatter.Format(logEvent, writer);
            return sb.ToString();
        }
        finally
        {
            StringBuilderPool.Return(sb);
        }
    }
}
