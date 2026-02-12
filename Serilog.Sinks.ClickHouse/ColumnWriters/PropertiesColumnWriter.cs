using Serilog.Events;
using Serilog.Formatting.Json;

namespace Serilog.Sinks.ClickHouse.ColumnWriters;

/// <summary>
/// Writes ALL log event properties as a JSON object.
/// This is useful for capturing enriched properties without
/// needing to define each one upfront.
///
/// Example output: {"UserId": 123, "RequestPath": "/api/users", "CorrelationId": "abc-123"}
///
/// Uses ClickHouse's native JSON type for efficient querying with dot notation:
/// SELECT * FROM logs WHERE properties.UserId = 123
/// </summary>
public class PropertiesColumnWriter : ColumnWriterBase
{
    private static readonly JsonValueFormatter ValueFormatter = new();

    public PropertiesColumnWriter(string columnName = "properties", string? columnType = null)
        : base(columnName, columnType ?? "JSON")
    {
    }

    public override object? GetValue(LogEvent logEvent, IFormatProvider? formatProvider = null)
    {
        if (logEvent.Properties.Count == 0)
            return "{}";

        var sb = StringBuilderPool.Get();
        try
        {
            using var writer = new StringWriter(sb);
            writer.Write('{');

            var first = true;
            foreach (var property in logEvent.Properties)
            {
                if (!first)
                    writer.Write(',');
                first = false;

                JsonValueFormatter.WriteQuotedJsonString(property.Key, writer);
                writer.Write(':');
                ValueFormatter.Format(property.Value, writer);
            }

            writer.Write('}');
            return sb.ToString();
        }
        finally
        {
            StringBuilderPool.Return(sb);
        }
    }
}
