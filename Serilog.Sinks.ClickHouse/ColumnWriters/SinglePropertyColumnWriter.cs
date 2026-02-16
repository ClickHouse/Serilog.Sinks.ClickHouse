using System.Text;
using ClickHouse.Driver.Constraints;
using Serilog.Events;
using Serilog.Formatting.Json;

namespace Serilog.Sinks.ClickHouse.ColumnWriters;

/// <summary>
/// Specifies how a property value should be written.
/// </summary>
public enum PropertyWriteMethod
{
    /// <summary>
    /// Extract the raw CLR value from ScalarValue.
    /// </summary>
    Raw,

    /// <summary>
    /// Use ToString() with optional format string.
    /// </summary>
    ToString,

    /// <summary>
    /// Serialize as JSON.
    /// </summary>
    Json,
}

/// <summary>
/// Writes a single property from the log event.
/// Useful for extracting specific enriched properties into typed columns.
/// </summary>
public class SinglePropertyColumnWriter : ColumnWriterBase
{
    private static readonly JsonValueFormatter JsonFormatter = new();

    /// <summary>
    /// The name of the property in LogEvent.Properties to extract.
    /// </summary>
    public string PropertyName { get; }

    /// <summary>
    /// How to write the property value.
    /// </summary>
    public PropertyWriteMethod WriteMethod { get; }

    /// <summary>
    /// Optional format string for ToString method.
    /// </summary>
    public string? Format { get; }

    /// <inheritdoc />
    public SinglePropertyColumnWriter(
        string propertyName,
        string? columnName = null,
        string? columnType = null,
        PropertyWriteMethod writeMethod = PropertyWriteMethod.Raw,
        string? format = null)
        : base(columnName ?? propertyName, columnType)
    {
        PropertyName = propertyName ?? throw new ArgumentNullException(nameof(propertyName));
        WriteMethod = writeMethod;
        Format = format;
    }

    /// <inheritdoc />
    public override object? GetValue(LogEvent logEvent, IFormatProvider? formatProvider = null)
    {
        if (!logEvent.Properties.TryGetValue(PropertyName, out var propertyValue))
        {
            return DBDefault.Value;
        }

        return WriteMethod switch
        {
            PropertyWriteMethod.Raw => ExtractRawValue(propertyValue),
            PropertyWriteMethod.Json => FormatAsJson(propertyValue),
            PropertyWriteMethod.ToString => propertyValue.ToString(Format, formatProvider),
            _ => propertyValue.ToString(Format, formatProvider),
        };
    }

    private static object? ExtractRawValue(LogEventPropertyValue propertyValue)
    {
        if (propertyValue is ScalarValue scalarValue)
        {
            return scalarValue.Value;
        }

        // For non-scalar values (sequences, structures), fall back to ToString
        return propertyValue.ToString();
    }

    private static string FormatAsJson(LogEventPropertyValue propertyValue)
    {
        var sb = new StringBuilder();
        using var writer = new StringWriter(sb);
        JsonFormatter.Format(propertyValue, writer);
        return sb.ToString();
    }
}
