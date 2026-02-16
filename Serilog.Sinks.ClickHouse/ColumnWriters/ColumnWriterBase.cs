using System.Text;
using Microsoft.Extensions.ObjectPool;
using Serilog.Events;

namespace Serilog.Sinks.ClickHouse.ColumnWriters;

/// <summary>
/// Base class for all column writers. Each writer knows how to extract
/// a specific value from a LogEvent for a particular column.
/// </summary>
public abstract class ColumnWriterBase
{
    /// <summary>
    /// Shared StringBuilder pool for column writers that build strings.
    /// </summary>
    protected static readonly ObjectPool<StringBuilder> StringBuilderPool =
        new DefaultObjectPoolProvider().CreateStringBuilderPool(
            initialCapacity: 256,
            maximumRetainedCapacity: 4096);

    /// <summary>
    /// The name of the column in the ClickHouse table.
    /// </summary>
    public string ColumnName { get; }

    /// <summary>
    /// The ClickHouse data type for this column (e.g. "String", "DateTime64(6)", "Nullable(String)").
    /// Null when the user manages the schema externally and table creation is not needed.
    /// </summary>
    public string? ColumnType { get; }

    protected ColumnWriterBase(string columnName, string? columnType)
    {
        ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
        ColumnType = columnType;
    }

    /// <summary>
    /// Extracts the value for this column from the given log event.
    /// </summary>
    /// <param name="logEvent">The log event to extract value from.</param>
    /// <param name="formatProvider">Optional format provider for rendering.</param>
    /// <returns>The value to insert into this column, or the default value if not available.</returns>
    public abstract object? GetValue(LogEvent logEvent, IFormatProvider? formatProvider = null);
}
