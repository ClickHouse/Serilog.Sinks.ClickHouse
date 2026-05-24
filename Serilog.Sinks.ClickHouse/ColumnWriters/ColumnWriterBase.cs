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

    /// <summary>
    /// Optional default expression for this column (e.g. "now()", "'unknown'", "0").
    /// When set, the column definition includes a <c>DEFAULT expr</c> clause.
    /// </summary>
    public string? Default { get; set; }

    /// <summary>
    /// Optional compression codec for this column (e.g. "ZSTD", "LZ4", "FSST, ZSTD", "DoubleDelta, ZSTD").
    /// When set, the column definition includes a <c>CODEC(...)</c> clause.
    /// </summary>
    public string? Codec { get; set; }

    /// <summary>
    /// Optional TTL expression for this column (e.g. "timestamp + INTERVAL 30 DAY").
    /// When set, the column definition includes a <c>TTL expr</c> clause.
    /// </summary>
    public string? Ttl { get; set; }

    /// <summary>
    /// Optional comment for this column (e.g. "Event timestamp in UTC").
    /// When set, the column definition includes a <c>COMMENT 'text'</c> clause.
    /// </summary>
    public string? Comment { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether this column should be excluded from the <c>INSERT</c> statement.
    /// Useful for columns whose values are generated automatically on the database side via <c>DEFAULT</c> expressions.
    /// </summary>
    public bool SkipWrite { get; set; }
    
    /// <summary>
    /// Initializes a new instance of the <see cref="ColumnWriterBase"/> class.
    /// </summary>
    /// <param name="columnName">The name of the column in the ClickHouse table.</param>
    /// <param name="columnType">The ClickHouse data type, or null if schema is managed externally.</param>
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
