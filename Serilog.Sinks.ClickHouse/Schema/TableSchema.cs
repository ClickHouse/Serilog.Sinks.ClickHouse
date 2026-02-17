using Serilog.Sinks.ClickHouse.ColumnWriters;

namespace Serilog.Sinks.ClickHouse.Schema;

/// <summary>
/// Defines the complete schema for a ClickHouse log table.
/// Immutable after creation.
/// </summary>
public sealed record TableSchema
{
    /// <summary>
    /// The database name (optional - uses default database if not specified).
    /// </summary>
    public string? Database { get; init; }

    /// <summary>
    /// The table name.
    /// </summary>
    public string TableName { get; init; } = "logs";

    /// <summary>
    /// The column writers that define the table columns.
    /// Each writer knows its column name, type, and how to extract values from LogEvents.
    /// </summary>
    public IReadOnlyList<ColumnWriterBase> Columns { get; init; } = Array.Empty<ColumnWriterBase>();

    /// <summary>
    /// The table engine configuration.
    /// </summary>
    public TableEngine Engine { get; init; } = new DefaultEngine();

    /// <summary>
    /// Optional table comment.
    /// </summary>
    public string? Comment { get; init; }

    /// <summary>
    /// Gets the fully qualified table name (database.table or just table).
    /// </summary>
    public string FullTableName => string.IsNullOrEmpty(Database) ? TableName : $"{Database}.{TableName}";

    /// <summary>
    /// Validates the schema configuration.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if the schema is invalid.</exception>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(TableName))
            throw new InvalidOperationException("Table name is required.");

        if (Columns is not { Count: > 0 })
            throw new InvalidOperationException("At least one column is required.");

        var columnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var column in Columns)
        {
            if (string.IsNullOrWhiteSpace(column.ColumnName))
                throw new InvalidOperationException("Column name cannot be empty.");

            if (!columnNames.Add(column.ColumnName))
                throw new InvalidOperationException($"Duplicate column name: {column.ColumnName}");
        }
    }
}
