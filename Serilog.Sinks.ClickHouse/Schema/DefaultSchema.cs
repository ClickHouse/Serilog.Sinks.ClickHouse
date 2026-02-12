using Serilog.Sinks.ClickHouse.ColumnWriters;

namespace Serilog.Sinks.ClickHouse.Schema;

/// <summary>
/// Provides default schema configurations for common use cases.
/// </summary>
public static class DefaultSchema
{
    /// <summary>
    /// Creates a sensible default schema suitable for most logging use cases.
    ///
    /// Default columns:
    /// - timestamp (DateTime64(3)) - Event timestamp in UTC
    /// - level (LowCardinality(String)) - Log level as string (e.g., "Information")
    /// - message (String) - Rendered message with property values substituted
    /// - message_template (String) - Raw message template
    /// - exception (Nullable(String)) - Exception details if present
    /// - properties (JSON) - All additional properties as JSON
    ///
    /// This schema captures everything needed for comprehensive logging while
    /// allowing flexible querying via the properties JSON column.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="database">Optional database name.</param>
    /// <returns>A complete TableSchema ready for use.</returns>
    public static TableSchema Create(string tableName, string? database = null)
    {
        return new TableSchema
        {
            Database = database,
            TableName = tableName,
            Columns = new ColumnWriterBase[]
            {
                new TimestampColumnWriter("timestamp", "DateTime64(3)", useUtc: true),
                new LevelColumnWriter("level"),
                new RenderedMessageColumnWriter("message"),
                new MessageTemplateColumnWriter("message_template"),
                new ExceptionColumnWriter("exception"),
                new PropertiesColumnWriter("properties"),
            },
            Engine = new DefaultEngine(),
        };
    }

    /// <summary>
    /// Creates a minimal schema with just the essential columns.
    ///
    /// Columns:
    /// - timestamp (DateTime64(3))
    /// - level (UInt8) - Log level as integer
    /// - message (String)
    ///
    /// Use this for high-volume logging where storage is a concern.
    /// </summary>
    public static TableSchema CreateMinimal(string tableName, string? database = null)
    {
        return new TableSchema
        {
            Database = database,
            TableName = tableName,
            Columns = new ColumnWriterBase[]
            {
                new TimestampColumnWriter("timestamp", "DateTime64(3)", useUtc: true),
                new LevelColumnWriter("level", asString: false),
                new RenderedMessageColumnWriter("message"),
            },
            Engine = new DefaultEngine(),
        };
    }

    /// <summary>
    /// Creates a comprehensive schema that includes the full log event as JSON.
    ///
    /// Columns:
    /// - timestamp (DateTime64(3))
    /// - level (LowCardinality(String))
    /// - message (String)
    /// - log_event (String) - Complete log event as JSON
    ///
    /// Use this when you need full event fidelity for debugging or compliance.
    /// </summary>
    public static TableSchema CreateComprehensive(string tableName, string? database = null)
    {
        return new TableSchema
        {
            Database = database,
            TableName = tableName,
            Columns = new ColumnWriterBase[]
            {
                new TimestampColumnWriter("timestamp", "DateTime64(3)", useUtc: true),
                new LevelColumnWriter("level", asString: true),
                new RenderedMessageColumnWriter("message"),
                new MessageTemplateColumnWriter("message_template"),
                new ExceptionColumnWriter("exception"),
                new PropertiesColumnWriter("properties"),
                new LogEventColumnWriter("log_event"),
            },
            Engine = new DefaultEngine(),
        };
    }
}
