using Serilog.Sinks.ClickHouse.ColumnWriters;

namespace Serilog.Sinks.ClickHouse.Schema;

/// <summary>
/// Provides default schema configurations for common use cases.
/// Each method returns a <see cref="SchemaBuilder"/> so presets can be customized
/// before calling <see cref="SchemaBuilder.Build"/>.
/// </summary>
/// <example>
/// Start from the default preset and add a custom property column:
/// <code>
/// var schema = DefaultSchema.Create("app_logs")
///     .AddPropertyColumn("UserId", "Nullable(Int64)")
///     .Build();
/// </code>
/// </example>
public static class DefaultSchema
{
    /// <summary>
    /// Creates a sensible default schema suitable for most logging use cases.
    ///
    /// Default columns:
    /// - timestamp (DateTime64(6)) - Event timestamp in UTC
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
    /// <returns>A <see cref="SchemaBuilder"/> that can be customized further or built directly.</returns>
    public static SchemaBuilder Create(string tableName, string? database = null)
    {
        var builder = new SchemaBuilder()
            .WithTableName(tableName)
            .AddTimestampColumn("timestamp", precision: 3, useUtc: true)
            .AddLevelColumn("level")
            .AddMessageColumn("message")
            .AddMessageTemplateColumn("message_template")
            .AddExceptionColumn("exception")
            .AddPropertiesColumn("properties");

        if (database is not null)
            builder.WithDatabase(database);

        return builder;
    }

    /// <summary>
    /// Creates a minimal schema with just the essential columns.
    ///
    /// Columns:
    /// - timestamp (DateTime64(6))
    /// - level (UInt8) - Log level as integer
    /// - message (String)
    ///
    /// Use this for high-volume logging where storage is a concern.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="database">Optional database name.</param>
    /// <returns>A <see cref="SchemaBuilder"/> that can be customized further or built directly.</returns>
    public static SchemaBuilder CreateMinimal(string tableName, string? database = null)
    {
        var builder = new SchemaBuilder()
            .WithTableName(tableName)
            .AddTimestampColumn("timestamp", precision: 3, useUtc: true)
            .AddLevelColumn("level", asString: false)
            .AddMessageColumn("message");

        if (database is not null)
            builder.WithDatabase(database);

        return builder;
    }

    /// <summary>
    /// Creates a comprehensive schema that includes the full log event as JSON.
    ///
    /// Columns:
    /// - timestamp (DateTime64(6))
    /// - level (LowCardinality(String))
    /// - message (String)
    /// - log_event (String) - Complete log event as JSON
    ///
    /// Use this when you need full event fidelity for debugging or compliance.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="database">Optional database name.</param>
    /// <returns>A <see cref="SchemaBuilder"/> that can be customized further or built directly.</returns>
    public static SchemaBuilder CreateComprehensive(string tableName, string? database = null)
    {
        var builder = new SchemaBuilder()
            .WithTableName(tableName)
            .AddTimestampColumn("timestamp", precision: 3, useUtc: true)
            .AddLevelColumn("level", asString: true)
            .AddMessageColumn("message")
            .AddMessageTemplateColumn("message_template")
            .AddExceptionColumn("exception")
            .AddPropertiesColumn("properties")
            .AddLogEventColumn("log_event");

        if (database is not null)
            builder.WithDatabase(database);

        return builder;
    }
}
