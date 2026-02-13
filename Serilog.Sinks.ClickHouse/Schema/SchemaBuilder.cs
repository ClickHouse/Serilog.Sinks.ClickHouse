using Serilog.Sinks.ClickHouse.ColumnWriters;

namespace Serilog.Sinks.ClickHouse.Schema;

/// <summary>
/// Fluent builder for creating TableSchema instances.
/// </summary>
public sealed class SchemaBuilder
{
    private string? _database;
    private string _tableName = "logs";
    private readonly List<ColumnWriterBase> _columns = new();
    private TableEngine _engine = new DefaultEngine();
    private string? _comment;

    /// <summary>
    /// Sets the database name.
    /// </summary>
    public SchemaBuilder WithDatabase(string database)
    {
        _database = database;
        return this;
    }

    /// <summary>
    /// Sets the table name.
    /// </summary>
    public SchemaBuilder WithTableName(string tableName)
    {
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        return this;
    }

    /// <summary>
    /// Adds a column writer to the schema.
    /// </summary>
    public SchemaBuilder AddColumn(ColumnWriterBase column)
    {
        _columns.Add(column ?? throw new ArgumentNullException(nameof(column)));
        return this;
    }

    /// <summary>
    /// Adds a timestamp column. ClickHouse type: <c>DateTime64({precision})</c>.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="precision">Sub-second precision (0–9). Default 3 produces <c>DateTime64(3)</c> (milliseconds).</param>
    /// <param name="useUtc">If true (default), stores <c>DateTimeOffset.UtcDateTime</c>; otherwise local time.</param>
    public SchemaBuilder AddTimestampColumn(string name = "timestamp", int precision = 3, bool useUtc = true)
    {
        _columns.Add(new TimestampColumnWriter(name, $"DateTime64({precision})", useUtc));
        return this;
    }

    /// <summary>
    /// Adds a log level column.
    /// ClickHouse type: <c>LowCardinality(String)</c> when <paramref name="asString"/> is true,
    /// <c>UInt8</c> when false.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="asString">If true (default), stores the level name (e.g. "Information"); otherwise the numeric value.</param>
    public SchemaBuilder AddLevelColumn(string name = "level", bool asString = true)
    {
        _columns.Add(new LevelColumnWriter(name, asString));
        return this;
    }

    /// <summary>
    /// Adds a rendered message column (property values substituted). ClickHouse type: <c>String</c>.
    /// </summary>
    /// <param name="name">Column name.</param>
    public SchemaBuilder AddMessageColumn(string name = "message")
    {
        _columns.Add(new RenderedMessageColumnWriter(name));
        return this;
    }

    /// <summary>
    /// Adds a raw message template column (without property substitution). ClickHouse type: <c>String</c>.
    /// </summary>
    /// <param name="name">Column name.</param>
    public SchemaBuilder AddMessageTemplateColumn(string name = "message_template")
    {
        _columns.Add(new MessageTemplateColumnWriter(name));
        return this;
    }

    /// <summary>
    /// Adds an exception column (<c>Exception.ToString()</c> or null). ClickHouse type: <c>Nullable(String)</c>.
    /// </summary>
    /// <param name="name">Column name.</param>
    public SchemaBuilder AddExceptionColumn(string name = "exception")
    {
        _columns.Add(new ExceptionColumnWriter(name));
        return this;
    }

    /// <summary>
    /// Adds a properties column that captures all log event properties as JSON. ClickHouse type: <c>JSON</c>.
    /// </summary>
    /// <param name="name">Column name.</param>
    public SchemaBuilder AddPropertiesColumn(string name = "properties")
    {
        _columns.Add(new PropertiesColumnWriter(name));
        return this;
    }

    /// <summary>
    /// Adds a properties column with a custom ClickHouse type string.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="columnType">ClickHouse type string (e.g. "JSON(Application String, UserId Int64)").</param>
    public SchemaBuilder AddPropertiesColumn(string name, string columnType)
    {
        _columns.Add(new PropertiesColumnWriter(name, columnType));
        return this;
    }

    /// <summary>
    /// Adds a column for the entire log event serialized as JSON. ClickHouse type: <c>String</c>.
    /// </summary>
    /// <param name="name">Column name.</param>
    public SchemaBuilder AddLogEventColumn(string name = "log_event")
    {
        _columns.Add(new LogEventColumnWriter(name));
        return this;
    }

    /// <summary>
    /// Adds a column for a single named property extracted from the log event.
    /// The ClickHouse type is not auto-determined — provide <paramref name="type"/> when the sink
    /// manages table creation (e.g. <c>"Nullable(Int64)"</c>, <c>"Nullable(String)"</c>).
    /// If the table schema is managed externally, <paramref name="type"/> can be omitted.
    /// </summary>
    /// <param name="propertyName">The property name in <c>LogEvent.Properties</c> to extract.</param>
    /// <param name="type">ClickHouse column type. Required for automatic table creation; optional if the table already exists.</param>
    /// <param name="columnName">Column name in ClickHouse. Defaults to <paramref name="propertyName"/>.</param>
    /// <param name="writeMethod">How to serialize the value (Raw CLR value, ToString, or JSON).</param>
    public SchemaBuilder AddPropertyColumn(
        string propertyName,
        string? type = null,
        string? columnName = null,
        PropertyWriteMethod writeMethod = PropertyWriteMethod.Raw)
    {
        _columns.Add(new SinglePropertyColumnWriter(propertyName, columnName, type, writeMethod));
        return this;
    }

    /// <summary>
    /// Sets the table engine directly.
    /// </summary>
    public SchemaBuilder WithEngine(TableEngine engine)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        return this;
    }

    /// <summary>
    /// Sets a comment for the table.
    /// </summary>
    public SchemaBuilder WithComment(string comment)
    {
        _comment = comment;
        return this;
    }

    /// <summary>
    /// Builds the TableSchema instance.
    /// </summary>
    public TableSchema Build()
    {
        if (_columns.Count == 0)
            throw new InvalidOperationException("At least one column must be added to the schema.");

        var schema = new TableSchema
        {
            Database = _database,
            TableName = _tableName,
            Columns = _columns.ToList().AsReadOnly(),
            Engine = _engine,
            Comment = _comment,
        };

        schema.Validate();
        return schema;
    }
}
