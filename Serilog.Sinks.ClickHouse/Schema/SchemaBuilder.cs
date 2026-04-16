using Serilog.Sinks.ClickHouse.ColumnWriters;

namespace Serilog.Sinks.ClickHouse.Schema;

/// <summary>
/// Fluent builder for creating TableSchema instances.
/// </summary>
public sealed class SchemaBuilder
{
    private string? _database;
    private string _tableName = "logs";
    private string? _clusterName;
    private readonly List<ColumnWriterBase> _columns = new();
    private readonly List<string> _indexes = new();
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
    /// Sets the cluster name for distributed DDL (ON CLUSTER clause).
    /// </summary>
    /// <param name="clusterName">The ClickHouse cluster name.</param>
    public SchemaBuilder OnCluster(string clusterName)
    {
        if (string.IsNullOrWhiteSpace(clusterName))
            throw new ArgumentException("Cluster name cannot be empty.", nameof(clusterName));

        _clusterName = clusterName;
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
    /// <param name="precision">Sub-second precision (0–9). Default produces <c>DateTime64(6) (microseconds)</c>.</param>
    /// <param name="useUtc">If true (default), stores <c>DateTimeOffset.UtcDateTime</c>; otherwise local time.</param>
    /// <param name="codec">Optional compression codec (e.g. "DoubleDelta, ZSTD").</param>
    /// <param name="defaultExpression">Optional DEFAULT expression (e.g. "now()").</param>
    /// <param name="ttl">Optional column-level TTL expression.</param>
    /// <param name="comment">Optional column comment.</param>
    public SchemaBuilder AddTimestampColumn(
        string name = "timestamp",
        int precision = 6,
        bool useUtc = true,
        string? codec = null,
        string? defaultExpression = null,
        string? ttl = null,
        string? comment = null)
    {
        if (precision < 0)
            throw new ArgumentOutOfRangeException(nameof(precision), "Precision must be greater than or equal to zero.");
        if (precision > 9)
            throw new ArgumentOutOfRangeException(nameof(precision), "Precision must be smaller than or equal to 9.");

        _columns.Add(new TimestampColumnWriter(name, $"DateTime64({precision})", useUtc)
        {
            Codec = codec, Default = defaultExpression, Ttl = ttl, Comment = comment,
        });
        return this;
    }

    /// <summary>
    /// Adds a log level column.
    /// ClickHouse type: <c>LowCardinality(String)</c> when <paramref name="asString"/> is true,
    /// <c>UInt8</c> when false.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="asString">If true (default), stores the level name (e.g. "Information"); otherwise the numeric value.</param>
    /// <param name="codec">Optional compression codec (e.g. "ZSTD").</param>
    /// <param name="defaultExpression">Optional DEFAULT expression.</param>
    /// <param name="ttl">Optional column-level TTL expression.</param>
    /// <param name="comment">Optional column comment.</param>
    public SchemaBuilder AddLevelColumn(
        string name = "level",
        bool asString = true,
        string? codec = null,
        string? defaultExpression = null,
        string? ttl = null,
        string? comment = null)
    {
        _columns.Add(new LevelColumnWriter(name, asString)
        {
            Codec = codec, Default = defaultExpression, Ttl = ttl, Comment = comment,
        });
        return this;
    }

    /// <summary>
    /// Adds a rendered message column (property values substituted). ClickHouse type: <c>String</c>.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="codec">Optional compression codec (e.g. "ZSTD").</param>
    /// <param name="defaultExpression">Optional DEFAULT expression.</param>
    /// <param name="ttl">Optional column-level TTL expression.</param>
    /// <param name="comment">Optional column comment.</param>
    public SchemaBuilder AddMessageColumn(
        string name = "message",
        string? codec = null,
        string? defaultExpression = null,
        string? ttl = null,
        string? comment = null)
    {
        _columns.Add(new RenderedMessageColumnWriter(name)
        {
            Codec = codec, Default = defaultExpression, Ttl = ttl, Comment = comment,
        });
        return this;
    }

    /// <summary>
    /// Adds a raw message template column (without property substitution). ClickHouse type: <c>String</c>.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="codec">Optional compression codec (e.g. "ZSTD").</param>
    /// <param name="defaultExpression">Optional DEFAULT expression.</param>
    /// <param name="ttl">Optional column-level TTL expression.</param>
    /// <param name="comment">Optional column comment.</param>
    public SchemaBuilder AddMessageTemplateColumn(
        string name = "message_template",
        string? codec = null,
        string? defaultExpression = null,
        string? ttl = null,
        string? comment = null)
    {
        _columns.Add(new MessageTemplateColumnWriter(name)
        {
            Codec = codec, Default = defaultExpression, Ttl = ttl, Comment = comment,
        });
        return this;
    }

    /// <summary>
    /// Adds an exception column (<c>Exception.ToString()</c> or null). ClickHouse type: <c>Nullable(String)</c>.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="codec">Optional compression codec (e.g. "ZSTD").</param>
    /// <param name="defaultExpression">Optional DEFAULT expression.</param>
    /// <param name="ttl">Optional column-level TTL expression.</param>
    /// <param name="comment">Optional column comment.</param>
    public SchemaBuilder AddExceptionColumn(
        string name = "exception",
        string? codec = null,
        string? defaultExpression = null,
        string? ttl = null,
        string? comment = null)
    {
        _columns.Add(new ExceptionColumnWriter(name)
        {
            Codec = codec, Default = defaultExpression, Ttl = ttl, Comment = comment,
        });
        return this;
    }

    /// <summary>
    /// Adds a properties column that captures all log event properties as JSON. ClickHouse type: <c>JSON</c>.
    /// To set column-level DDL options (codec, default, ttl, comment), use the
    /// <see cref="AddPropertiesColumn(string, string, string?, string?, string?, string?)"/> overload
    /// with an explicit <c>columnType</c>, or <see cref="AddColumn"/> with a pre-built
    /// <see cref="PropertiesColumnWriter"/>.
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
    /// <param name="codec">Optional compression codec (e.g. "ZSTD").</param>
    /// <param name="defaultExpression">Optional DEFAULT expression.</param>
    /// <param name="ttl">Optional column-level TTL expression.</param>
    /// <param name="comment">Optional column comment.</param>
    public SchemaBuilder AddPropertiesColumn(
        string name,
        string columnType,
        string? codec = null,
        string? defaultExpression = null,
        string? ttl = null,
        string? comment = null)
    {
        _columns.Add(new PropertiesColumnWriter(name, columnType)
        {
            Codec = codec, Default = defaultExpression, Ttl = ttl, Comment = comment,
        });
        return this;
    }

    /// <summary>
    /// Adds a column for the entire log event serialized as JSON. ClickHouse type: <c>String</c>.
    /// </summary>
    /// <param name="name">Column name.</param>
    /// <param name="codec">Optional compression codec (e.g. "ZSTD").</param>
    /// <param name="defaultExpression">Optional DEFAULT expression.</param>
    /// <param name="ttl">Optional column-level TTL expression.</param>
    /// <param name="comment">Optional column comment.</param>
    public SchemaBuilder AddLogEventColumn(
        string name = "log_event",
        string? codec = null,
        string? defaultExpression = null,
        string? ttl = null,
        string? comment = null)
    {
        _columns.Add(new LogEventColumnWriter(name)
        {
            Codec = codec, Default = defaultExpression, Ttl = ttl, Comment = comment,
        });
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
    /// <param name="codec">Optional compression codec (e.g. "ZSTD").</param>
    /// <param name="defaultExpression">Optional DEFAULT expression.</param>
    /// <param name="ttl">Optional column-level TTL expression.</param>
    /// <param name="comment">Optional column comment.</param>
    public SchemaBuilder AddPropertyColumn(
        string propertyName,
        string? type = null,
        string? columnName = null,
        PropertyWriteMethod writeMethod = PropertyWriteMethod.Raw,
        string? codec = null,
        string? defaultExpression = null,
        string? ttl = null,
        string? comment = null)
    {
        _columns.Add(new SinglePropertyColumnWriter(propertyName, columnName, type, writeMethod)
        {
            Codec = codec, Default = defaultExpression, Ttl = ttl, Comment = comment,
        });
        return this;
    }

    /// <summary>
    /// Adds a raw index definition to the table schema.
    /// The definition is included verbatim in the CREATE TABLE statement.
    /// </summary>
    /// <param name="indexDefinition">
    /// The full index clause, e.g. <c>"INDEX idx_level level TYPE set(0) GRANULARITY 1"</c>.
    /// </param>
    public SchemaBuilder AddIndex(string indexDefinition)
    {
        if (string.IsNullOrWhiteSpace(indexDefinition))
            throw new ArgumentException("Index definition cannot be empty.", nameof(indexDefinition));

        _indexes.Add(indexDefinition);
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
    /// Sets the table engine directly using a SQL expression.
    /// </summary>
    public SchemaBuilder WithEngine(string engineExpression)
    {
        if (string.IsNullOrWhiteSpace(engineExpression))
            throw new ArgumentNullException(nameof(engineExpression));

        _engine = new CustomEngine(engineExpression);
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
            ClusterName = _clusterName,
            Columns = _columns.ToList().AsReadOnly(),
            Indexes = _indexes.ToList().AsReadOnly(),
            Engine = _engine,
            Comment = _comment,
        };

        schema.Validate();
        return schema;
    }
}
