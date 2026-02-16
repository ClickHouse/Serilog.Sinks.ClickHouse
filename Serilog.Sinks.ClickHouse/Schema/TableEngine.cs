namespace Serilog.Sinks.ClickHouse.Schema;

/// <summary>
/// Base class for ClickHouse table engines.
/// </summary>
public abstract record TableEngine
{
    /// <summary>
    /// Generates the ENGINE clause and related settings for CREATE TABLE.
    /// </summary>
    public abstract string ToSql();
}

/// <summary>
/// A table engine defined by a custom SQL engine clause.
/// </summary>
public record CustomEngine : TableEngine
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CustomEngine"/> class.
    /// </summary>
    /// <param name="engineDefinition">The raw SQL engine definition.</param>
    public CustomEngine(string engineDefinition)
    {
        EngineDefinition = engineDefinition;
    }

    private string EngineDefinition { get; }

    /// <inheritdoc />
    public override string ToSql() => EngineDefinition;
}

/// <summary>
/// MergeTree engine with timestamp-based ordering and monthly partitioning.
/// </summary>
public record DefaultEngine : TableEngine
{
    /// <inheritdoc />
    public override string ToSql() =>
        """
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (timestamp)
        """;
}
