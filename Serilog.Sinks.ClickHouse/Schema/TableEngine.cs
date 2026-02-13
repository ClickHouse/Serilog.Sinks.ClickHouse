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

public record CustomEngine : TableEngine
{
    public CustomEngine(string engineDefinition)
    {
        EngineDefinition = engineDefinition;
    }

    private string EngineDefinition { get; }

    public override string ToSql() => EngineDefinition;
}

/// <summary>
/// MergeTree engine with timestamp-based ordering and monthly partitioning.
/// </summary>
public record DefaultEngine : TableEngine
{
    public override string ToSql() =>
        """
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (timestamp)
        """;
}
