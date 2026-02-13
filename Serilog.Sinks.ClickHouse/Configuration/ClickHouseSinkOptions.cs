using Serilog.Events;
using Serilog.Sinks.ClickHouse.Client;
using Serilog.Sinks.ClickHouse.Schema;

namespace Serilog.Sinks.ClickHouse.Configuration;

/// <summary>
/// Complete configuration for the ClickHouse sink.
/// </summary>
public sealed record ClickHouseSinkOptions
{
    /// <summary>
    /// ClickHouse connection string.
    /// </summary>
    public string ConnectionString { get; init; } = string.Empty;

    /// <summary>
    /// Table schema configuration.
    /// </summary>
    public TableSchema? Schema { get; init; }

    /// <summary>
    /// Table creation configuration.
    /// </summary>
    public TableCreationOptions TableCreation { get; init; } = new();

    /// <summary>
    /// Minimum log level to write.
    /// </summary>
    public LogEventLevel MinimumLevel { get; init; } = LogEventLevel.Verbose;

    /// <summary>
    /// Format provider for message rendering.
    /// </summary>
    public IFormatProvider? FormatProvider { get; init; }

    /// <summary>
    /// Called after a batch is successfully written to ClickHouse.
    /// Parameters: event count, write duration.
    /// </summary>
    public Action<int, TimeSpan>? OnBatchWritten { get; init; }

    /// <summary>
    /// Called when a batch fails to write to ClickHouse.
    /// Parameters: exception, event count.
    /// </summary>
    public Action<Exception, int>? OnBatchFailed { get; init; }

    /// <summary>
    /// Validates the options.
    /// </summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new InvalidOperationException("ConnectionString is required.");

        if (Schema is null)
            throw new InvalidOperationException("Schema is required.");

        Schema.Validate();
    }
}
