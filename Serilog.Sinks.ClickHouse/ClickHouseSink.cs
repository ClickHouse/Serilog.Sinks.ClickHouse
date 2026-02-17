using System.Diagnostics;
using ClickHouse.Driver;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Constraints;
using ClickHouse.Driver.Copy;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.ClickHouse.Client;
using Serilog.Sinks.ClickHouse.Configuration;
using Serilog.Sinks.ClickHouse.Schema;

namespace Serilog.Sinks.ClickHouse;

/// <summary>
/// Serilog sink for ClickHouse with flexible schema configuration,
/// resilience features, and observability.
/// </summary>
public sealed class ClickHouseSink : IBatchedLogEventSink, IDisposable
{
    private readonly ClickHouseSinkOptions _options;
    private readonly IClickHouseClient _client;
    private readonly bool _ownsClient;
    private readonly SchemaManager _schemaManager;
    private readonly IFormatProvider? _formatProvider;
    private readonly InsertOptions _insertOptions = new() { Format = RowBinaryFormat.RowBinaryWithDefaults };
    private bool _tableCreated;
    private bool _disposed;

    /// <summary>
    /// Creates a new ClickHouse sink with the specified options.
    /// The sink creates and owns the <see cref="IClickHouseClient"/> internally.
    /// </summary>
    public ClickHouseSink(ClickHouseSinkOptions options)
        : this(options, new ClickHouseClient(options.ConnectionString), ownsClient: true)
    {
    }

    /// <summary>
    /// Creates a new ClickHouse sink with an externally managed client.
    /// The sink does not dispose the client; the caller retains ownership.
    /// </summary>
    public ClickHouseSink(ClickHouseSinkOptions options, IClickHouseClient client)
        : this(options, client, ownsClient: false)
    {
    }

#if NET7_0_OR_GREATER
    /// <summary>
    /// Creates a new ClickHouse sink with a data source.
    /// The sink obtains and owns the <see cref="IClickHouseClient"/> from the data source.
    /// </summary>
    public ClickHouseSink(ClickHouseSinkOptions options, ClickHouseDataSource dataSource)
        : this(
            options,
            (dataSource ?? throw new ArgumentNullException(nameof(dataSource))).GetClient(),
            ownsClient: true)
    {
    }
#endif

    /// <summary>
    /// Creates a new ClickHouse sink with explicit client ownership control.
    /// </summary>
    /// <param name="options">The sink options.</param>
    /// <param name="client">The ClickHouse client.</param>
    /// <param name="ownsClient">
    /// When <c>true</c>, the sink disposes the client on <see cref="Dispose"/>.
    /// When <c>false</c>, the caller is responsible for disposing the client.
    /// </param>
    internal ClickHouseSink(ClickHouseSinkOptions options, IClickHouseClient client, bool ownsClient)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _options.Validate();

        _client = client ?? throw new ArgumentNullException(nameof(client));
        _ownsClient = ownsClient;
        _schemaManager = new SchemaManager(_client);
        _formatProvider = options.FormatProvider;
    }

    /// <summary>
    /// Emits a batch of log events to ClickHouse.
    /// </summary>
    public async Task EmitBatchAsync(IReadOnlyCollection<LogEvent> batch)
    {
        if (_disposed)
            return;

        var stopwatch = Stopwatch.StartNew();

        try
        {
            // Ensure table exists on first batch
            if (!_tableCreated)
            {
                await EnsureTableCreatedAsync().ConfigureAwait(false);
                _tableCreated = true;
            }

            // Transform log events to row arrays using column writers
            var columns = _options.Schema!.Columns.Select(c => c.ColumnName);
            var rows = batch.Select(TransformLogEvent);

            // Bulk insert
            
            await _client.InsertBinaryAsync(
                SqlGenerator.EscapeTableName(_options.Schema!.FullTableName),
                columns,
                rows,
                _insertOptions).ConfigureAwait(false);

            stopwatch.Stop();
            SelfLog.WriteLine("Successfully wrote {0} events to ClickHouse in {1}ms", batch.Count, stopwatch.ElapsedMilliseconds);

            // Invoke success callback
            _options.OnBatchWritten?.Invoke(batch.Count, stopwatch.Elapsed);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            SelfLog.WriteLine("Failed to write {0} events to ClickHouse: {1}", batch.Count, ex.Message);

            // Invoke failure callback
            _options.OnBatchFailed?.Invoke(ex, batch.Count);

            throw; // Re-throw to let Serilog handle retry logic
        }
    }

    /// <summary>
    /// Called when the sink is being shut down.
    /// </summary>
    public Task OnEmptyBatchAsync()
    {
        return Task.CompletedTask;
    }

    private async Task EnsureTableCreatedAsync()
    {
        try
        {
            await _schemaManager.EnsureTableAsync(_options.Schema!, _options.TableCreation).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SelfLog.WriteLine("Failed to ensure table exists: {0}", ex.Message);
            throw;
        }
    }

    private object[] TransformLogEvent(LogEvent logEvent)
    {
        var columns = _options.Schema!.Columns;
        var values = new object[columns.Count];

        for (var i = 0; i < columns.Count; i++)
        {
            try
            {
                values[i] = columns[i].GetValue(logEvent, _formatProvider) ?? DBNull.Value;
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine("Error extracting column '{0}': {1}", columns[i].ColumnName, ex.Message);
                values[i] = DBDefault.Value;
            }
        }

        return values;
    }

    /// <summary>
    /// Disposes resources used by the sink.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        if (_ownsClient)
            _client.Dispose();
    }
}
