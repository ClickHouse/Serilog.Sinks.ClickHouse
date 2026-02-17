using ClickHouse.Driver;
using ClickHouse.Driver.ADO;
using Serilog.Configuration;
using Serilog.Events;
using Serilog.Sinks.ClickHouse.Client;
using Serilog.Sinks.ClickHouse.Schema;

namespace Serilog.Sinks.ClickHouse.Configuration;

/// <summary>
/// Extension methods for configuring the ClickHouse sink.
/// </summary>
public static class ClickHouseSinkExtensions
{
    // Default batching configuration values
    private const int DefaultBatchSizeLimit = 10_000;
    private const int DefaultQueueLimit = 100_000;
    private static readonly TimeSpan DefaultFlushInterval = TimeSpan.FromSeconds(5);

    private static BatchingOptions CreateBatchingOptions(int batchSizeLimit, TimeSpan? flushInterval, int queueLimit)
        => new()
        {
            BatchSizeLimit = batchSizeLimit,
            BufferingTimeLimit = flushInterval ?? DefaultFlushInterval,
            QueueLimit = queueLimit,
        };

    private static ClickHouseSinkOptions CreateOptions(
        string tableName,
        string? database,
        TableCreationMode tableCreation,
        LogEventLevel minimumLevel,
        IFormatProvider? formatProvider,
        Action<int, TimeSpan>? onBatchWritten,
        Action<Exception, int>? onBatchFailed,
        string connectionString = "injected")
        => new()
        {
            ConnectionString = connectionString,
            Schema = DefaultSchema.Create(tableName, database).Build(),
            TableCreation = new TableCreationOptions { Mode = tableCreation },
            MinimumLevel = minimumLevel,
            FormatProvider = formatProvider,
            OnBatchWritten = onBatchWritten,
            OnBatchFailed = onBatchFailed,
        };

    /// <summary>
    /// Writes log events to a ClickHouse database using full options configuration.
    /// </summary>
    /// <param name="loggerConfiguration">The logger sink configuration.</param>
    /// <param name="options">The sink options.</param>
    /// <param name="batchingOptions">Optional batching configuration. If null, defaults are used.</param>
    public static LoggerConfiguration ClickHouse(
        this LoggerSinkConfiguration loggerConfiguration,
        ClickHouseSinkOptions options,
        BatchingOptions? batchingOptions = null)
    {
        ArgumentNullException.ThrowIfNull(loggerConfiguration);
        ArgumentNullException.ThrowIfNull(options);

        options.Validate();

        var sink = new ClickHouseSink(options);
        batchingOptions ??= CreateBatchingOptions(DefaultBatchSizeLimit, DefaultFlushInterval, DefaultQueueLimit);

        return loggerConfiguration.Sink(sink, batchingOptions, options.MinimumLevel);
    }

    /// <summary>
    /// Writes log events to a ClickHouse database using full options and a pre-built client.
    /// Use this when the <see cref="IClickHouseClient"/> is managed externally (e.g. via DI).
    /// </summary>
    /// <param name="loggerConfiguration">The logger sink configuration.</param>
    /// <param name="options">The sink options. <see cref="ClickHouseSinkOptions.ConnectionString"/> is not used when a client is provided.</param>
    /// <param name="client">A pre-built ClickHouse client.</param>
    /// <param name="batchingOptions">Optional batching configuration. If null, defaults are used.</param>
    public static LoggerConfiguration ClickHouse(
        this LoggerSinkConfiguration loggerConfiguration,
        ClickHouseSinkOptions options,
        IClickHouseClient client,
        BatchingOptions? batchingOptions = null)
    {
        ArgumentNullException.ThrowIfNull(loggerConfiguration);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(client);

        // ConnectionString is not needed when a pre-built client is provided.
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            options = options with { ConnectionString = "injected" };

        var sink = new ClickHouseSink(options, client);
        batchingOptions ??= CreateBatchingOptions(DefaultBatchSizeLimit, DefaultFlushInterval, DefaultQueueLimit);

        return loggerConfiguration.Sink(sink, batchingOptions, options.MinimumLevel);
    }

#if NET7_0_OR_GREATER
    /// <summary>
    /// Writes log events to a ClickHouse database using full options and a data source.
    /// Use this when the <see cref="ClickHouseDataSource"/> is managed externally (e.g. via DI).
    /// </summary>
    /// <param name="loggerConfiguration">The logger sink configuration.</param>
    /// <param name="options">The sink options. <see cref="ClickHouseSinkOptions.ConnectionString"/> is not used when a data source is provided.</param>
    /// <param name="dataSource">A pre-built ClickHouse data source.</param>
    /// <param name="batchingOptions">Optional batching configuration. If null, defaults are used.</param>
    public static LoggerConfiguration ClickHouse(
        this LoggerSinkConfiguration loggerConfiguration,
        ClickHouseSinkOptions options,
        ClickHouseDataSource dataSource,
        BatchingOptions? batchingOptions = null)
    {
        ArgumentNullException.ThrowIfNull(loggerConfiguration);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(dataSource);

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            options = options with { ConnectionString = "injected" };

        var sink = new ClickHouseSink(options, dataSource);
        batchingOptions ??= CreateBatchingOptions(DefaultBatchSizeLimit, DefaultFlushInterval, DefaultQueueLimit);

        return loggerConfiguration.Sink(sink, batchingOptions, options.MinimumLevel);
    }
#endif
    
    /// <summary>
    /// Writes log events to a ClickHouse database using the default schema.
    /// This is the simplest way to get started - just provide connection string and table name.
    /// </summary>
    /// <param name="loggerConfiguration">The logger configuration.</param>
    /// <param name="connectionString">ClickHouse connection string.</param>
    /// <param name="tableName">Target table name.</param>
    /// <param name="database">Optional database name.</param>
    /// <param name="batchSizeLimit">Maximum events per batch (default: 100).</param>
    /// <param name="flushInterval">Time between flushes (default: 5 seconds).</param>
    /// <param name="queueLimit">Maximum events in queue (default: 100,000).</param>
    /// <param name="tableCreation">Table creation mode (default: CreateIfNotExists).</param>
    /// <param name="minimumLevel">Minimum log level (default: Verbose).</param>
    /// <param name="formatProvider">Format provider for message rendering.</param>
    /// <param name="onBatchWritten">Callback invoked after successful batch write.</param>
    /// <param name="onBatchFailed">Callback invoked when batch write fails.</param>
    public static LoggerConfiguration ClickHouse(
        this LoggerSinkConfiguration loggerConfiguration,
        string connectionString,
        string tableName,
        string? database = null,
        int batchSizeLimit = DefaultBatchSizeLimit,
        TimeSpan? flushInterval = null,
        int queueLimit = DefaultQueueLimit,
        TableCreationMode tableCreation = TableCreationMode.CreateIfNotExists,
        LogEventLevel minimumLevel = LogEventLevel.Verbose,
        IFormatProvider? formatProvider = null,
        Action<int, TimeSpan>? onBatchWritten = null,
        Action<Exception, int>? onBatchFailed = null)
    {
        var options = CreateOptions(tableName, database, tableCreation, minimumLevel, formatProvider, onBatchWritten, onBatchFailed, connectionString);

        return loggerConfiguration.ClickHouse(options, CreateBatchingOptions(batchSizeLimit, flushInterval, queueLimit));
    }

    /// <summary>
    /// Writes log events to a ClickHouse database with a custom schema builder.
    /// </summary>
    public static LoggerConfiguration ClickHouse(
        this LoggerSinkConfiguration loggerConfiguration,
        string connectionString,
        Action<SchemaBuilder> configureSchema,
        int batchSizeLimit = DefaultBatchSizeLimit,
        TimeSpan? flushInterval = null,
        int queueLimit = DefaultQueueLimit,
        TableCreationMode tableCreation = TableCreationMode.CreateIfNotExists,
        LogEventLevel minimumLevel = LogEventLevel.Verbose,
        IFormatProvider? formatProvider = null,
        Action<int, TimeSpan>? onBatchWritten = null,
        Action<Exception, int>? onBatchFailed = null)
    {
        ArgumentNullException.ThrowIfNull(configureSchema);

        var schemaBuilder = new SchemaBuilder();
        configureSchema(schemaBuilder);

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = connectionString,
            Schema = schemaBuilder.Build(),
            TableCreation = new TableCreationOptions
            {
                Mode = tableCreation,
            },
            MinimumLevel = minimumLevel,
            FormatProvider = formatProvider,
            OnBatchWritten = onBatchWritten,
            OnBatchFailed = onBatchFailed,
        };

        return loggerConfiguration.ClickHouse(options, CreateBatchingOptions(batchSizeLimit, flushInterval, queueLimit));
    }
    
    /// <summary>
    /// Writes log events to a ClickHouse database using client settings and the default schema.
    /// </summary>
    /// <param name="loggerConfiguration">The logger configuration.</param>
    /// <param name="settings">ClickHouse client settings.</param>
    /// <param name="tableName">Target table name.</param>
    /// <param name="database">Optional database name.</param>
    /// <param name="batchSizeLimit">Maximum events per batch (default: 100).</param>
    /// <param name="flushInterval">Time between flushes (default: 5 seconds).</param>
    /// <param name="queueLimit">Maximum events in queue (default: 100,000).</param>
    /// <param name="tableCreation">Table creation mode (default: CreateIfNotExists).</param>
    /// <param name="minimumLevel">Minimum log level (default: Verbose).</param>
    /// <param name="formatProvider">Format provider for message rendering.</param>
    /// <param name="onBatchWritten">Callback invoked after successful batch write.</param>
    /// <param name="onBatchFailed">Callback invoked when batch write fails.</param>
    public static LoggerConfiguration ClickHouse(
        this LoggerSinkConfiguration loggerConfiguration,
        ClickHouseClientSettings settings,
        string tableName,
        string? database = null,
        int batchSizeLimit = DefaultBatchSizeLimit,
        TimeSpan? flushInterval = null,
        int queueLimit = DefaultQueueLimit,
        TableCreationMode tableCreation = TableCreationMode.CreateIfNotExists,
        LogEventLevel minimumLevel = LogEventLevel.Verbose,
        IFormatProvider? formatProvider = null,
        Action<int, TimeSpan>? onBatchWritten = null,
        Action<Exception, int>? onBatchFailed = null)
    {
        ArgumentNullException.ThrowIfNull(loggerConfiguration);
        ArgumentNullException.ThrowIfNull(settings);

        var client = new ClickHouseClient(settings);
        var options = CreateOptions(tableName, database, tableCreation, minimumLevel, formatProvider, onBatchWritten, onBatchFailed);
        options.Validate();

        // The sink owns the client since we created it here.
        var sink = new ClickHouseSink(options, client, ownsClient: true);
        var batchingOptions = CreateBatchingOptions(batchSizeLimit, flushInterval, queueLimit);

        return loggerConfiguration.Sink(sink, batchingOptions, options.MinimumLevel);
    }

    /// <summary>
    /// Writes log events to a ClickHouse database using a pre-built client and the default schema.
    /// </summary>
    /// <param name="loggerConfiguration">The logger configuration.</param>
    /// <param name="client">A pre-built ClickHouse client.</param>
    /// <param name="tableName">Target table name.</param>
    /// <param name="database">Optional database name.</param>
    /// <param name="batchSizeLimit">Maximum events per batch (default: 100).</param>
    /// <param name="flushInterval">Time between flushes (default: 5 seconds).</param>
    /// <param name="queueLimit">Maximum events in queue (default: 100,000).</param>
    /// <param name="tableCreation">Table creation mode (default: CreateIfNotExists).</param>
    /// <param name="minimumLevel">Minimum log level (default: Verbose).</param>
    /// <param name="formatProvider">Format provider for message rendering.</param>
    /// <param name="onBatchWritten">Callback invoked after successful batch write.</param>
    /// <param name="onBatchFailed">Callback invoked when batch write fails.</param>
    public static LoggerConfiguration ClickHouse(
        this LoggerSinkConfiguration loggerConfiguration,
        IClickHouseClient client,
        string tableName,
        string? database = null,
        int batchSizeLimit = DefaultBatchSizeLimit,
        TimeSpan? flushInterval = null,
        int queueLimit = DefaultQueueLimit,
        TableCreationMode tableCreation = TableCreationMode.CreateIfNotExists,
        LogEventLevel minimumLevel = LogEventLevel.Verbose,
        IFormatProvider? formatProvider = null,
        Action<int, TimeSpan>? onBatchWritten = null,
        Action<Exception, int>? onBatchFailed = null)
    {
        ArgumentNullException.ThrowIfNull(client);

        var options = CreateOptions(tableName, database, tableCreation, minimumLevel, formatProvider, onBatchWritten, onBatchFailed);

        return loggerConfiguration.ClickHouse(options, client, CreateBatchingOptions(batchSizeLimit, flushInterval, queueLimit));
    }

#if NET7_0_OR_GREATER
    /// <summary>
    /// Writes log events to a ClickHouse database using a data source and the default schema.
    /// </summary>
    /// <param name="loggerConfiguration">The logger configuration.</param>
    /// <param name="dataSource">A pre-built ClickHouse data source.</param>
    /// <param name="tableName">Target table name.</param>
    /// <param name="database">Optional database name.</param>
    /// <param name="batchSizeLimit">Maximum events per batch (default: 100).</param>
    /// <param name="flushInterval">Time between flushes (default: 5 seconds).</param>
    /// <param name="queueLimit">Maximum events in queue (default: 100,000).</param>
    /// <param name="tableCreation">Table creation mode (default: CreateIfNotExists).</param>
    /// <param name="minimumLevel">Minimum log level (default: Verbose).</param>
    /// <param name="formatProvider">Format provider for message rendering.</param>
    /// <param name="onBatchWritten">Callback invoked after successful batch write.</param>
    /// <param name="onBatchFailed">Callback invoked when batch write fails.</param>
    public static LoggerConfiguration ClickHouse(
        this LoggerSinkConfiguration loggerConfiguration,
        ClickHouseDataSource dataSource,
        string tableName,
        string? database = null,
        int batchSizeLimit = DefaultBatchSizeLimit,
        TimeSpan? flushInterval = null,
        int queueLimit = DefaultQueueLimit,
        TableCreationMode tableCreation = TableCreationMode.CreateIfNotExists,
        LogEventLevel minimumLevel = LogEventLevel.Verbose,
        IFormatProvider? formatProvider = null,
        Action<int, TimeSpan>? onBatchWritten = null,
        Action<Exception, int>? onBatchFailed = null)
    {
        ArgumentNullException.ThrowIfNull(dataSource);

        var options = CreateOptions(tableName, database, tableCreation, minimumLevel, formatProvider, onBatchWritten, onBatchFailed);

        return loggerConfiguration.ClickHouse(options, dataSource, CreateBatchingOptions(batchSizeLimit, flushInterval, queueLimit));
    }
#endif
}
