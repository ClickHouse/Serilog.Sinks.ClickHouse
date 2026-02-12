# Serilog.Sinks.ClickHouse

A [Serilog](https://serilog.net/) sink that writes structured log events to [ClickHouse](https://clickhouse.com/). Events are batched for efficient bulk inserts, and the table is auto-created on first write.

```
dotnet add package Serilog.Sinks.ClickHouse
```

## Quick Start

```csharp
Log.Logger = new LoggerConfiguration()
    .WriteTo.ClickHouse(
        connectionString: "Host=localhost;Port=9000;Database=logs",
        tableName: "app_logs")
    .CreateLogger();

Log.Information("User {UserId} logged in from {IpAddress}", 123, "10.0.0.1");
```

This creates a table `app_logs` with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | `DateTime64(3)` | Event timestamp (UTC) |
| `level` | `LowCardinality(String)` | Log level (`Information`, `Warning`, etc.) |
| `message` | `String` | Rendered message with property values substituted |
| `message_template` | `String` | Raw Serilog message template |
| `exception` | `Nullable(String)` | `Exception.ToString()` or null |
| `properties` | `JSON` | All log event properties as native JSON |

All enriched properties are captured in the `properties` column and can be queried with ClickHouse's JSON dot notation:

```sql
SELECT * FROM app_logs WHERE properties.UserId = 123
```

## Batching

The sink uses Serilog's `BatchingOptions` to control buffer size and flush events. You can tune the defaults:

```csharp
.WriteTo.ClickHouse(
    connectionString: "Host=localhost;Port=9000;Database=logs",
    tableName: "app_logs",
    batchSizeLimit: 500,           // Max events per batch (default: 100)
    flushInterval: TimeSpan.FromSeconds(10),  // Time between flushes (default: 5s)
    queueLimit: 100_000)           // Max events in queue (default: 100,000)
```

## Custom Schema

Use the schema builder to control which columns are created, their names, types, and the table engine:

```csharp
.WriteTo.ClickHouse(
    connectionString: "Host=localhost;Port=9000;Database=logs",
    configureSchema: schema => schema
        .WithTableName("custom_logs")
        .AddTimestampColumn("event_time", precision: 6)
        .AddLevelColumn("severity", asString: true)
        .AddMessageColumn()
        .AddExceptionColumn()
        .AddPropertiesColumn()
        .AddPropertyColumn("UserId", "Nullable(Int64)", writeMethod: PropertyWriteMethod.Raw)
        .AddPropertyColumn("RequestPath", "Nullable(String)")
        .WithEngine(new CustomEngine(
            "ENGINE = MergeTree() ORDER BY (event_time) PARTITION BY toYYYYMM(event_time)")))
```

### Schema Builder Methods

| Method | Description |
|--------|-------------|
| `AddTimestampColumn(name, precision, useUtc)` | Event timestamp. Default: `DateTime64(3)`, UTC. |
| `AddLevelColumn(name, asString)` | Log level as `LowCardinality(String)` or `UInt8`. |
| `AddMessageColumn(name)` | Rendered message (properties substituted). |
| `AddMessageTemplateColumn(name)` | Raw message template. |
| `AddExceptionColumn(name)` | Exception string or null. |
| `AddPropertiesColumn(name)` | All properties as `JSON`. |
| `AddPropertiesColumn(name, columnType)` | Properties with a custom type string. |
| `AddPropertyColumn(property, type, ...)` | Single named property extracted into its own column. |
| `AddLogEventColumn(name)` | Entire log event serialized as JSON. |
| `AddColumn(columnWriter)` | Any custom `ColumnWriterBase` implementation. |

### Preset Schemas

```csharp
// Default — the 6-column schema shown above
DefaultSchema.Create("app_logs");

// Minimal — just timestamp, level (numeric), and message
DefaultSchema.CreateMinimal("app_logs");

// Comprehensive — default + full log event JSON
DefaultSchema.CreateComprehensive("app_logs");
```

## Extracting Individual Properties

Use `SinglePropertyColumnWriter` to pull specific enriched properties into dedicated typed columns. This is useful when you want to query or index a known property efficiently instead of parsing JSON.

```csharp
.AddPropertyColumn("UserId", "Nullable(Int64)", writeMethod: PropertyWriteMethod.Raw)
.AddPropertyColumn("RequestPath", "Nullable(String)")
.AddPropertyColumn("CorrelationId", "Nullable(UUID)")
```

Write methods:

| Method | Behavior |
|--------|----------|
| `PropertyWriteMethod.Raw` | Extracts the raw CLR value from `ScalarValue` (default) |
| `PropertyWriteMethod.ToString` | Calls `.ToString()` on the property value |
| `PropertyWriteMethod.Json` | Serializes the property as JSON |

If a log event doesn't contain the named property, the sink sends a ClickHouse column default (the field is skipped and ClickHouse applies the type's default value, e.g. `0` for `Int64`, `''` for `String`).

## Table Creation Modes

```csharp
.WriteTo.ClickHouse(
    connectionString: "...",
    tableName: "app_logs",
    tableCreation: TableCreationMode.CreateIfNotExists)  // default
```

| Mode | Behavior |
|------|----------|
| `CreateIfNotExists` | Runs `CREATE TABLE IF NOT EXISTS` on first batch (default, safe, idempotent) |
| `None` | Assumes the table already exists. Validates existence on startup by default. |
| `DropAndRecreate` | Drops and recreates the table. **Destroys data** — dev/testing only. |

## Callbacks

Hook into batch lifecycle for metrics or alerting:

```csharp
.WriteTo.ClickHouse(
    connectionString: "...",
    tableName: "app_logs",
    onBatchWritten: (count, duration) =>
        Console.WriteLine($"Wrote {count} events in {duration.TotalMilliseconds}ms"),
    onBatchFailed: (ex, count) =>
        Console.WriteLine($"Failed to write {count} events: {ex.Message}"))
```

## Full Options

For complete control, pass a `ClickHouseSinkOptions` directly:

```csharp
var options = new ClickHouseSinkOptions
{
    ConnectionString = "Host=localhost;Port=9000;Database=logs",
    Schema = DefaultSchema.Create("app_logs"),
    TableCreation = new TableCreationOptions
    {
        Mode = TableCreationMode.CreateIfNotExists,
        ValidateOnStartup = true
    },
    MinimumLevel = LogEventLevel.Information,
    FormatProvider = CultureInfo.InvariantCulture,
    OnBatchWritten = (count, duration) => { /* metrics */ },
    OnBatchFailed = (ex, count) => { /* alerting */ }
};

Log.Logger = new LoggerConfiguration()
    .WriteTo.ClickHouse(options)
    .CreateLogger();
```

## Dependency Injection / ASP.NET

If you already have a `ClickHouseClient`, `ClickHouseDataSource`, or `ClickHouseClientSettings` registered in your DI container, pass it directly instead of a connection string:

### Using `ClickHouseDataSource` (recommended for .NET 7+)

```csharp
var builder = WebApplication.CreateBuilder(args);

// Register the data source in DI
builder.Services.AddSingleton(_ =>
    new ClickHouseDataSource("Host=localhost;Port=9000;Database=logs"));

builder.Host.UseSerilog((context, services, loggerConfiguration) =>
{
    var dataSource = services.GetRequiredService<ClickHouseDataSource>();
    loggerConfiguration.WriteTo.ClickHouse(dataSource, tableName: "app_logs");
});
```

### Using `IClickHouseClient`

```csharp
builder.Host.UseSerilog((context, services, loggerConfiguration) =>
{
    var client = services.GetRequiredService<IClickHouseClient>();
    loggerConfiguration.WriteTo.ClickHouse(client, tableName: "app_logs");
});
```

### Using `ClickHouseClientSettings`

```csharp
builder.Host.UseSerilog((context, services, loggerConfiguration) =>
{
    var settings = services.GetRequiredService<ClickHouseClientSettings>();
    loggerConfiguration.WriteTo.ClickHouse(settings, tableName: "app_logs");
});
```

All DI overloads accept the same optional parameters (`database`, `batchSizeLimit`, `flushInterval`, etc.) as the connection-string overload. For full control, pass a `ClickHouseSinkOptions` alongside the client or data source:

```csharp
loggerConfiguration.WriteTo.ClickHouse(options, client);
loggerConfiguration.WriteTo.ClickHouse(options, dataSource); // .NET 7+
```

## Connection String

The sink uses [ClickHouse.Driver](https://github.com/ClickHouse/clickhouse-dotnet) for the connection. Connection string format:

```
Host=localhost;Port=9000;Database=logs;User=default;Password=
```

## Supported Frameworks

net6.0, net8.0, net9.0, net10.0

## License

Apache-2.0
