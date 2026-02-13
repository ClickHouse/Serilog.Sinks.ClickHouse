# About

A [Serilog](https://serilog.net/) sink that writes structured log events to [ClickHouse](https://clickhouse.com/). Events are batched for efficient bulk inserts. Optionally, the library handles table creation automatically.

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

### Preset Schemas

```csharp
// Default — the 6-column schema shown above
var schema = DefaultSchema.Create("app_logs").Build();

// Minimal — just timestamp, level (numeric), and message
var schema = DefaultSchema.CreateMinimal("app_logs").Build();

// Comprehensive — default + full log event JSON
var schema = DefaultSchema.CreateComprehensive("app_logs").Build();

// Start from a preset and add custom columns
// Each preset returns a SchemaBuilder, which can be further customized before calling .Build()
var schema = DefaultSchema.Create("app_logs")
    .AddPropertyColumn("UserId", "Nullable(Int64)")
    .AddPropertyColumn("RequestPath", "Nullable(String)")
    .Build();
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