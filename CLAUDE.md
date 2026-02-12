# CLAUDE.md — Serilog.Sinks.ClickHouse

## Project Overview

A Serilog sink that writes structured log events to ClickHouse using the official `ClickHouse.Driver`. Implements `IBatchedLogEventSink` to delegate batching to Serilog. Features automatic schema management, a fluent column/schema builder API, DI support, and per-column error isolation.

**Owner:** ClickHouse
**License:** Apache-2.0
**Repo:** https://github.com/ClickHouse/Serilog.Sinks.ClickHouse

## Build & Test Commands

```bash
# Restore
dotnet restore

# Build (all target frameworks)
dotnet build --configuration Release

# Build for a specific framework
dotnet build --configuration Release --framework net8.0

# Run all tests (requires Docker for integration tests)
dotnet test --configuration Release --framework net8.0

# Run unit tests only (no Docker needed)
dotnet test --configuration Release --framework net8.0 --filter "Category!=Integration"

# Run integration tests only (requires Docker)
dotnet test --configuration Release --framework net8.0 --filter "Category=Integration"
```

## Target Frameworks

net6.0, net8.0, net9.0, net10.0 (CI also tests net7.0). Conditional compilation via `#if NET7_0_OR_GREATER` for `ClickHouseDataSource` DI support.

## Project Structure

```
Serilog.Sinks.ClickHouse/           # Main library
├── ClickHouseSink.cs                # Core sink (IBatchedLogEventSink)
├── Configuration/
│   ├── ClickHouseSinkExtensions.cs  # LoggerSinkConfiguration extension methods
│   └── ClickHouseSinkOptions.cs     # Sealed config record with validation
├── ColumnWriters/
│   ├── ColumnWriterBase.cs          # Abstract base — subclass GetValue()
│   ├── StandardColumnWriters.cs     # Timestamp, Level, Message, Exception
│   ├── PropertiesColumnWriter.cs    # All enrichment properties as JSON
│   ├── LogEventColumnWriter.cs      # Full event as JSON
│   └── SinglePropertyColumnWriter.cs
├── Schema/
│   ├── SchemaBuilder.cs             # Fluent builder for TableSchema
│   ├── TableSchema.cs               # Immutable schema record
│   ├── DefaultSchema.cs             # Presets: Default, Minimal, Comprehensive
│   ├── SchemaManager.cs             # Table creation & validation
│   ├── SqlGenerator.cs              # SQL generation with escaping
│   └── TableEngine.cs              # MergeTree engine abstraction

Serilog.Sinks.ClickHouse.Tests/     # Test project
├── Fixtures/
│   ├── ClickHouseFixture.cs         # Testcontainers ClickHouse lifecycle
│   └── LogEventBuilder.cs           # Fluent test log event builder
├── Integration/                     # Tests requiring Docker + ClickHouse
└── Unit/                            # NSubstitute mock-based tests
    ├── ColumnWriters/
    └── Schema/
```

## Key Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| ClickHouse.Driver | 1.0.0 | ADO.NET ClickHouse driver (local source in nuget.config) |
| Serilog | 4.3.1 | Logging framework + batching via IBatchedLogEventSink |
| NUnit | 4.3.2 | Test framework |
| NSubstitute | 5.3.0 | Mocking |
| Testcontainers.ClickHouse | 4.10.0 | Docker containers for integration tests |

**Note:** `ClickHouse.Driver` is referenced from a local path (`/home/alex/Code/clickhouse-cs3/ClickHouse.Driver/bin/Release`) in `nuget.config` for development.

## Code Conventions

- **C# latest** with nullable reference types enabled, implicit usings
- **Naming:** PascalCase public members, `_camelCase` private fields
- **Records** for immutable config/schema types (sealed records with init-only properties)
- **Builder pattern** for schema construction (`SchemaBuilder`) and test data (`LogEventBuilder`)
- **Strategy pattern** for column writers — extend `ColumnWriterBase`, implement `GetValue()`
- **Error handling:** Per-column isolation — failed columns get `DBDefault.Value`, logged via `SelfLog`
- **Validation:** `ArgumentNullException.ThrowIfNull()` for null guards; `.Validate()` methods on options
- **Code analysis:** `EnforceCodeStyleInBuild=true`, `AnalysisMode=Recommended`, `AnalysisLevel=latest`
- XML doc comments on all public APIs

## Architecture Notes

- **Batching** is handled entirely by Serilog's `IBatchedLogEventSink` — no custom queue logic
- **Lazy table creation** — `EnsureTableAsync()` runs on first batch emit, not at startup
- **Bulk inserts** use `RowBinaryWithDefaults` format via `ClickHouseBulkCopy`
- **Callbacks** (`OnBatchWritten`, `OnBatchFailed`) for observability — no built-in retry
- **IClickHouseClient** interface enables unit testing with mocks
- **SQL injection prevention** via `SqlGenerator` with regex identifier validation and backtick quoting
- **Integration tests** use TestContainers to run ClickHouse

## CI/CD

- **Tests:** GitHub Actions matrix across net6.0–net10.0 on ubuntu-latest, fail-fast disabled
- **Release:** Manual workflow_dispatch — build, DigiCert code signing, NuGet push, GitHub release
