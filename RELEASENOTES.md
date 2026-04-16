## v2.2.0

- Column-level DDL options: `codec`, `defaultExpression`, `ttl`, and `comment` on all `Add*Column` builder methods, maps to ClickHouse `CODEC(...)`, `DEFAULT expr`, `TTL expr`, and `COMMENT 'text'` clauses
- `SchemaBuilder.OnCluster()`: adds `ON CLUSTER cluster_name` to `CREATE TABLE` and `DROP TABLE` DDL for distributed ClickHouse deployments

## v2.1.0

- `SchemaBuilder.AddIndex()` — add ClickHouse data-skipping indexes to auto-created tables (e.g. `set`, `minmax`, `tokenbf_v1`)

## v2.0.0

Complete rewrite of the ClickHouse Serilog sink, built on top of the official [ClickHouse.Driver](https://github.com/ClickHouse/clickhouse-cs).

- New `IBatchedLogEventSink` implementation — batching is fully delegated to Serilog
- Fluent schema builder API for custom column layouts, table engines, and TTL
- Built-in column writers for timestamp, level, message, exception, properties, and full log event JSON
- `SinglePropertyColumnWriter` for extracting individual enriched properties into dedicated typed columns
- Properties stored as native ClickHouse `JSON` with dot-notation query support
- Automatic table creation with `CreateIfNotExists`, `DropAndRecreate`, and `None` modes
- Per-column error isolation — failed columns get default values instead of dropping the entire event
- DI-friendly overloads for `ClickHouseClient`, `ClickHouseDataSource` (.NET 7+), and `ClickHouseClientSettings`
- Observability callbacks (`OnBatchWritten`, `OnBatchFailed`) for metrics and alerting
- Targets net6.0, net8.0, net9.0, net10.0

See the [README](https://github.com/ClickHouse/Serilog.Sinks.ClickHouse) for usage and configuration.
