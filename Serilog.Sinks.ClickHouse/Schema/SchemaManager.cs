using ClickHouse.Driver;
using Serilog.Debugging;
using Serilog.Sinks.ClickHouse.Schema;

namespace Serilog.Sinks.ClickHouse.Client;

/// <summary>
/// Table creation mode options.
/// </summary>
public enum TableCreationMode
{
    /// <summary>
    /// Create the table if it doesn't exist (default, safe, idempotent).
    /// </summary>
    CreateIfNotExists,

    /// <summary>
    /// Don't create the table - assume it exists (for managed schemas).
    /// </summary>
    None,

    /// <summary>
    /// Drop and recreate the table (dangerous! for dev/testing only).
    /// </summary>
    DropAndRecreate,
}

/// <summary>
/// Options for table creation and management.
/// </summary>
public sealed record TableCreationOptions
{
    /// <summary>
    /// How to handle table creation.
    /// </summary>
    public TableCreationMode Mode { get; init; } = TableCreationMode.CreateIfNotExists;

    /// <summary>
    /// Whether to validate the table exists on startup.
    /// </summary>
    public bool ValidateOnStartup { get; init; } = true;
}

/// <summary>
/// Manages table schema creation and validation.
/// </summary>
public sealed class SchemaManager
{
    private readonly IClickHouseClient _client;

    public SchemaManager(IClickHouseClient client)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
    }

    /// <summary>
    /// Ensures the table is ready based on the creation options.
    /// </summary>
    public async Task EnsureTableAsync(
        TableSchema schema,
        TableCreationOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(options);

        schema.Validate();

        switch (options.Mode)
        {
            case TableCreationMode.CreateIfNotExists:
                await CreateTableIfNotExistsAsync(schema, cancellationToken).ConfigureAwait(false);
                break;

            case TableCreationMode.DropAndRecreate:
                await DropAndRecreateTableAsync(schema, cancellationToken).ConfigureAwait(false);
                break;

            case TableCreationMode.None:
                if (options.ValidateOnStartup)
                {
                    await ValidateTableExistsAsync(schema, cancellationToken).ConfigureAwait(false);
                }
                break;
        }
    }

    /// <summary>
    /// Creates the table if it doesn't exist.
    /// </summary>
    public async Task CreateTableIfNotExistsAsync(TableSchema schema, CancellationToken cancellationToken = default)
    {
        try
        {
            var sql = SqlGenerator.GenerateCreateTable(schema);
            await _client.ExecuteNonQueryAsync(sql, cancellationToken: cancellationToken).ConfigureAwait(false);
            SelfLog.WriteLine("Created or verified table {0}", schema.FullTableName);
        }
        catch (Exception ex)
        {
            SelfLog.WriteLine("Failed to create table {0}: {1}", schema.FullTableName, ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Drops and recreates the table. Use with caution!
    /// </summary>
    public async Task DropAndRecreateTableAsync(TableSchema schema, CancellationToken cancellationToken = default)
    {
        try
        {
            var dropSql = SqlGenerator.GenerateDropTable(schema);
            await _client.ExecuteNonQueryAsync(dropSql, cancellationToken: cancellationToken).ConfigureAwait(false);
            SelfLog.WriteLine("Dropped table {0}", schema.FullTableName);

            var createSql = SqlGenerator.GenerateCreateTable(schema);
            await _client.ExecuteNonQueryAsync(createSql, cancellationToken: cancellationToken).ConfigureAwait(false);
            SelfLog.WriteLine("Created table {0}", schema.FullTableName);
        }
        catch (Exception ex)
        {
            SelfLog.WriteLine("Failed to drop/recreate table {0}: {1}", schema.FullTableName, ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Validates that the table exists.
    /// </summary>
    public async Task ValidateTableExistsAsync(TableSchema schema, CancellationToken cancellationToken = default)
    {
        var result = await _client.ExecuteScalarAsync(
            $"EXISTS {SqlGenerator.EscapeTableName(schema.FullTableName)}",
            cancellationToken: cancellationToken).ConfigureAwait(false);

        var exists = result is (byte)1;
        if (!exists)
        {
            throw new InvalidOperationException(
                $"Table '{schema.FullTableName}' does not exist. " +
                $"Create the table manually or set TableCreationMode to CreateIfNotExists.");
        }
    }
}
