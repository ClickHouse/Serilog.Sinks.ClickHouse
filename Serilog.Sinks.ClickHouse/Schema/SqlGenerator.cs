using System.Text;
using System.Text.RegularExpressions;

namespace Serilog.Sinks.ClickHouse.Schema;

/// <summary>
/// Generates SQL statements for ClickHouse tables with proper escaping
/// to prevent SQL injection.
/// </summary>
public static class SqlGenerator
{
    // Valid identifier pattern: starts with letter or underscore, contains only alphanumeric and underscores
    private static readonly Regex ValidIdentifierPattern = new(
        @"^[a-zA-Z_][a-zA-Z0-9_]*$",
        RegexOptions.Compiled);

    /// <summary>
    /// Generates a CREATE TABLE IF NOT EXISTS statement for the given schema.
    /// </summary>
    public static string GenerateCreateTable(TableSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        schema.Validate();

        var columnsWithoutType = schema.Columns
            .Where(c => string.IsNullOrWhiteSpace(c.ColumnType))
            .Select(c => c.ColumnName)
            .ToList();

        if (columnsWithoutType.Count > 0)
        {
            throw new InvalidOperationException(
                $"Cannot generate CREATE TABLE: columns [{string.Join(", ", columnsWithoutType)}] " +
                $"have no ColumnType specified. Either specify a type or use TableCreationMode.None " +
                $"for externally managed schemas.");
        }

        var sb = new StringBuilder();

        sb.Append("CREATE TABLE IF NOT EXISTS ");
        sb.Append(EscapeTableName(schema.FullTableName));

        if (!string.IsNullOrEmpty(schema.ClusterName))
        {
            sb.Append(" ON CLUSTER ");
            sb.Append(EscapeIdentifier(schema.ClusterName));
        }

        sb.AppendLine(" (");

        var columns = schema.Columns.ToList();
        var hasIndexes = schema.Indexes is { Count: > 0 };

        for (var i = 0; i < columns.Count; i++)
        {
            var column = columns[i];
            sb.Append("    ");
            sb.Append(EscapeIdentifier(column.ColumnName));
            sb.Append(' ');
            sb.Append(column.ColumnType);

            if (!string.IsNullOrWhiteSpace(column.Default))
            {
                sb.Append(" DEFAULT ");
                sb.Append(column.Default);
            }

            if (!string.IsNullOrWhiteSpace(column.Comment))
            {
                sb.Append(" COMMENT '");
                sb.Append(EscapeString(column.Comment));
                sb.Append('\'');
            }

            if (!string.IsNullOrWhiteSpace(column.Codec))
            {
                sb.Append(" CODEC(");
                sb.Append(column.Codec);
                sb.Append(')');
            }

            if (!string.IsNullOrWhiteSpace(column.Ttl))
            {
                sb.Append(" TTL ");
                sb.Append(column.Ttl);
            }

            if (i < columns.Count - 1 || hasIndexes)
                sb.Append(',');

            sb.AppendLine();
        }

        if (hasIndexes)
        {
            for (var i = 0; i < schema.Indexes.Count; i++)
            {
                sb.Append("    ");
                sb.Append(schema.Indexes[i]);

                if (i < schema.Indexes.Count - 1)
                    sb.Append(',');

                sb.AppendLine();
            }
        }

        sb.AppendLine(")");
        sb.Append(schema.Engine.ToSql());

        if (!string.IsNullOrEmpty(schema.Comment))
        {
            sb.AppendLine();
            sb.Append("COMMENT '");
            sb.Append(EscapeString(schema.Comment));
            sb.Append('\'');
        }

        return sb.ToString();
    }

    /// <summary>
    /// Generates a DROP TABLE IF EXISTS statement.
    /// </summary>
    public static string GenerateDropTable(TableSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        var sql = $"DROP TABLE IF EXISTS {EscapeTableName(schema.FullTableName)}";

        if (!string.IsNullOrEmpty(schema.ClusterName))
            sql += $" ON CLUSTER {EscapeIdentifier(schema.ClusterName)}";

        // SYNC waits for Replicated tables to detach from Keeper before returning,
        // preventing REPLICA_ALREADY_EXISTS on drop-then-recreate. No-op for non-Replicated engines.
        sql += " SYNC";

        return sql;
    }

    /// <summary>
    /// Generates an EXISTS check query.
    /// </summary>
    public static string GenerateExistsQuery(TableSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return $"EXISTS {EscapeTableName(schema.FullTableName)}";
    }

    /// <summary>
    /// Escapes a table name for safe use in SQL.
    /// Handles both simple names and database.table format.
    /// </summary>
    public static string EscapeTableName(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name cannot be empty.", nameof(tableName));

        // Handle database.table format
        var parts = tableName.Split('.', 2);
        return parts.Length == 2
            ? $"{EscapeIdentifier(parts[0])}.{EscapeIdentifier(parts[1])}"
            : EscapeIdentifier(parts[0]);
    }

    /// <summary>
    /// Escapes an identifier (column name, table name) for safe use in SQL.
    /// Uses backticks for quoting.
    /// </summary>
    public static string EscapeIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("Identifier cannot be empty.", nameof(identifier));

        // If it's a simple valid identifier, return as-is (no quoting needed)
        if (ValidIdentifierPattern.IsMatch(identifier))
            return identifier;

        // Otherwise, quote with backticks and escape any backticks inside
        return $"`{identifier.Replace("`", "``")}`";
    }

    /// <summary>
    /// Escapes a string value for use in SQL.
    /// </summary>
    public static string EscapeString(string value)
    {
        if (value is null)
            return string.Empty;

        return value
            .Replace("\\", "\\\\")
            .Replace("'", "\\'");
    }
}
