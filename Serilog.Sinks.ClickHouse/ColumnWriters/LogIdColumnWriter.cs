using Serilog.Events;

namespace Serilog.Sinks.ClickHouse.ColumnWriters;


/// <summary>
/// Defines the strategy used to generate unique identifiers for log entries.
/// </summary>
public enum LogIdGenerator
{
    /// <summary>
    /// Generates a standard <see cref="Guid.NewGuid"/> in the C# application before writing to ClickHouse.
    /// Compatible with all ClickHouse versions.
    /// </summary>
    CSharpGuid,
    
    /// <summary>
    /// Relies on the ClickHouse built-in <c>generateUUIDv4()</c> function. 
    /// The identifier is generated automatically on the database side during insertion.
    /// </summary>
    ClickHouseUUIDv4,
    
    /// <summary>
    /// Relies on the ClickHouse built-in <c>generateUUIDv7()</c> function. 
    /// Generates time-sorted UUIDs on the database side. 
    /// Requires ClickHouse version 24.1 or higher.
    /// </summary>
    ClickHouseUUIDv7
}


/// <summary>
/// Writes the log id.
/// If <c>asString</c> is true, uses String. otherwise uses UUID (default).
/// If <c>columnType</c> is passed, it overrides the above.
/// </summary>
public class LogIdColumnWriter : ColumnWriterBase
{
    private readonly bool _asString;

    /// <inheritdoc />
    public LogIdColumnWriter(string columnName = "log_id", bool asString = false, string? columnType = null) 
        : base(columnName, columnType ?? (asString ? "String" : "UUID"))
    {
        _asString = asString;
    }

    /// <inheritdoc />
    public override object? GetValue(LogEvent logEvent, IFormatProvider? formatProvider = null)
    {
        if (SkipWrite)
        {
            return null; 
        }

        var guid = Guid.NewGuid();
        return _asString ? guid.ToString() : guid;
    }
}