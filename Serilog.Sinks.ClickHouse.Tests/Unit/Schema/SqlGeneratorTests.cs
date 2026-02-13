using Serilog.Sinks.ClickHouse.ColumnWriters;
using Serilog.Sinks.ClickHouse.Schema;

namespace Serilog.Sinks.ClickHouse.Tests.Unit.Schema;

public class SqlGeneratorTests
{
    [Test]
    public void EscapeIdentifier_ReturnsUnquoted_ForSimpleIdentifier()
    {
        var result = SqlGenerator.EscapeIdentifier("column_name");
        Assert.That(result, Is.EqualTo("column_name"));
    }

    [Test]
    public void EscapeIdentifier_QuotesWithBackticks_ForIdentifierWithSpaces()
    {
        var result = SqlGenerator.EscapeIdentifier("column name");
        Assert.That(result, Is.EqualTo("`column name`"));
    }

    [Test]
    public void EscapeIdentifier_EscapesBackticks_InIdentifier()
    {
        var result = SqlGenerator.EscapeIdentifier("column`name");
        Assert.That(result, Is.EqualTo("`column``name`"));
    }

    [Test]
    public void EscapeIdentifier_ThrowsOnEmptyString()
    {
        Assert.Throws<ArgumentException>(() => SqlGenerator.EscapeIdentifier(""));
    }

    [Test]
    public void EscapeTableName_HandlesSimpleName()
    {
        var result = SqlGenerator.EscapeTableName("logs");
        Assert.That(result, Is.EqualTo("logs"));
    }

    [Test]
    public void EscapeTableName_HandlesDatabaseDotTable()
    {
        var result = SqlGenerator.EscapeTableName("mydb.logs");
        Assert.That(result, Is.EqualTo("mydb.logs"));
    }

    [Test]
    public void EscapeTableName_HandlesSpecialCharacters()
    {
        var result = SqlGenerator.EscapeTableName("my-db.my-logs");
        Assert.That(result, Is.EqualTo("`my-db`.`my-logs`"));
    }

    [Test]
    public void EscapeString_EscapesSingleQuotes()
    {
        var result = SqlGenerator.EscapeString("It's a test");
        Assert.That(result, Is.EqualTo("It\\'s a test"));
    }

    [Test]
    public void EscapeString_EscapesBackslashes()
    {
        var result = SqlGenerator.EscapeString("C:\\path\\to\\file");
        Assert.That(result, Is.EqualTo("C:\\\\path\\\\to\\\\file"));
    }

    [Test]
    public void GenerateCreateTable_GeneratesValidSql()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn()
            .AddLevelColumn()
            .AddMessageColumn()
            .Build();

        var sql = SqlGenerator.GenerateCreateTable(schema);

        Assert.That(sql, Does.Contain("CREATE TABLE IF NOT EXISTS logs"));
        Assert.That(sql, Does.Contain("timestamp DateTime64(3)"));
        Assert.That(sql, Does.Contain("level LowCardinality(String)"));
        Assert.That(sql, Does.Contain("message String"));
    }

    [Test]
    public void GenerateCreateTable_IncludesDatabase_WhenSpecified()
    {
        var schema = new SchemaBuilder()
            .WithDatabase("mydb")
            .WithTableName("logs")
            .AddTimestampColumn()
            .Build();

        var sql = SqlGenerator.GenerateCreateTable(schema);

        Assert.That(sql, Does.Contain("CREATE TABLE IF NOT EXISTS mydb.logs"));
    }

    [Test]
    public void GenerateCreateTable_IncludesComment_WhenSpecified()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn()
            .WithComment("Application logs")
            .Build();

        var sql = SqlGenerator.GenerateCreateTable(schema);

        Assert.That(sql, Does.Contain("COMMENT 'Application logs'"));
    }

    [Test]
    public void GenerateCreateTable_IncludesTtl_WhenSpecified()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn()
            .WithEngine(new CustomEngine("ENGINE = MergeTree() ORDER BY (timestamp)\nTTL timestamp + INTERVAL 30 DAY"))
            .Build();

        var sql = SqlGenerator.GenerateCreateTable(schema);

        Assert.That(sql, Does.Contain("TTL timestamp + INTERVAL 30 DAY"));
    }

    [Test]
    public void GenerateDropTable_GeneratesValidSql()
    {
        var schema = new SchemaBuilder()
            .WithDatabase("mydb")
            .WithTableName("logs")
            .AddTimestampColumn()
            .Build();

        var sql = SqlGenerator.GenerateDropTable(schema);

        Assert.That(sql, Is.EqualTo("DROP TABLE IF EXISTS mydb.logs"));
    }

    [Test]
    public void GenerateExistsQuery_GeneratesValidSql()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn()
            .Build();

        var sql = SqlGenerator.GenerateExistsQuery(schema);

        Assert.That(sql, Is.EqualTo("EXISTS logs"));
    }

    [Test]
    public void GenerateCreateTable_ThrowsWhenColumnHasNoType()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn()
            .AddPropertyColumn("UserId")
            .Build();

        var ex = Assert.Throws<InvalidOperationException>(
            () => SqlGenerator.GenerateCreateTable(schema));

        Assert.That(ex!.Message, Does.Contain("UserId"));
        Assert.That(ex.Message, Does.Contain("no ColumnType"));
    }

    [Test]
    public void GenerateCreateTable_ThrowsWithAllMissingColumnNames()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddPropertyColumn("UserId")
            .AddPropertyColumn("RequestPath")
            .Build();

        var ex = Assert.Throws<InvalidOperationException>(
            () => SqlGenerator.GenerateCreateTable(schema));

        Assert.That(ex!.Message, Does.Contain("UserId"));
        Assert.That(ex.Message, Does.Contain("RequestPath"));
    }

    [Test]
    public void GenerateCreateTable_SucceedsWhenAllColumnsHaveTypes()
    {
        var schema = new SchemaBuilder()
            .WithTableName("logs")
            .AddTimestampColumn()
            .AddPropertyColumn("UserId", type: "Int64")
            .Build();

        Assert.DoesNotThrow(() => SqlGenerator.GenerateCreateTable(schema));
    }
}
