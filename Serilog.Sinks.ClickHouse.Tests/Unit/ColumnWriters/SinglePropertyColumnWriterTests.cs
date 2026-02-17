using ClickHouse.Driver.Constraints;
using Serilog.Sinks.ClickHouse.ColumnWriters;
using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Unit.ColumnWriters;

public class SinglePropertyColumnWriterTests
{
    [Test]
    public void GetValue_ReturnsPropertyValue_UsingToStringMethod()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .WithProperty("UserId", 123)
            .Build();

        var writer = new SinglePropertyColumnWriter("UserId", writeMethod: PropertyWriteMethod.ToString);

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo("123"));
    }

    [Test]
    public void GetValue_ReturnsRawValue_UsingRawMethod()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .WithProperty("UserId", 123)
            .Build();

        var writer = new SinglePropertyColumnWriter("UserId", writeMethod: PropertyWriteMethod.Raw);

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo(123));
        Assert.That(result, Is.InstanceOf<int>());
    }

    [Test]
    public void GetValue_ReturnsJsonValue_UsingJsonMethod()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .WithProperty("UserId", 123)
            .Build();

        var writer = new SinglePropertyColumnWriter("UserId", writeMethod: PropertyWriteMethod.Json);

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo("123"));
    }

    [Test]
    public void GetValue_ReturnsJsonForString_UsingJsonMethod()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .WithProperty("Name", "John")
            .Build();

        var writer = new SinglePropertyColumnWriter("Name", writeMethod: PropertyWriteMethod.Json);

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo("\"John\"")); // JSON string includes quotes
    }

    [Test]
    public void GetValue_ReturnsDBDefault_WhenPropertyNotFound()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .Build();

        var writer = new SinglePropertyColumnWriter("NonExistent", columnType: "Nullable(Int32)");

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.SameAs(DBDefault.Value));
    }

    [Test]
    public void GetValue_ReturnsDBDefault_ForNonNullableType_WhenPropertyNotFound()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .Build();

        var writer = new SinglePropertyColumnWriter("NonExistent", columnType: "Int32");

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.SameAs(DBDefault.Value));
    }

    [Test]
    public void Constructor_UsesPropertyNameAsColumnName_WhenNotSpecified()
    {
        var writer = new SinglePropertyColumnWriter("UserId");

        Assert.That(writer.ColumnName, Is.EqualTo("UserId"));
        Assert.That(writer.PropertyName, Is.EqualTo("UserId"));
    }

    [Test]
    public void Constructor_AllowsDifferentColumnName()
    {
        var writer = new SinglePropertyColumnWriter("UserId", columnName: "user_id");

        Assert.That(writer.ColumnName, Is.EqualTo("user_id"));
        Assert.That(writer.PropertyName, Is.EqualTo("UserId"));
    }

    [Test]
    public void Constructor_ThrowsOnNullPropertyName()
    {
        Assert.Throws<ArgumentNullException>(() => new SinglePropertyColumnWriter(null!));
    }

    [Test]
    public void GetValue_HandlesGuidProperty_WithRawMethod()
    {
        var guid = Guid.NewGuid();
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .WithProperty("CorrelationId", guid)
            .Build();

        var writer = new SinglePropertyColumnWriter("CorrelationId", writeMethod: PropertyWriteMethod.Raw);

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo(guid));
    }

    [Test]
    public void GetValue_HandlesDateTimeProperty_WithRawMethod()
    {
        var dateTime = new DateTime(2024, 6, 15, 14, 30, 0);
        var logEvent = new LogEventBuilder()
            .WithMessage("Test")
            .WithProperty("EventTime", dateTime)
            .Build();

        var writer = new SinglePropertyColumnWriter("EventTime", writeMethod: PropertyWriteMethod.Raw);

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.EqualTo(dateTime));
    }

    [Test]
    public void Constructor_ColumnTypeIsNull_WhenNotSpecified()
    {
        var writer = new SinglePropertyColumnWriter("UserId");

        Assert.That(writer.ColumnType, Is.Null);
    }

    [Test]
    public void Constructor_ColumnTypeIsSet_WhenSpecified()
    {
        var writer = new SinglePropertyColumnWriter("UserId", columnType: "Int64");

        Assert.That(writer.ColumnType, Is.EqualTo("Int64"));
    }
}
