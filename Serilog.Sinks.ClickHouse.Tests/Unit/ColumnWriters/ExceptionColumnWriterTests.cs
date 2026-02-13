using Serilog.Sinks.ClickHouse.ColumnWriters;
using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Unit.ColumnWriters;

public class ExceptionColumnWriterTests
{
    [Test]
    public void GetValue_ReturnsExceptionString_WhenExceptionPresent()
    {
        var exception = new InvalidOperationException("Test exception message");
        var logEvent = new LogEventBuilder()
            .WithException(exception)
            .Build();

        var writer = new ExceptionColumnWriter();

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.Not.Null);
        Assert.That(result, Is.InstanceOf<string>());
        Assert.That((string)result!, Does.Contain("InvalidOperationException"));
        Assert.That((string)result!, Does.Contain("Test exception message"));
    }

    [Test]
    public void GetValue_ReturnsNull_WhenNoException()
    {
        var logEvent = new LogEventBuilder()
            .WithMessage("No exception here")
            .Build();

        var writer = new ExceptionColumnWriter();

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void GetValue_IncludesInnerException_InOutput()
    {
        var innerException = new ArgumentException("Inner message");
        var outerException = new InvalidOperationException("Outer message", innerException);
        var logEvent = new LogEventBuilder()
            .WithException(outerException)
            .Build();

        var writer = new ExceptionColumnWriter();

        var result = writer.GetValue(logEvent);

        Assert.That(result, Is.Not.Null);
        Assert.That((string)result!, Does.Contain("Outer message"));
        Assert.That((string)result!, Does.Contain("Inner message"));
        Assert.That((string)result!, Does.Contain("ArgumentException"));
    }

    [Test]
    public void Constructor_UsesNullableStringType_ByDefault()
    {
        var writer = new ExceptionColumnWriter();
        Assert.That(writer.ColumnType, Is.EqualTo("Nullable(String)"));
    }

    [Test]
    public void Constructor_DefaultColumnNameIsException()
    {
        var writer = new ExceptionColumnWriter();
        Assert.That(writer.ColumnName, Is.EqualTo("exception"));
    }
}
