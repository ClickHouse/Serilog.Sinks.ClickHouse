using ClickHouse.Driver;
using NSubstitute;
using Serilog.Sinks.ClickHouse.Client;
using Serilog.Sinks.ClickHouse.Configuration;
using Serilog.Sinks.ClickHouse.Schema;
using Serilog.Sinks.ClickHouse.Tests.Fixtures;

namespace Serilog.Sinks.ClickHouse.Tests.Unit;

/// <summary>
/// Unit tests for ClickHouseSink that test constructor validation,
/// dispose behavior, and error handling — cases where mocking is necessary.
/// Data-path tests are in Integration/ClickHouseSinkIntegrationTests.
/// </summary>
public class ClickHouseSinkTests
{
    private IClickHouseClient _mockClient = null!;
    private ClickHouseSinkOptions _defaultOptions = null!;

    [SetUp]
    public void SetUp()
    {
        _mockClient = Substitute.For<IClickHouseClient>();

        _defaultOptions = new ClickHouseSinkOptions
        {
            ConnectionString = "Host=localhost;Port=9000;",
            Schema = DefaultSchema.Create("test_logs"),
            TableCreation = new TableCreationOptions
            {
                Mode = TableCreationMode.None,
                ValidateOnStartup = false,
            },
        };
    }

    [TearDown]
    public void TearDown()
    {
        _mockClient?.Dispose();
    }

    [Test]
    public void Constructor_ThrowsOnNullOptions()
    {
        Assert.Throws<ArgumentNullException>(() => new ClickHouseSink(null!, _mockClient));
    }

    [Test]
    public void Constructor_ThrowsOnNullClient()
    {
        Assert.Throws<ArgumentNullException>(() => new ClickHouseSink(_defaultOptions, (IClickHouseClient)null!));
    }

    [Test]
    public void Constructor_ThrowsOnInvalidOptions()
    {
        var invalidOptions = new ClickHouseSinkOptions
        {
            ConnectionString = "",
            Schema = DefaultSchema.Create("test"),
        };

        Assert.Throws<InvalidOperationException>(() => new ClickHouseSink(invalidOptions, _mockClient));
    }

    [Test]
    public async Task EmitBatchAsync_DoesNotWrite_WhenDisposed()
    {
        var sink = new ClickHouseSink(_defaultOptions, _mockClient);
        sink.Dispose();

        await sink.EmitBatchAsync(new[] { new LogEventBuilder().WithMessage("Test").Build() });

        await _mockClient.DidNotReceive().InsertBinaryAsync(
            Arg.Any<string>(),
            Arg.Any<IEnumerable<string>>(),
            Arg.Any<IEnumerable<object[]>>(),
            Arg.Any<InsertOptions>(),
            cancellationToken: Arg.Any<CancellationToken>());
    }

    [Test]
    public void Dispose_DisposesClient()
    {
        var sink = new ClickHouseSink(_defaultOptions, _mockClient);
        sink.Dispose();
        _mockClient.Received(1).Dispose();
    }

    [Test]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var sink = new ClickHouseSink(_defaultOptions, _mockClient);
        sink.Dispose();
        sink.Dispose();
        _mockClient.Received(1).Dispose();
    }

    [Test]
    public async Task EmitBatchAsync_InvokesOnBatchFailed_OnError()
    {
        Exception? capturedException = null;
        int? capturedCount = null;

        var expectedException = new InvalidOperationException("Database error");
        _mockClient.InsertBinaryAsync(
                Arg.Any<string>(),
                Arg.Any<IEnumerable<string>>(),
                Arg.Any<IEnumerable<object[]>>(),
                Arg.Any<InsertOptions>(),
                cancellationToken: Arg.Any<CancellationToken>())
            .Returns(Task.FromException<long>(expectedException));

        var options = new ClickHouseSinkOptions
        {
            ConnectionString = "Host=localhost;Port=9000;",
            Schema = DefaultSchema.Create("test_logs"),
            TableCreation = new TableCreationOptions
            {
                Mode = TableCreationMode.None,
                ValidateOnStartup = false,
            },
            OnBatchFailed = (ex, count) =>
            {
                capturedException = ex;
                capturedCount = count;
            },
        };

        using var sink = new ClickHouseSink(options, _mockClient);

        Assert.ThrowsAsync<InvalidOperationException>(
            () => sink.EmitBatchAsync(new[] { new LogEventBuilder().WithMessage("Test").Build() }));

        Assert.That(capturedException, Is.SameAs(expectedException));
        Assert.That(capturedCount, Is.EqualTo(1));
    }
}
