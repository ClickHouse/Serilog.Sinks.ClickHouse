using ClickHouse.Driver.ADO;

namespace Serilog.Sinks.ClickHouse.Tests.Unit;

/// <summary>
/// Unit tests for <see cref="ClickHouseClientIdentity"/>, which tags the ClickHouse
/// client's User-Agent with this sink's library identity.
/// </summary>
public class ClickHouseClientIdentityTests
{
    [Test]
    public void WithLibraryTag_SetsLibTagToLibraryName()
    {
        var settings = new ClickHouseClientSettings("Host=localhost");

        var tagged = settings.WithLibraryTag();

        Assert.That(tagged.ApplicationInfo, Contains.Key("lib"));
        Assert.That(tagged.ApplicationInfo["lib"], Is.EqualTo(ClickHouseClientIdentity.LibraryName));
    }

    [Test]
    public void WithLibraryTag_PreservesExistingApplicationInfoTags()
    {
        var settings = new ClickHouseClientSettings("Host=localhost")
        {
            ApplicationInfo = new Dictionary<string, string> { ["app"] = "MyApp", ["ver"] = "1.2.3" },
        };

        var tagged = settings.WithLibraryTag();

        Assert.Multiple(() =>
        {
            Assert.That(tagged.ApplicationInfo["app"], Is.EqualTo("MyApp"));
            Assert.That(tagged.ApplicationInfo["ver"], Is.EqualTo("1.2.3"));
            Assert.That(tagged.ApplicationInfo["lib"], Is.EqualTo(ClickHouseClientIdentity.LibraryName));
        });
    }

    [Test]
    public void WithLibraryTag_DoesNotMutateOriginalSettings()
    {
        var settings = new ClickHouseClientSettings("Host=localhost");

        settings.WithLibraryTag();

        Assert.That(settings.ApplicationInfo, Does.Not.ContainKey("lib"));
    }

    [Test]
    public void CreateTaggedSettings_ParsesConnectionStringAndAppliesLibTag()
    {
        var settings = ClickHouseClientIdentity.CreateTaggedSettings("Host=example;Port=8123");

        Assert.Multiple(() =>
        {
            Assert.That(settings.Host, Is.EqualTo("example"));
            Assert.That(settings.ApplicationInfo["lib"], Is.EqualTo(ClickHouseClientIdentity.LibraryName));
        });
    }
}
