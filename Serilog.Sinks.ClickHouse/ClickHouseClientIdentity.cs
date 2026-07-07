using ClickHouse.Driver.ADO;

namespace Serilog.Sinks.ClickHouse;

/// <summary>
/// Tags the ClickHouse client's <c>User-Agent</c> with this sink's identity so that
/// queries issued by the sink are attributable server-side via
/// <see cref="ClickHouseClientSettings.ApplicationInfo"/> (ClickHouse.Driver 1.3.0+).
/// </summary>
internal static class ClickHouseClientIdentity
{
    /// <summary>
    /// Value of the <c>lib</c> User-Agent tag identifying this library.
    /// </summary>
    internal const string LibraryName = "Serilog.Sinks.ClickHouse";

    /// <summary>
    /// Returns a copy of <paramref name="settings"/> with the <c>lib</c> tag set to
    /// <see cref="LibraryName"/>, preserving any caller-supplied ApplicationInfo tags.
    /// </summary>
    internal static ClickHouseClientSettings WithLibraryTag(this ClickHouseClientSettings settings)
    {
        var applicationInfo = new Dictionary<string, string>();
        if (settings.ApplicationInfo is not null)
        {
            foreach (var tag in settings.ApplicationInfo)
            {
                applicationInfo[tag.Key] = tag.Value;
            }
        }

        applicationInfo["lib"] = LibraryName;
        return new ClickHouseClientSettings(settings) { ApplicationInfo = applicationInfo };
    }

    /// <summary>
    /// Builds client settings from a connection string with the <c>lib</c> tag applied.
    /// </summary>
    internal static ClickHouseClientSettings CreateTaggedSettings(string connectionString)
        => new ClickHouseClientSettings(connectionString).WithLibraryTag();
}
