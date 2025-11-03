using System.Collections.Generic;

namespace Serilog.Sinks.ClickHouse.Provider
{
    public class ColumnOptions
    {
        public IEnumerable<string> RemoveStandardColumns { get; set; }
    }
}