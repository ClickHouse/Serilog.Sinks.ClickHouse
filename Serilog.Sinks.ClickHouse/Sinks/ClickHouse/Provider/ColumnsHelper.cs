using System.Collections.Generic;
using System.Reflection;

namespace Serilog.Sinks.ClickHouse.Provider
{
    static class ColumnsHelper
    {
        public static List<ColumnAttribute> Mapping<T>()
        {
            var dict = new List<ColumnAttribute>();
            var props = typeof(T).GetProperties();
            foreach (var p in props)
            {
                var colAttr = p.GetCustomAttribute<ColumnAttribute>();
                if (colAttr != null)
                    dict.Add(colAttr);
            }

            return dict;
        }

        public static Dictionary<PropertyInfo,ColumnAttribute> Props<T>()
        {
            var dict = new Dictionary<PropertyInfo,ColumnAttribute>();
            var props = typeof(T).GetProperties();
            foreach (var p in props)
            {
                var columnAttribute = p.GetCustomAttribute<ColumnAttribute>();
                if (columnAttribute != null)
                {
                    dict.Add(p,columnAttribute);
                }
            }

            return dict;
        }
    }
}
