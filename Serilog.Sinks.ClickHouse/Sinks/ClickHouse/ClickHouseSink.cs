using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Serilog.Core;
using Serilog.Events;
using Serilog.Debugging;
using Serilog.Sinks.ClickHouse.Provider;

namespace Serilog.Sinks.ClickHouse
{
    public class ClickHouseSink : IBatchedLogEventSink
    {
        private readonly IFormatProvider _formatProvider;
        private readonly ClickHouseProvider<ColumnFormatter> _provider;
        private readonly IEnumerable<AdditionalColumn> _additionalColumns;
        private readonly ColumnOptions _columnOptions;

        public ClickHouseSink(
            string connectionString,
            string tableName,
            ColumnOptions columnOptions = null,
            IEnumerable<AdditionalColumn> additionalColumns = null,
            IFormatProvider formatProvider = null,
            bool autoCreateSqlTable = true)
        {
            _columnOptions = columnOptions;
            _additionalColumns = additionalColumns;
            _formatProvider = formatProvider;
            _provider = new ClickHouseProvider<ColumnFormatter>(tableName, connectionString, columnOptions, additionalColumns, autoCreateSqlTable);
        }

        public async Task EmitBatchAsync(IReadOnlyCollection<LogEvent> events)
        {
            try
            {
                await _provider.FlushAsync(events.Select(e => new ColumnFormatter(e, _formatProvider, _additionalColumns, _columnOptions?.RemoveStandardColumns)));
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine("Unable to write {0} log events to the database due to following error: {1}", events.Count(), ex.Message);
            }
        }

        public Task OnEmptyBatchAsync() => Task.CompletedTask;
    }
}
