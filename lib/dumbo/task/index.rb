require 'dumbo/task/base'

module Dumbo
  module Task
    class Index < Base
      def initialize(source, namespace, interval)
        @source = source
        @namespace = namespace
        @interval = interval
        @datasource = @source['dataSource']
        @datasource = "#{@source['dataSource']}_#{@namespace}" if @namespace != 'default'
      end

      def as_json(options = {})
        interval = "#{@interval.first.iso8601}/#{@interval.last.iso8601}"
        config = {
          type: 'index',
          spec: {
            dataSchema: {
              dataSource: @datasource,
              metricsSpec: (@source['metrics'] || {}).map do |x|
                { type: x['type'], name: x['name'], fieldName: x['fieldName'] }
              # WARNING: do NOT use count for events, will count in segment vs count in raw input
              end + [{ type: "doubleSum", name: "events", fieldName: "events" }],
              granularitySpec: {
                segmentGranularity: @source['output']['segmentGranularity'] || "hour",
                queryGranularity: @source['output']['queryGranularity'] || "minute",
                intervals: [interval],
              }
            },
            ioConfig: {
              type: 'index',
              firehose: {
                type: "ingestSegment",
                dataSource: @datasource,
                interval: interval,
                dimensions: @source['dimensions'],
              },
            },
            tuningConfig: {
              type: 'index',
              rowFlushBoundary: 400000,
            },
          },
        }
        if (@source['output']['targetPartitionSize'] || 0) > 0
          config[:spec][:tuningConfig][:targetPartitionSize] = @source['output']['targetPartitionSize']
          config[:spec][:tuningConfig][:numShards] = -1
        elsif (@source['output']['numShards'] || 0) > 1
          config[:spec][:tuningConfig][:targetPartitionSize] = -1
          config[:spec][:tuningConfig][:numShards] = @source['output']['numShards']
        end
        config
      end
    end
  end
end
