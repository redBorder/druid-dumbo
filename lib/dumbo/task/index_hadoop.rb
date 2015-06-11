require 'dumbo/task/base'

module Dumbo
  module Task
    class IndexHadoop < Base
      def initialize(source, namenodes, namespace, interval, path, hadoop_version)
        @source = source
        @namespace = namespace
        @namenodes = namenodes
        @interval = interval
        @path = path
        @datasource = @source['dataSource']
        @datasource = "#{@source['dataSource']}_#{@namespace}" if @namespace != 'not_namespace_uuid'
        @hadoop_version = hadoop_version
      end

      def as_json(options = {})
        config = {
          type: 'index_hadoop',
          hadoopDependencyCoordinates: ["org.apache.hadoop:hadoop-client:#{@hadoop_version}"],
          spec: {
            dataSchema: {
              dataSource: @datasource,
              parser: {
                parseSpec: {
                  format: "json",
                  timestampSpec: {
                    column: ((@source['input']['timestamp'] || {})['column'] || "timestamp"),
                    format: ((@source['input']['timestamp'] || {})['format'] || "ruby"),
                  },
                  dimensionsSpec: {
                    dimensions: (@source['dimensions'] || []),
                    spatialDimensions: (@source['spacialDimensions'] || []),
                  }
                }
              },
              metricsSpec: (@source['metrics'] || {}).map do |x|
                { type: x['type'], name: x['name'], fieldName: x['fieldName'] }
              end + [{ type: "count", name: "events" }],
              granularitySpec: {
                segmentGranularity: @source['output']['segmentGranularity'] || "hour",
                queryGranularity: @source['output']['queryGranularity'] || "minute",
                intervals: ["#{@interval.first.iso8601}/#{@interval.last.iso8601}"],
              }
            },
            ioConfig: {
              type: 'hadoop',
              inputSpec: {
                type: 'static',
                paths: "hdfs://#{@namenodes.first}:8020#{@path}",
              },
            },
            tuningConfig: {
              type: "hadoop",
              overwriteFiles: true,
              partitionsSpec: {
                type: "none",
              },
            },
          },
        }
        if (@source['output']['numShards'] || 0) > 1
          config[:spec][:tuningConfig][:partitionsSpec] = {
            type: "hashed",
            targetPartitionSize: -1,
            numShards: @source['output']['numShards'],
          }
        end
        config
      end
    end
  end
end
