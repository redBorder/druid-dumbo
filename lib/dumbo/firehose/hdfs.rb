require 'webhdfs'
require 'dumbo/time_ext'

module Dumbo
  class Firehose
    class HDFS
      def initialize(namenodes, sources)
        [namenodes].flatten.each do |host|
          begin
            $log.info("connecting to", namenode: host)
            @hdfs = WebHDFS::Client.new(host, 50070)
            @hdfs.list('/')
            break
          rescue
            $log.info("failed to use", namenode: host)
            @hdfs = nil
          end
        end
        raise "no namenode is up and running" unless @hdfs
        @slots = {}
        @sources = sources
      end

      def slots(topic, namespace, interval)
        @slots["#{topic}_#{namespace}_#{interval}"] ||= slots!(topic, namespace, interval)
      end

      def slots!(topic, namespace, interval)
        interval = interval.map { |t| t.floor(1.hour).utc }
        $log.info("scanning HDFS for", interval: interval)
        interval = (interval.first.to_i..interval.last.to_i)
        interval.step(1.hour).map do |time|
          Slot.new(@sources, @hdfs, topic, namespace, Time.at(time).utc)
        end.reject do |slot|
          slot.events.to_i < 1
        end
      end

      def namespaces(hdfs_root)
        folders = @hdfs.list(hdfs_root).select do |entry|
          entry['pathSuffix'] =~ /\A[0-9]*\Z/ || entry['pathSuffix'] == 'not_namespace_uuid'
        end

        namespaces_ids = folders.map { |f| f['pathSuffix'] }
        $log.info('found HDFS', namespaces: namespaces_ids)
        namespaces_ids
      end

      class Slot
        attr_reader :topic, :time, :paths, :events

        def initialize(sources, hdfs, topic, namespace, time)
          @sources = sources
          @hdfs = hdfs
          @topic = topic
          @namespace = namespace
          @time = time
          @paths = paths!
          @events = @paths.map do |path|
            File.basename(path).split('.')[3].to_i
          end.reduce(:+)
        end

        def patterns
          @paths.map do |path|
            tokens = path.split('/')
            suffix = tokens[-1].split('.')
            tokens[-1] = "*.#{suffix[-1]}"
            tokens.join('/')
          end.compact.uniq.sort
        end

        def paths!
          begin
            @sources[@topic]['input']['camus'].map do |hdfs_root|
              path = "#{hdfs_root}/#{@namespace}/hourly/#{@time.strftime("%Y/%m/%d/%H")}"
              begin
                @hdfs.list(path).map do |entry|
                  File.join(path, entry['pathSuffix']) if entry['pathSuffix'] =~ /\.gz$/
                end
              rescue => e
                $log.warn("No events in #{path}, ignoring")
                nil
              end
            end.flatten.compact
          rescue
            $log.error("#{@topic} -> input.camus must be an array of HDFS paths")
            exit 1
          end
        end
      end
    end
  end
end
