#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'stringio'

# Results formatter
module Shell
  module Formatter
    # Base abstract class for results formatting.
    class Base
      attr_reader :row_count

      def is_valid_io?(obj)
        obj.instance_of?(IO) || obj.instance_of?(StringIO) || obj == Kernel
      end

      def refresh_width
        @max_width = 0
        if $stdout.tty?
          begin
            @max_width = Java.jline.TerminalFactory.get.getWidth
          rescue NameError => e
            # nocommit debug log and ignore
          end
        end
      end

      # Takes an output stream and a print width.
      def initialize(opts = {})
        options = {
          output_stream: Kernel
        }.merge(opts)

        @out = options[:output_stream]
        refresh_width
        @row_count = 0

        # raise an error if the stream is not valid
        raise(TypeError, "Type #{@out.class} of parameter #{@out} is not IO") unless is_valid_io?(@out)
      end

      def header(args = [], widths = [])
        refresh_width
        row(args, false, widths) unless args.empty?
        @row_count = 0
      end

      # Output a row.
      # Inset is whether or not to offset row by a space.
      def row(args = [], inset = true, widths = [])
        # Print out nothing
        return if !args || args.empty?

        # Print a string
        if args.is_a?(String)
          output_str(args)
          @out.puts
          return
        end

        # TODO: Look at the type.  Is it RowResult?
        if args.length == 1
          splits = split(@max_width, dump(args[0]))
          for l in splits
            output(@max_width, l)
            @out.puts
          end
        elsif args.length == 2
          if @max_width == 0
            col1width = col2width = 0
          else
            col1width = !widths || widths.empty? ? @max_width / 4 : @max_width * widths[0] / 100
            col2width = !widths || widths.length < 2 ? @max_width - col1width - 2 : @max_width * widths[1] / 100 - 2
          end
          splits1 = split(col1width, dump(args[0]))
          splits2 = split(col2width, dump(args[1]))
          biggest = splits2.length > splits1.length ? splits2.length : splits1.length
          index = 0
          while index < biggest
            # Inset by one space if inset is set.
            @out.print(' ') if inset
            output(col1width, splits1[index])
            # Add extra space so second column lines up w/ second column output
            @out.print(' ') unless inset
            @out.print(' ')
            output(col2width, splits2[index])
            index += 1
            @out.puts
          end
        else
          # Print a space to set off multi-column rows
          print ' '
          first = true
          for e in args
            @out.print ' ' unless first
            first = false
            @out.print e
          end
          puts
        end
        @row_count += 1
      end

      # Output the scan metrics. Can be filtered to output only those metrics whose keys exists
      # in the metric_filter
      def scan_metrics(scan_metrics = nil, metric_filter = [])
        return if scan_metrics.nil?
        unless scan_metrics.is_a?(org.apache.hadoop.hbase.client.metrics.ScanMetrics)
          raise(ArgumentError, \
                'Argument should be org.apache.hadoop.hbase.client.metrics.ScanMetrics')
        end
        # prefix output with empty line
        @out.puts
        # save row count to restore after printing metrics
        # (metrics should not count towards row count)
        saved_row_count = @row_count
        iter = scan_metrics.getMetricsMap.entrySet.iterator
        metric_hash = {}
        # put keys in hash so they can be sorted easily
        while iter.hasNext
          metric = iter.next
          metric_hash[metric.getKey.to_s] = metric.getValue.to_s
        end
        # print in alphabetical order
        row(%w[METRIC VALUE], false)
        metric_hash.sort.map do |key, value|
          if !metric_filter || metric_filter.empty? || metric_filter.include?(key)
            row([key, value])
          end
        end

        @row_count = saved_row_count
        nil
      end

      def split(width, str)
        return [str] if width == 0
        result = []
        index = 0
        while index < str.length
          result << str.slice(index, width)
          index += width
        end
        result
      end

      def dump(str)
        return if str.instance_of?(Integer)
        # Remove double-quotes added by 'dump'.
        str
      end

      def output_str(str)
        output(@max_width, str)
      end

      def output_strln(str)
        output_str(str)
        @out.puts
      end

      def output(width, str)
        str = '' if str.nil?
        if !width || width == str.length
          @out.print(str)
        else
          @out.printf('%-*s', width, str)
        end
      end

      def footer(row_count = nil, is_stale = false)
        row_count ||= @row_count
        # Only output elapsed time and row count if startTime passed
        @out.puts(format('%d row(s)', row_count))
        @out.puts(' (possible stale results) ') if is_stale == true
      end
    end

    class Console < Base
    end

    class XHTMLFormatter < Base
      # http://www.germane-software.com/software/rexml/doc/classes/REXML/Document.html
      # http://www.crummy.com/writing/RubyCookbook/test_results/75942.html
    end

    class JSON < Base
    end
  end
end
