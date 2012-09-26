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

# Results formatter
module Shell
  module Formatter
    # Base abstract class for results formatting.
    class Base
      attr_reader :row_count

      def is_valid_io?(obj)
        obj.instance_of?(IO) || obj == Kernel
      end

      def refresh_width()
        if $stdout.tty?
          @max_width = Java::jline.Terminal.getTerminal().getTerminalWidth()
        else
          @max_width = 0
        end
      end

      # Takes an output stream and a print width.
      def initialize(opts = {})
        options = {
          :output_stream => Kernel,
        }.merge(opts)

        @out = options[:output_stream]
        refresh_width
        @row_count = 0

        # raise an error if the stream is not valid
        raise(TypeError, "Type #{@out.class} of parameter #{@out} is not IO") unless is_valid_io?(@out)
      end

      def header(args = [], widths = [])
        refresh_width
        row(args, false, widths) if args.length > 0
        @row_count = 0
      end

      # Output a row.
      # Inset is whether or not to offset row by a space.
      def row(args = [], inset = true, widths = [])
        # Print out nothing
        return if !args || args.empty?

        # Print a string
        if args.is_a?(String)
          output(args)
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
            col1width = (not widths or widths.length == 0) ? @max_width / 4 : @max_width * widths[0] / 100
            col2width = (not widths or widths.length < 2) ? @max_width - col1width - 2 : @max_width * widths[1] / 100 - 2
          end
          splits1 = split(col1width, dump(args[0]))
          splits2 = split(col2width, dump(args[1]))
          biggest = (splits2.length > splits1.length)? splits2.length: splits1.length
          index = 0
          while index < biggest
            # Inset by one space if inset is set.
            @out.print(" ") if inset
            output(col1width, splits1[index])
            # Add extra space so second column lines up w/ second column output
            @out.print(" ") unless inset
            @out.print(" ")
            output(col2width, splits2[index])
            index += 1
            @out.puts
          end
        else
          # Print a space to set off multi-column rows
          print ' '
          first = true
          for e in args
            @out.print " " unless first
            first = false
            @out.print e
          end
          puts
        end
        @row_count += 1
      end

      def split(width, str)
        if width == 0
          return [str]
        end
        result = []
        index = 0
        while index < str.length do
          result << str.slice(index, width)
          index += width
        end
        result
      end

      def dump(str)
        return if str.instance_of?(Fixnum)
        # Remove double-quotes added by 'dump'.
        return str
      end

      def output(str)
        output(@max_width, str)
      end

      def output(width, str)
        if str == nil
          str = ''
        end
        if not width or width == str.length
          @out.print(str)
        else
          @out.printf('%-*s', width, str)
        end
      end

      def calculateRunTime(start_time = nil)
        return unless start_time
        return (Time.now - start_time)
      end
    end
    
    #RowFormatter formats the return response with number of rows altered.
    #Use this for operations (command groups) for which row count makes sense, eg: delete, add etc.
    #Formatter is now defined at command group level.
    class RowFormatter < Base
      def footer(start_time = nil, row_count = nil)
        timeSpent = calculateRunTime(start_time)
        unless(row_count.is_a?(Integer))
          row_count = nil
        end
        row_count ||= @row_count
        # Only output elapsed time and row count if startTime passed
        @out.puts("%d row(s) in %.4f seconds" % [row_count, timeSpent])
      end
    end
    
    #AdminFormatter is used for all non row specific operations (command groups) such as version, status etc.
    #Formatter is now defined at command group level.
    class AdminFormatter < Base
      def footer(start_time = nil, msg = "")
        timeSpent = calculateRunTime(start_time)
        if(msg.is_a?(Integer))
          msg = msg, " rows"
        end
        @out.puts("Succeeded in %.4f seconds.\n%s" % [timeSpent, msg])
      end
    end
    
  end
end

