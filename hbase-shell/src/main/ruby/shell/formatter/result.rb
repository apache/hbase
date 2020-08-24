#
# Copyright The Apache Software Foundation
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
require 'shell/formatter/util'

module Shell
  module Formatter
    ##
    # Mixin providing helper methods for commands that print cell-scannable results
    module ResultMixin
      private def get_row_bytes(c)
        org.apache.hadoop.hbase.util.Bytes.copy(c.getRowArray, c.getRowOffset, c.getRowLength)
      end

      private def get_family_bytes(c)
        org.apache.hadoop.hbase.util.Bytes.copy(c.getFamilyArray, c.getFamilyOffset, c.getFamilyLength)
      end

      private def get_qualifier_bytes(c)
        org.apache.hadoop.hbase.util.Bytes.copy(c.getQualifierArray, c.getQualifierOffset, c.getQualifierLength)
      end

      ##
      # iterates until cell scanner has been exhausted
      #
      # @param [org.apache.hadoop.hbase.CellScanner] cell scanner or subclass like result
      # @return [Enumerator] generates Cell objects
      def cell_iterator(cell_scanner)
        Enumerator.new do |yielder|
          loop do
            cell = cell_scanner.current
            yielder.yield cell unless cell.nil?
            break unless cell_scanner.advance
          end
        end
      end

      ##
      # iteraters until result scanner has been exhausted
      #
      # @param [ResultScanner] result_scanner
      # @return [Enumerator] generates Result objects
      def result_iterator(result_scanner)
        Enumerator.new do |yielder|
          iter = result_scanner.iterator
          loop do
            break unless iter.hasNext

            yielder.yield iter.next
          end
        end
      end

      ##
      # print cells, using converters from the given table
      #
      # @param [Enumerator] cells generates Cell objects
      # @param [Hbase::Table] table used for converters
      # @param [String] converter name of the method to use from converter_class
      # @param [String] converter_class
      def print_result(cells, table, converter = nil, converter_class = nil)
        converter ||= 'toStringBinary'
        converter_class ||= 'org.apache.hadoop.hbase.util.Bytes'

        table_formatter.start_table(
            headers: %w[ROW COLUMN TIMESTAMP VALUE],
            widths: [nil, nil, ::Shell::Formatter::Util::ISO8601_WIDTH, nil]
        )

        cells.each do |c|
          # Get the family and qualifier of the cell without escaping non-printable characters. It is crucial that
          # column is constructed in this consistent way to that it can be used as a key.
          row_bytes = get_row_bytes c
          family_bytes = get_family_bytes c
          qualifier_bytes = get_qualifier_bytes c
          column = "#{family_bytes}:#{qualifier_bytes}"

          value = table.convert(column, c, converter_class, converter)

          # Use the FORMATTER to determine how column is printed
          row = table.convert_bytes(row_bytes, converter_class, converter)
          family = table.convert_bytes(family_bytes, converter_class, converter)
          qualifier = table.convert_bytes(qualifier_bytes, converter_class, converter)
          formatted_column = "#{family}:#{qualifier}"

          timestamp = ::Shell::Formatter::Util.to_iso_8601 c.getTimestamp

          table_formatter.row([row, formatted_column, timestamp, value])
        end

        table_formatter.close_table(num_rows: table_formatter.get_rows_printed)

        nil
      end
    end
  end
end
