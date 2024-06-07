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

module Shell
  module Commands
    class GetCounter < Command
      def help
        <<-EOF
Return a counter cell value at specified table/row/column coordinates.
A counter cell should be managed with atomic increment functions on HBase
and the data should be binary encoded (as long value). Example:

  hbase> get_counter 'ns1:t1', 'r1', 'c1'
  hbase> get_counter 't1', 'r1', 'c1'

The same commands also can be run on a table reference. Suppose you had a reference
t to table 't1', the corresponding command would be:

  hbase> t.get_counter 'r1', 'c1'

Alternately, we can run the following commands to get counter cell values for multiple
columns at specified table/row coordinates.

  hbase> get_counter 'ns1:t1', 'r1', ['c1', 'c2']
  hbase> get_counter 't1', 'r1', ['c1', 'c2']

The same commands also can be run on a table reference.

  hbase> t.get_counter 'r1', ['c1', 'c2']

EOF
      end

      # Gets the counter value(s) from a table for a specific row and column(s).
      # If the column parameter is an array, it retrieves multiple counter values.
      # If no counter is found, it prints an error message.
      #
      # @param table [String] the name of the table
      # @param row [String] the row key
      # @param column [String, Array<String>] the column name(s)
      def command(table, row, column)
        get_counter(table(table), row, column)
      end

      def get_counter(table, row, column)
        # when column is an array, then it is a case of multi column get counter
        if column.is_a?(Array)
          if cnts = table._get_counter_multi_column_internal(row, column)
            puts "COUNTER VALUES = #{cnts}"
          else
            puts 'No counters found at specified coordinates'
          end
        else
          if cnt = table._get_counter_internal(row, column)
            puts "COUNTER VALUE = #{cnt}"
          else
            puts 'No counter found at specified coordinates'
          end
        end
      end
    end
  end
end

::Hbase::Table.add_shell_command('get_counter')
