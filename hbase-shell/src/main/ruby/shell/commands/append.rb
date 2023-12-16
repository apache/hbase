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
    class Append < Command
      def help
        <<-EOF
Appends a cell 'value' at specified table/row/column coordinates.

  hbase> append 't1', 'r1', 'c1', 'value', ATTRIBUTES=>{'mykey'=>'myvalue'}
  hbase> append 't1', 'r1', 'c1', 'value', {VISIBILITY=>'PRIVATE|SECRET'}

The same commands also can be run on a table reference. Suppose you had a reference
t to table 't1', the corresponding command would be:

  hbase> t.append 'r1', 'c1', 'value', ATTRIBUTES=>{'mykey'=>'myvalue'}
  hbase> t.append 'r1', 'c1', 'value', {VISIBILITY=>'PRIVATE|SECRET'}

Alternately, we can run the following commands for appending cell values for
multiple columns at specified table/row coordinates.

  hbase> append 't1', 'r1', {'c1'=>'value1', 'c2'=>'value2'}, {ATTRIBUTES=>{'mykey'=>'myvalue'}}
  hbase> append 't1', 'r1', {'c1'=>'value1', 'c2'=>'value2'}, {VISIBILITY=>'PRIVATE|SECRET'}

The same commands also can be run on a table reference.

  hbase> t.append 'r1', {'c1'=>'value1', 'c2'=>'value2'}, {ATTRIBUTES=>{'mykey'=>'myvalue'}}
  hbase> t.append 'r1', {'c1'=>'value1', 'c2'=>'value2'}, {VISIBILITY=>'PRIVATE|SECRET'}

EOF
      end

      def command(table_name, row, column, value = value_omitted = {}, args = args_omitted = {})
        table = table(table_name)
        @start_time = Time.now
        # Conditional block to omit passing optional arguments explicitly
        if !value_omitted.nil?
          # value field was not passed (will reach only for multi column append)
          append(table, row, column)
        elsif !args_omitted.nil?
          # args field was not passed (must not be passed for multi column append)
          append(table, row, column, value)
        else
          append(table, row, column, value, args)
        end
      end

      def append(table, row, column, value = value_omitted = {}, args = args_omitted = {})
        if column.is_a?(Hash)
          # args field must not be passed; already contained in value field
          raise(ArgumentError, 'wrong number of arguments') if args_omitted.nil?
          if current_values = table._append_multi_column_internal(row, column, value)
            puts "CURRENT VALUES = #{current_values}"
          end
        else
          # value field must be passed by user
          raise(ArgumentError, 'wrong number of arguments') unless value_omitted.nil?
          if current_value = table._append_internal(row, column, value, args)
            puts "CURRENT VALUE = #{current_value}"
          end
        end
      end
    end
  end
end

# add append command to Table
::Hbase::Table.add_shell_command('append')
