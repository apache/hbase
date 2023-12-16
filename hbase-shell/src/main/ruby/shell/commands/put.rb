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
    class Put < Command
      def help
        <<-EOF
Put a cell 'value' at specified table/row/column and optionally
timestamp coordinates.  To put a cell value into table 'ns1:t1' or 't1'
at row 'r1' under column 'c1' marked with the time 'ts1', do:

  hbase> put 'ns1:t1', 'r1', 'c1', 'value'
  hbase> put 't1', 'r1', 'c1', 'value'
  hbase> put 't1', 'r1', 'c1', 'value', ts1
  hbase> put 't1', 'r1', 'c1', 'value', {ATTRIBUTES=>{'mykey'=>'myvalue'}}
  hbase> put 't1', 'r1', 'c1', 'value', ts1, {ATTRIBUTES=>{'mykey'=>'myvalue'}}
  hbase> put 't1', 'r1', 'c1', 'value', ts1, {VISIBILITY=>'PRIVATE|SECRET'}

The same commands also can be run on a table reference. Suppose you had a reference
t to table 't1', the corresponding command would be:

  hbase> t.put 'r1', 'c1', 'value', ts1, {ATTRIBUTES=>{'mykey'=>'myvalue'}}

Alternately, we can put cell values for multiple columns at specified table/row and
optionally timestamp coordinates.

  hbase> put 'ns1:t1', 'r1', {'c1'=>'value1', 'c2'=>'value2'}
  hbase> put 't1', 'r1', {'c1'=>'value1', 'c2'=>'value2'}
  hbase> put 't1', 'r1', {'c1'=>'value1', 'c2'=>'value2'}, ts1
  hbase> put 't1', 'r1', {'c1'=>'value1', 'c2'=>'value2'}, {ATTRIBUTES=>{'mykey'=>'myvalue'}}
  hbase> put 't1', 'r1', {'c1'=>'value1', 'c2'=>'value2'}, ts1, {ATTRIBUTES=>{'mykey'=>'myvalue'}}
  hbase> put 't1', 'r1', {'c1'=>'value1', 'c2'=>'value2'}, ts1, {VISIBILITY=>'PRIVATE|SECRET'}

The same commands also can be run on a table reference.

  hbase> t.put 'r1', {'c1'=>'value1', 'c2'=>'value2'}, ts1, {ATTRIBUTES=>{'mykey'=>'myvalue'}}
EOF
      end

      def command(table, row, column, value = value_omitted = {}, timestamp = nil, args = args_omitted = {})
        # Conditional block to omit passing optional arguments explicitly
        if !value_omitted.nil?
          # value field was not passed (will reach only for multi column put)
          put table(table), row, column
        elsif !args_omitted.nil?
          # args field was not passed (must not be passed for multi column put)
          put table(table), row, column, value, timestamp
        else
          put table(table), row, column, value, timestamp, args
        end
      end

      def put(table, row, column, value = value_omitted = {}, timestamp = nil, args = args_omitted = {})
        @start_time = Time.now
        # when column is a hash map, then it is a case of multi column put
        if column.is_a?(Hash)
          # args field must not be passed; already contained in timestamp field
          raise(ArgumentError, 'wrong number of arguments') if args_omitted.nil?
          value = nil unless value != {}
          timestamp = {} if timestamp.nil?
          table._put_multi_column_internal(row, column, value, timestamp)
        else
          # value field must be passed by user
          raise(ArgumentError, 'wrong number of arguments') unless value_omitted.nil?
          table._put_internal(row, column, value, timestamp, args)
        end
      end
    end
  end
end

# Add the method table.put that calls Put.put
::Hbase::Table.add_shell_command('put')
