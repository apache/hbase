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
    class Count < Command
      def help
        <<-EOF
Count the number of rows in a table.  Return value is the number of rows.
This operation may take a LONG time (Run '$HADOOP_HOME/bin/hadoop jar
hbase.jar rowcount' to run a counting mapreduce job). Current count is shown
every 1000 rows by default. Count interval may be optionally specified. Scan
caching is enabled on count scans by default. Default cache size is 10 rows.
If your rows are small in size, you may want to increase this
parameter. Examples:

 hbase> count 'ns1:t1'
 hbase> count 't1'
 hbase> count 't1', INTERVAL => 100000
 hbase> count 't1', CACHE => 1000
 hbase> count 't1', INTERVAL => 10, CACHE => 1000
 hbase> count 't1', FILTER => "
    (QualifierFilter (>=, 'binary:xyz')) AND (TimestampsFilter ( 123, 456))"
 hbase> count 't1', COLUMNS => ['c1', 'c2'], STARTROW => 'abc', STOPROW => 'xyz'

The same commands also can be run on a table reference. Suppose you had a reference
t to table 't1', the corresponding commands would be:

 hbase> t.count
 hbase> t.count INTERVAL => 100000
 hbase> t.count CACHE => 1000
 hbase> t.count INTERVAL => 10, CACHE => 1000
 hbase> t.count FILTER => "
    (QualifierFilter (>=, 'binary:xyz')) AND (TimestampsFilter ( 123, 456))"
 hbase> t.count COLUMNS => ['c1', 'c2'], STARTROW => 'abc', STOPROW => 'xyz'

By default, this operation does not cause any new blocks to be read into
the RegionServer block cache. This is typically the desired action; however,
if you want to force all blocks for a table to be loaded into the block cache
on-demand, you can pass the 'CACHE_BLOCKS' option with a value of 'true'. A value
of 'false' is the default and will result in no blocks being cached. This
command can be used in conjunction with all other options.

hbase> count 'ns1:t1', CACHE_BLOCKS => true
hbase> count 'ns1:t1', CACHE_BLOCKS => 'true'
hbase> count 'ns1:t1', INTERVAL => 100000, CACHE_BLOCKS => false
EOF
      end

      def command(table, params = {})
        count(table(table), params)
      end

      def count(table, params = {})
        # If the second parameter is an integer, then it is the old command syntax
        params = { 'INTERVAL' => params } if params.is_a?(Integer)

        # Try to be nice and convert a string to a bool
        if params.include?('CACHE_BLOCKS') and params['CACHE_BLOCKS'].is_a?(String)
          if params['CACHE_BLOCKS'].downcase == 'true'
            params['CACHE_BLOCKS'] = true
          elsif params['CACHE_BLOCKS'].downcase == 'false'
            params['CACHE_BLOCKS'] = false
          else
            raise(ArgumentError, "Expected CACHE_BLOCKS value to be a boolean or the string 'true' or 'false'")
          end
        end

        # Merge params with defaults
        params = {
          'INTERVAL' => 1000,
          'CACHE' => 10,
          'CACHE_BLOCKS' => false
        }.merge(params)

        scan = table._hash_to_scan(params)
        # Call the counter method
        @start_time = Time.now
        formatter.header
        count = table._count_internal(params['INTERVAL'].to_i, scan, params['CACHE_BLOCKS']) do |cnt, row|
          formatter.row(["Current count: #{cnt}, row: #{row}"])
        end
        formatter.footer(count)
        count
      end
    end
  end
end

# Add the method table.count that calls count.count
::Hbase::Table.add_shell_command('count')
