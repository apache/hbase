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

require 'hbase'

include HBaseConstants

module Hbase
  # Constructor tests
  class TableConstructorTest < Test::Unit::TestCase
    include TestHelpers
    def setup
      setup_hbase
    end

    def teardown
      shutdown
    end

    define_test "Hbase::Table constructor should not fail for existent tables" do
      assert_nothing_raised do
        table('hbase:meta').close()
      end
    end
  end

  # Helper methods tests
  class TableHelpersTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      create_test_table(@test_name)
      @test_table = table(@test_name)
    end

    def tearDown
      @test_table.close()
      shutdown
    end

    define_test "is_meta_table? method should return true for the meta table" do
      assert(table('hbase:meta').is_meta_table?)
    end

    define_test "is_meta_table? method should return false for a normal table" do
      assert(!@test_table.is_meta_table?)
    end

    #-------------------------------------------------------------------------------

    define_test "get_all_columns should return columns list" do
      cols = table('hbase:meta').get_all_columns
      assert_kind_of(Array, cols)
      assert(cols.length > 0)
    end

    #-------------------------------------------------------------------------------

    define_test "parse_column_name should not return a qualifier for name-only column specifiers" do
      col, qual = table('hbase:meta').parse_column_name('foo')
      assert_not_nil(col)
      assert_nil(qual)
    end

    define_test "parse_column_name should support and empty column qualifier" do
      col, qual = table('hbase:meta').parse_column_name('foo:')
      assert_not_nil(col)
      assert_not_nil(qual)
    end

    define_test "parse_column_name should return a qualifier for family:qualifier column specifiers" do
      col, qual = table('hbase:meta').parse_column_name('foo:bar')
      assert_not_nil(col)
      assert_not_nil(qual)
    end
  end

  # Simple data management methods tests
  class TableSimpleMethodsTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      create_test_table(@test_name)
      @test_table = table(@test_name)
      
      # Insert data to perform delete operations
      @test_table.put("101", "x:a", "1")
      @test_table.put("101", "x:a", "2", Time.now.to_i)
      
      @test_table.put("102", "x:a", "1", 1212)
      @test_table.put("102", "x:a", "2", 1213)
      
      @test_table.put(103, "x:a", "3")
      @test_table.put(103, "x:a", "4")
      
      @test_table.put("104", "x:a", 5)
      @test_table.put("104", "x:b", 6)
      
      @test_table.put(105, "x:a", "3")
      @test_table.put(105, "x:a", "4")
    end

    def teardown
      @test_table.close
      shutdown
    end

    define_test "put should work without timestamp" do
      @test_table.put("123", "x:a", "1")
    end

    define_test "put should work with timestamp" do
      @test_table.put("123", "x:a", "2", Time.now.to_i)
    end

    define_test "put should work with integer keys" do
      @test_table.put(123, "x:a", "3")
    end

    define_test "put should work with integer values" do
      @test_table.put("123", "x:a", 4)
    end
    
    define_test "put should work with attributes" do
       @test_table.put("123", "x:a", 4, {ATTRIBUTES=>{'mykey'=>'myvalue'}})
    end

    #-------------------------------------------------------------------------------

    define_test "delete should work without timestamp" do
      @test_table.delete("101", "x:a")
      res = @test_table._get_internal('101', 'x:a')
      assert_nil(res)
    end

    define_test "delete should work with timestamp" do
      @test_table.delete("102", "x:a", 1214)
      res = @test_table._get_internal('102', 'x:a')
      assert_nil(res)
    end

    define_test "delete should work with integer keys" do
      @test_table.delete(103, "x:a")
      res = @test_table._get_internal('103', 'x:a')
      assert_nil(res)
    end

    #-------------------------------------------------------------------------------

    define_test "deleteall should work w/o columns and timestamps" do
      @test_table.deleteall("104")
      res = @test_table._get_internal('104', 'x:a', 'x:b')
      assert_nil(res)
    end

    define_test "deleteall should work with integer keys" do
      @test_table.deleteall(105)
      res = @test_table._get_internal('105', 'x:a')
      assert_nil(res)
    end

    define_test "append should work with value" do
      @test_table.append("123", 'x:cnt2', '123')
    end
    #-------------------------------------------------------------------------------

    define_test "get_counter should work with integer keys" do
      @test_table.incr(12345, 'x:cnt')
      assert_kind_of(Fixnum, @test_table._get_counter_internal(12345, 'x:cnt'))
    end

    define_test "get_counter should return nil for non-existent counters" do
      assert_nil(@test_table._get_counter_internal(12345, 'x:qqqq'))
    end
  end

  # Complex data management methods tests
  class TableComplexMethodsTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      create_test_table(@test_name)
      @test_table = table(@test_name)

      # Instert test data
      @test_ts = 12345678
      @test_table.put(1, "x:a", 1)
      @test_table.put(1, "x:b", 2, @test_ts)

      @test_table.put(2, "x:a", 11)
      @test_table.put(2, "x:b", 12, @test_ts)
      
      @test_table.put(3, "x:a", 21, {ATTRIBUTES=>{'mykey'=>'myvalue'}})
      @test_table.put(3, "x:b", 22, @test_ts, {ATTRIBUTES=>{'mykey'=>'myvalue'}})
    end

    def teardown
      @test_table.close
      shutdown
    end

    define_test "count should work w/o a block passed" do
      assert(@test_table._count_internal > 0)
    end

    define_test "count should work with a block passed (and yield)" do
      rows = []
      cnt = @test_table._count_internal(1) do |cnt, row|
        rows << row
      end
      assert(cnt > 0)
      assert(!rows.empty?)
    end

    #-------------------------------------------------------------------------------

    define_test "get should work w/o columns specification" do
      res = @test_table._get_internal('1')
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end
    
    define_test "get should work for data written with Attributes" do
      res = @test_table._get_internal('3')
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get should work with integer keys" do
      res = @test_table._get_internal(1)
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get should work with hash columns spec and a single string COLUMN parameter" do
      res = @test_table._get_internal('1', COLUMN => 'x:a')
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_nil(res['x:b'])
    end

    define_test "get should work with hash columns spec and a single string COLUMNS parameter" do
      res = @test_table._get_internal('1', COLUMNS => 'x:a')
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_nil(res['x:b'])
    end

    define_test "get should work with hash columns spec and an array of strings COLUMN parameter" do
      res = @test_table._get_internal('1', COLUMN => [ 'x:a', 'x:b' ])
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get should work with hash columns spec and an array of strings COLUMNS parameter" do
      res = @test_table._get_internal('1', COLUMNS => [ 'x:a', 'x:b' ])
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end
    
    define_test "get should work with hash columns spec and an array of strings COLUMNS parameter with AUTHORIZATIONS" do
      res = @test_table._get_internal('1', COLUMNS => [ 'x:a', 'x:b' ], AUTHORIZATIONS=>['PRIVATE'])
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get should work with hash columns spec and TIMESTAMP only" do
      res = @test_table._get_internal('1', TIMESTAMP => @test_ts)
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end
    
     define_test "get should work with hash columns spec and TIMESTAMP and AUTHORIZATIONS" do
      res = @test_table._get_internal('1', TIMESTAMP => 1234, AUTHORIZATIONS=>['PRIVATE'])
      assert_nil(res)
    end
    
    define_test "get should fail with hash columns spec and strange COLUMN value" do
      assert_raise(ArgumentError) do
        @test_table._get_internal('1', COLUMN => {})
      end
    end

    define_test "get should fail with hash columns spec and strange COLUMNS value" do
      assert_raise(ArgumentError) do
        @test_table._get_internal('1', COLUMN => {})
      end
    end

    define_test "get should fail with hash columns spec and no TIMESTAMP or COLUMN[S]" do
      assert_raise(ArgumentError) do
        @test_table._get_internal('1', { :foo => :bar })
      end
    end

    define_test "get should work with a string column spec" do
      res = @test_table._get_internal('1', 'x:b')
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get should work with an array columns spec" do
      res = @test_table._get_internal('1', 'x:a', 'x:b')
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get should work with an array or arrays columns spec (yeah, crazy)" do
      res = @test_table._get_internal('1', ['x:a'], ['x:b'])
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test "get with a block should yield (column, value) pairs" do
      res = {}
      @test_table._get_internal('1') { |col, val| res[col] = val }
      assert_equal(res.keys.sort, [ 'x:a', 'x:b' ])
    end
    
    define_test "get should support COLUMNS with value CONVERTER information" do
        @test_table.put(1, "x:c", [1024].pack('N'))
        @test_table.put(1, "x:d", [98].pack('N'))
        begin
          res = @test_table._get_internal('1', ['x:c:toInt'], ['x:d:c(org.apache.hadoop.hbase.util.Bytes).toInt'])
          assert_not_nil(res)
          assert_kind_of(Hash, res)
          assert_not_nil(/value=1024/.match(res['x:c']))
          assert_not_nil(/value=98/.match(res['x:d']))
        ensure
          # clean up newly added columns for this test only.
          @test_table.delete(1, "x:c")
          @test_table.delete(1, "x:d")
        end
    end

    define_test "get should support FILTER" do
      @test_table.put(1, "x:v", "thisvalue")
      begin
        res = @test_table._get_internal('1', FILTER => "ValueFilter(=, 'binary:thisvalue')")
        assert_not_nil(res)
        assert_kind_of(Hash, res)
        assert_not_nil(res['x:v'])
        assert_nil(res['x:a'])
        res = @test_table._get_internal('1', FILTER => "ValueFilter(=, 'binary:thatvalue')")
        assert_nil(res)
      ensure
        # clean up newly added columns for this test only.
        @test_table.delete(1, "x:v")
      end
    end

    #-------------------------------------------------------------------------------

    define_test "scan should work w/o any params" do
      res = @test_table._scan_internal
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_not_nil(res['2'])
      assert_not_nil(res['2']['x:a'])
      assert_not_nil(res['2']['x:b'])
    end

    define_test "scan should support STARTROW parameter" do
      res = @test_table._scan_internal STARTROW => '2'
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_nil(res['1'])
      assert_not_nil(res['2'])
      assert_not_nil(res['2']['x:a'])
      assert_not_nil(res['2']['x:b'])
    end

    define_test "scan should support STOPROW parameter" do
      res = @test_table._scan_internal STOPROW => '2'
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_nil(res['2'])
    end

    define_test "scan should support ROWPREFIXFILTER parameter (test 1)" do
      res = @test_table._scan_internal ROWPREFIXFILTER => '1'
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_nil(res['2'])
    end

    define_test "scan should support ROWPREFIXFILTER parameter (test 2)" do
      res = @test_table._scan_internal ROWPREFIXFILTER => '2'
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_nil(res['1'])
      assert_not_nil(res['2'])
      assert_not_nil(res['2']['x:a'])
      assert_not_nil(res['2']['x:b'])
    end

    define_test "scan should support LIMIT parameter" do
      res = @test_table._scan_internal LIMIT => 1
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_nil(res['2'])
    end
    
    define_test "scan should support REVERSED parameter" do
      res = @test_table._scan_internal REVERSED => true
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_not_nil(res['2'])
      assert_not_nil(res['2']['x:a'])
      assert_not_nil(res['2']['x:b'])
    end

    define_test "scan should support TIMESTAMP parameter" do
      res = @test_table._scan_internal TIMESTAMP => @test_ts
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_not_nil(res['2'])
      assert_nil(res['2']['x:a'])
      assert_not_nil(res['2']['x:b'])
    end

    define_test "scan should support TIMERANGE parameter" do
      res = @test_table._scan_internal TIMERANGE => [0, 1]
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_nil(res['1'])
      assert_nil(res['2'])
    end

    define_test "scan should support COLUMNS parameter with an array of columns" do
      res = @test_table._scan_internal COLUMNS => [ 'x:a', 'x:b' ]
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_not_nil(res['2'])
      assert_not_nil(res['2']['x:a'])
      assert_not_nil(res['2']['x:b'])
    end
    
    define_test "scan should support COLUMNS parameter with a single column name" do
      res = @test_table._scan_internal COLUMNS => 'x:a'
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_nil(res['1']['x:b'])
      assert_not_nil(res['2'])
      assert_not_nil(res['2']['x:a'])
      assert_nil(res['2']['x:b'])
    end

    define_test "scan should work with raw and version parameter" do
      # Create test table if it does not exist
      @test_name_raw = "hbase_shell_tests_raw_scan"
      create_test_table(@test_name_raw)
      @test_table = table(@test_name_raw)

      # Instert test data
      @test_table.put(1, "x:a", 1)
      @test_table.put(2, "x:raw1", 11)
      @test_table.put(2, "x:raw1", 11)
      @test_table.put(2, "x:raw1", 11)
      @test_table.put(2, "x:raw1", 11)

      args = {}
      numRows = 0
      count = @test_table._scan_internal(args) do |row, cells| # Normal Scan
        numRows += 1
      end
      assert_equal(numRows, 2, "Num rows scanned without RAW/VERSIONS are not 2") 

      args = {VERSIONS=>10,RAW=>true} # Since 4 versions of row with rowkey 2 is been added, we can use any number >= 4 for VERSIONS to scan all 4 versions.
      numRows = 0
      count = @test_table._scan_internal(args) do |row, cells| # Raw Scan
        numRows += 1
      end
      assert_equal(numRows, 5, "Num rows scanned without RAW/VERSIONS are not 5") # 5 since , 1 from row key '1' and other 4 from row key '4'
    end



    define_test "scan should fail on invalid COLUMNS parameter types" do
      assert_raise(ArgumentError) do
        @test_table._scan_internal COLUMNS => {}
      end
    end

    define_test "scan should fail on non-hash params" do
      assert_raise(ArgumentError) do
        @test_table._scan_internal 123
      end
    end

    define_test "scan with a block should yield rows and return rows counter" do
      rows = {}
      res = @test_table._scan_internal { |row, cells| rows[row] = cells }
      assert_equal(rows.keys.size, res)
    end
    
    define_test "scan should support COLUMNS with value CONVERTER information" do
      @test_table.put(1, "x:c", [1024].pack('N'))
      @test_table.put(1, "x:d", [98].pack('N'))
      begin
        res = @test_table._scan_internal COLUMNS => ['x:c:toInt', 'x:d:c(org.apache.hadoop.hbase.util.Bytes).toInt']
        assert_not_nil(res)
        assert_kind_of(Hash, res)
        assert_not_nil(/value=1024/.match(res['1']['x:c']))
        assert_not_nil(/value=98/.match(res['1']['x:d']))
      ensure
        # clean up newly added columns for this test only.
        @test_table.delete(1, "x:c")
        @test_table.delete(1, "x:d")
      end
    end

    define_test "scan should support FILTER" do
      @test_table.put(1, "x:v", "thisvalue")
      begin
        res = @test_table._scan_internal FILTER => "ValueFilter(=, 'binary:thisvalue')"
        assert_not_equal(res, {}, "Result is empty")
        assert_kind_of(Hash, res)
        assert_not_nil(res['1'])
        assert_not_nil(res['1']['x:v'])
        assert_nil(res['1']['x:a'])
        assert_nil(res['2'])
        res = @test_table._scan_internal FILTER => "ValueFilter(=, 'binary:thatvalue')"
        assert_equal(res, {}, "Result is not empty")
      ensure
        # clean up newly added columns for this test only.
        @test_table.delete(1, "x:v")
      end
    end

    define_test "scan hbase meta table" do
      res = table("hbase:meta")._scan_internal
      assert_not_nil(res)
    end

    define_test "mutation with TTL should expire" do
      @test_table.put('ttlTest', 'x:a', 'foo', { TTL => 1000 } )
      begin
        res = @test_table._get_internal('ttlTest', 'x:a')
        assert_not_nil(res)
        sleep 2
        res = @test_table._get_internal('ttlTest', 'x:a')
        assert_nil(res)
      ensure
        @test_table.delete('ttlTest', 'x:a')
      end
    end

    define_test "Split count for a table" do
      @testTableName = "tableWithSplits"
      create_test_table_with_splits(@testTableName, SPLITS => ['10', '20', '30', '40'])
      @table = table(@testTableName)
      splits = @table._get_splits_internal()
      #Total splits is 5 but here count is 4 as we ignore implicit empty split.
      assert_equal(4, splits.size)
      assert_equal(["10", "20", "30", "40"], splits)
      drop_test_table(@testTableName)
    end

    define_test "Split count for a empty table" do
      splits = @test_table._get_splits_internal()
      #Empty split should not be part of this array.
      assert_equal(0, splits.size)
      assert_equal([], splits)
    end
  end
end
