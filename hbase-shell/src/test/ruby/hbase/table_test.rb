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

require 'hbase_constants'

module Hbase
  # Constructor tests
  class TableConstructorTest < Test::Unit::TestCase
    include TestHelpers
    include HBaseConstants

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
    include HBaseConstants

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      create_test_table(@test_name)
      @test_table = table(@test_name)
      
      # Insert data to perform delete operations
      @test_table.put("102", "x:a", "2", 1212)
      @test_table.put(103, "x:a", "3", 1214)

      @test_table.put("104", "x:a", 5)
      @test_table.put("104", "x:b", 6)

      @test_table.put(105, "x:a", "3")
      @test_table.put(105, "x:a", "4")

      @test_table.put(106, "x:a", "3", 1588765900000)
      @test_table.put(106, "x:b", "4", 1588765900010)

      @test_table.put("111", "x:a", "5")
      @test_table.put("111", "x:b", "6")
      @test_table.put("112", "x:a", "5")
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
    define_test "delete should work with string keys" do
      @test_table.delete('102', 'x:a', 1212)
      res = @test_table._get_internal('102', 'x:a')
      assert_nil(res)
    end

    define_test "delete should work with integer keys" do
      res = @test_table._get_internal('103', 'x:a')
      assert_not_nil(res)
      @test_table.delete(103, 'x:a', 1214)
      res = @test_table._get_internal('103', 'x:a')
      assert_nil(res)
    end

    define_test "delete should set proper cell type" do
      del = @test_table._createdelete_internal('104', 'x:a', 1212)
      assert_equal(del.get('x'.to_java_bytes, 'a'.to_java_bytes).get(0).getType.getCode,
                   org.apache.hadoop.hbase::KeyValue::Type::DeleteColumn.getCode)
      del = @test_table._createdelete_internal('104', 'x:a', 1212, [], false)
      assert_equal(del.get('x'.to_java_bytes, 'a'.to_java_bytes).get(0).getType.getCode,
                   org.apache.hadoop.hbase::KeyValue::Type::Delete.getCode)
      del = @test_table._createdelete_internal('104', 'x', 1212)
      assert_equal(del.get('x'.to_java_bytes, nil).get(0).getType.getCode,
                   org.apache.hadoop.hbase::KeyValue::Type::DeleteFamily.getCode)
      del = @test_table._createdelete_internal('104', 'x', 1212, [], false)
      assert_equal(del.get('x'.to_java_bytes, nil).get(0).getType.getCode,
                   org.apache.hadoop.hbase::KeyValue::Type::DeleteFamilyVersion.getCode)
    end

    #-------------------------------------------------------------------------------

    define_test "deleteall should work w/o columns and timestamps" do
      @test_table.deleteall("104")
      res = @test_table._get_internal('104', 'x:a', 'x:b')
      assert_nil(res)
    end

    define_test "deleteall should work with timestamps but w/o columns" do
      @test_table.deleteall("106", "", 1588765900005)
      res = @test_table._get_internal('106', 'x:a')
      assert_nil(res)
      res = @test_table._get_internal('106', 'x:b')
      assert_not_nil(res)
    end

    define_test "deleteall should work with integer keys" do
      @test_table.deleteall(105)
      res = @test_table._get_internal('105', 'x:a')
      assert_nil(res)
    end

    define_test "deletall should work with row prefix" do
      @test_table.deleteall({ROWPREFIXFILTER => '11'})
      res1 = @test_table._get_internal('111')
      assert_nil(res1)
      res2 = @test_table._get_internal('112')
      assert_nil(res2)
    end

    define_test "deleteall with row prefix in hbase:meta should not be allowed." do
      assert_raise(ArgumentError) do
        @meta_table = table('hbase:meta')
        @meta_table.deleteall({ROWPREFIXFILTER => "test_meta"})
      end
    end

    define_test "append should work with value" do
      @test_table.append("123", 'x:cnt2', '123')
      assert_equal("123123", @test_table._append_internal("123", 'x:cnt2', '123'))
    end

    define_test 'append should work without qualifier' do
      @test_table.append('1001', 'x', '123')
      assert_equal('123321', @test_table._append_internal('1001', 'x', '321'))
    end

    #-------------------------------------------------------------------------------
    define_test 'incr should work without qualifier' do
      @test_table.incr('1010', 'x', 123)
      assert_equal(123, @test_table._get_counter_internal('1010', 'x'))
      @test_table.incr('1010', 'x', 123)
      assert_equal(246, @test_table._get_counter_internal('1010', 'x'))
    end

    define_test "get_counter should work with integer keys" do
      @test_table.incr(12345, 'x:cnt')
      assert_kind_of(Fixnum, @test_table._get_counter_internal(12345, 'x:cnt'))
    end

    define_test "get_counter should return nil for non-existent counters" do
      assert_nil(@test_table._get_counter_internal(12345, 'x:qqqq'))
    end
  end

  # Complex data management methods tests
  # rubocop:disable Metrics/ClassLength
  class TableComplexMethodsTest < Test::Unit::TestCase
    include TestHelpers
    include HBaseConstants

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
      @test_table.put(1, "x:\x11", [921].pack("N"))

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

    define_test "count should support STARTROW parameter" do
      count = @test_table.count STARTROW => '4'
      assert(count == 0)
    end

    define_test "count should support STOPROW parameter" do
      count = @test_table.count STOPROW => '0'
      assert(count == 0)
    end

    define_test "count should support COLUMNS parameter" do
      @test_table.put(4, "x:c", "31")
      begin
        count = @test_table.count COLUMNS => [ 'x:c']
        assert(count == 1)
      ensure
        @test_table.deleteall(4, 'x:c')
      end
    end

    define_test "count should support FILTER parameter" do
      count = @test_table.count FILTER => "ValueFilter(=, 'binary:11')"
      assert(count == 1)
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
      res = @test_table._get_internal('1', COLUMN => [ "x:\x11", 'x:a', 'x:b' ])
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:\x11'])
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

    define_test "get should work with non-printable columns and values" do
      res = @test_table._get_internal('1', COLUMNS => [ "x:\x11" ])
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_match(/value=\\x00\\x00\\x03\\x99/, res[ 'x:\x11' ])

      res = @test_table._get_internal('1', COLUMNS => [ "x:\x11:toInt" ])
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_match(/value=921/, res[ 'x:\x11' ])
    end

    define_test "get should work with hash columns spec and TIMESTAMP only" do
      res = @test_table._get_internal('1', TIMESTAMP => @test_ts)
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_nil(res['x:a'])
      assert_not_nil(res['x:b'])
    end

    define_test 'get should work with hash columns spec and TIMESTAMP and' \
                ' AUTHORIZATIONS' do
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

    define_test "get with a block should yield (formatted column, value) pairs" do
      res = {}
      @test_table._get_internal('1') { |col, val| res[col] = val }
      assert_equal([ 'x:\x11', 'x:a', 'x:b' ], res.keys.sort)
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
          @test_table.deleteall(1, 'x:c')
          @test_table.deleteall(1, 'x:d')
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
        @test_table.deleteall(1, 'x:v')
      end
    end

    define_test 'get should work with a custom converter class' do
      @test_table.put(1, 'x:v', 1234)
      begin
        res = @test_table._get_internal('1', 'COLUMNS' =>
                    ['x:v:c(org.apache.hadoop.hbase.util.Bytes).len'])
        assert_not_nil(res)
        assert_kind_of(Hash, res)
        assert_not_nil(res['x:v'])
        assert_not_nil(/value=4/.match(res['x:v']))
      ensure
        # clean up newly added columns for this test only.
        @test_table.deleteall(1, 'x:v')
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

    define_test "scan should support STARTKEY parameter" do
      res = @test_table._scan_internal STARTKEY => '2'
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

    define_test "scan should support ENDROW parameter" do
      res = @test_table._scan_internal ENDROW => '2'
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_nil(res['2'])
    end

    define_test "scan should support ENDKEY parameter" do
      res = @test_table._scan_internal ENDKEY => '2'
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_nil(res['2'])
    end

    define_test 'scan should support ROWPREFIXFILTER parameter (test 1)' do
      res = @test_table._scan_internal ROWPREFIXFILTER => '1'
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['1'])
      assert_not_nil(res['1']['x:a'])
      assert_not_nil(res['1']['x:b'])
      assert_nil(res['2'])
    end

    define_test 'scan should support ROWPREFIXFILTER parameter (test 2)' do
      res = @test_table._scan_internal ROWPREFIXFILTER => '2'
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_nil(res['1'])
      assert_not_nil(res['2'])
      assert_not_nil(res['2']['x:a'])
      assert_not_nil(res['2']['x:b'])
    end

    define_test 'scan should support LIMIT parameter' do
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

    define_test 'scan should support REGION_REPLICA_ID' do
      res = @test_table._scan_internal REGION_REPLICA_ID => 0
      assert_not_nil(res)
    end

    define_test 'scan should support ISOLATION_LEVEL' do
      res = @test_table._scan_internal ISOLATION_LEVEL => 'READ_COMMITTED'
      assert_not_nil(res)
    end

    define_test 'scan should support READ_TYPE parameter' do
      res = @test_table._scan_internal READ_TYPE => 'PREAD'
      assert_not_nil(res)
      res = @test_table._scan_internal READ_TYPE => 'STREAM'
      assert_not_nil(res)
      res = @test_table._scan_internal READ_TYPE => 'DEFAULT'
      assert_not_nil(res)
    end

    define_test 'scan should support ALLOW_PARTIAL_RESULTS' do
      res = @test_table._scan_internal ALLOW_PARTIAL_RESULTS => true
      assert_not_nil(res)
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
      num_rows = 0
      @test_table._scan_internal(args) do # Normal Scan
        num_rows += 1
      end
      assert_equal(num_rows, 2,
                   'Num rows scanned without RAW/VERSIONS are not 2')

      args = { VERSIONS => 10, RAW => true } # Since 4 versions of row with rowkey 2 is been added, we can use any number >= 4 for VERSIONS to scan all 4 versions.
      num_rows = 0
      @test_table._scan_internal(args) do # Raw Scan
        num_rows += 1
      end
      # 5 since , 1 from row key '1' and other 4 from row key '4'
      assert_equal(num_rows, 5,
                   'Num rows scanned without RAW/VERSIONS are not 5')

      @test_table.delete(1, 'x:a')
      args = {}
      num_rows = 0
      @test_table._scan_internal(args) do # Normal Scan
        num_rows += 1
      end
      assert_equal(num_rows, 1,
                   'Num rows scanned without RAW/VERSIONS are not 1')

      args = { VERSIONS => 10, RAW => true }
      num_rows = 0
      @test_table._scan_internal(args) do # Raw Scan
        num_rows += 1
      end
      # 6 since , 2 from row key '1' and other 4 from row key '4'
      assert_equal(num_rows, 6,
                   'Num rows scanned without RAW/VERSIONS are not 5')
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
      assert_equal([rows.keys.size,false], res)
    end
    
    define_test "scan should support COLUMNS with value CONVERTER information" do
      @test_table.put(1, "x:c", [1024].pack('N'))
      @test_table.put(1, "x:d", [98].pack('N'))
      @test_table.put(1, "x:\x11", [712].pack('N'))
      begin
        res = @test_table._scan_internal COLUMNS => ['x:c:toInt', 'x:d:c(org.apache.hadoop.hbase.util.Bytes).toInt', "x:\x11:toInt"]
        assert_not_nil(res)
        assert_kind_of(Hash, res)
        assert_match(/value=1024/, res['1']['x:c'])
        assert_match(/value=98/, res['1']['x:d'])
        assert_match(/value=712/, res['1']['x:\x11'])
      ensure
        # clean up newly added columns for this test only.
        @test_table.deleteall(1, 'x:c')
        @test_table.deleteall(1, 'x:d')
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
        @test_table.deleteall(1, 'x:v')
      end
    end

    define_test "scan should support FILTER with non-ASCII bytes" do
      @test_table.put(4, "x:a", "\x82")
      begin
        res = @test_table._scan_internal FILTER => "SingleColumnValueFilter('x', 'a', >=, 'binary:\x82', true, true)"
        assert_not_equal(res, {}, "Result is empty")
        assert_kind_of(Hash, res)
        assert_not_nil(res['4'])
        assert_not_nil(res['4']['x:a'])
        assert_nil(res['1'])
        assert_nil(res['2'])
      ensure
        # clean up newly added columns for this test only.
        @test_table.deleteall(4, 'x:a')
      end
    end

    define_test "scan hbase meta table" do
      res = table("hbase:meta")._scan_internal
      assert_not_nil(res)
    end

    define_test 'scan should work with a custom converter class' do
      @test_table.put(1, 'x:v', 1234)
      begin
        res = @test_table._scan_internal 'COLUMNS' =>
                    ['x:v:c(org.apache.hadoop.hbase.util.Bytes).len']
        assert_not_nil(res)
        assert_kind_of(Hash, res)
        assert_not_nil(res['1']['x:v'])
        assert_not_nil(/value=4/.match(res['1']['x:v']))
      ensure
        # clean up newly added columns for this test only.
        @test_table.deleteall(1, 'x:v')
      end
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
        @test_table.deleteall('ttlTest', 'x:a')
      end
    end

    define_test 'Split count for a table' do
      @test_table_name = 'table_with_splits'
      create_test_table_with_splits(@test_table_name, SPLITS => %w[10 20 30 40])
      @table = table(@test_table_name)
      splits = @table._get_splits_internal
      # Total splits is 5 but here count is 4 as we ignore implicit empty split.
      assert_equal(4, splits.size)
      assert_equal(%w[10 20 30 40], splits)
      drop_test_table(@test_table_name)
    end

    define_test 'Split count for a table by file' do
      @test_table_name = 'table_with_splits_file'
      @splits_file = 'target/generated-test-sources/splits.txt'
      File.exist?(@splits_file) && File.delete(@splits_file)
      file = File.new(@splits_file, 'w')
      %w[10 20 30 40].each { |item| file.puts item }
      file.close
      create_test_table_with_splits_file(@test_table_name,
                                         SPLITS_FILE => @splits_file)
      @table = table(@test_table_name)
      splits = @table._get_splits_internal
      # Total splits is 5 but here count is 4 as we ignore implicit empty split.
      assert_equal(4, splits.size)
      assert_equal(%w[10 20 30 40], splits)
      drop_test_table(@test_table_name)
      File.delete(@splits_file)
    end

    define_test 'Split count for a empty table' do
      splits = @test_table._get_splits_internal
      # Empty split should not be part of this array.
      assert_equal(0, splits.size)
      assert_equal([], splits)
    end

    define_test 'Split count for a table with region replicas' do
      @test_table_name = 'tableWithRegionReplicas'
      create_test_table_with_region_replicas(@test_table_name, 3,
                                             SPLITS => ['10'])
      @table = table(@test_table_name)
      splits = @table._get_splits_internal
      # In this case, total splits should be 1 even if the number of region
      # replicas is 3.
      assert_equal(1, splits.size)
      assert_equal(['10'], splits)
      drop_test_table(@test_table_name)
    end

    define_test "scan should throw an exception on a disabled table" do
      @test_table.disable
      begin
        assert_raise(RuntimeError) do
          @test_table.scan
        end
      ensure
        @test_table.enable
      end
    end
  end
  # rubocop:enable Metrics/ClassLength
end
