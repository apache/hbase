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

require 'shell'
require 'stringio'
require 'hbase_constants'
require 'hbase/hbase'
require 'hbase/table'

include HBaseConstants

module Hbase
  class AdminHelpersTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_admin_test_table"
      create_test_table(@test_name)
    end

    def teardown
      shutdown
    end

    define_test "exists? should return true when a table exists" do
      assert(command(:exists, 'hbase:meta'))
    end

    define_test "exists? should return false when a table exists" do
      assert(!command(:exists, 'NOT.EXISTS'))
    end

    define_test "enabled? should return true for enabled tables" do
      command(:enable, @test_name)
      assert(command(:is_enabled, @test_name))
    end

    define_test "enabled? should return false for disabled tables" do
      command(:disable, @test_name)
      assert(!command(:is_enabled, @test_name))
    end
  end

  # Simple administration methods tests
  # rubocop:disable Metrics/ClassLength
  class AdminMethodsTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      create_test_table(@test_name)

      # Create table test table name
      @create_test_name = 'hbase_create_table_test_table'
    end

    def teardown
      shutdown
    end

    define_test "list should return a list of tables" do
      list = command(:list)
      assert(list.member?(@test_name))
    end

    define_test "list should not return meta tables" do
      list = command(:list)
      assert(!list.member?('hbase:meta'))
    end

    define_test "list_namespace_tables for the system namespace should return a list of tables" do
      list = command(:list_namespace_tables, 'hbase')
      assert(list.count > 0)
    end

    define_test "list_namespace_tables for the default namespace should return a list of tables" do
      list = command(:list_namespace_tables, 'default')
      assert(list.count > 0)
    end

    define_test 'list_deadservers should return exact count of dead servers' do
      output = capture_stdout { command(:list_deadservers) }
      assert(output.include?('0 row(s)'))
    end

    define_test 'clear_deadservers should show exact row(s) count' do
      output = capture_stdout { command(:clear_deadservers, 'test.server.com,16020,1574583397867') }
      assert(output.include?('1 row(s)'))
    end

    #-------------------------------------------------------------------------------

    define_test "flush should work" do
      command(:flush, 'hbase:meta')
      servers = admin.list_liveservers
      servers.each do |s|
        command(:flush, s.toString)
      end
    end

    #-------------------------------------------------------------------------------

    define_test 'alter_status should work' do
      output = capture_stdout { command(:alter_status, @test_name) }
      assert(output.include?('1/1 regions updated'))
    end

    #-------------------------------------------------------------------------------

    define_test "compact should work" do
      command(:compact, 'hbase:meta')
    end

    #-------------------------------------------------------------------------------

    define_test "compaction_state should work" do
      command(:compaction_state, 'hbase:meta')
    end

    #-------------------------------------------------------------------------------

    define_test "major_compact should work" do
      command(:major_compact, 'hbase:meta')
    end

    #-------------------------------------------------------------------------------

    define_test "split should work" do
      begin
        command(:split, 'hbase:meta', nil)
      rescue org.apache.hadoop.hbase.ipc.RemoteWithExtrasException => e
        puts "can not split hbase:meta"
      end
    end

    #-------------------------------------------------------------------------------

    define_test "drop should fail on non-existent tables" do
      assert_raise(ArgumentError) do
        command(:drop, 'NOT.EXISTS')
      end
    end

    define_test "drop should fail on enabled tables" do
      assert_raise(ArgumentError) do
        command(:drop, @test_name)
      end
    end

    define_test "drop should drop tables" do
      command(:disable, @test_name)
      command(:drop, @test_name)
      assert(!command(:exists, @test_name))
    end

    #-------------------------------------------------------------------------------

    define_test "zk_dump should work" do
      assert_not_nil(admin.zk_dump)
    end

    #-------------------------------------------------------------------------------

    define_test "create should fail with non-string table names" do
      assert_raise(ArgumentError) do
        command(:create, 123, 'xxx')
      end
    end

    define_test "create should fail with non-string/non-hash column args" do
      assert_raise(ArgumentError) do
        command(:create, @create_test_name, 123)
      end
    end

    define_test "create should fail without columns" do
      drop_test_table(@create_test_name)
      assert_raise(ArgumentError) do
        command(:create, @create_test_name)
      end
    end

    define_test "create should fail without columns when called with options" do
      drop_test_table(@create_test_name)
      assert_raise(ArgumentError) do
        command(:create, @create_test_name, { OWNER => 'a' })
      end
    end

    define_test "create should work with string column args" do
      drop_test_table(@create_test_name)
      command(:create, @create_test_name, 'a', 'b')
      assert_equal(['a:', 'b:'], table(@create_test_name).get_all_columns.sort)
    end

    define_test "create should work with hash column args" do
      drop_test_table(@create_test_name)
      command(:create, @create_test_name, { NAME => 'a'}, { NAME => 'b'})
      assert_equal(['a:', 'b:'], table(@create_test_name).get_all_columns.sort)
    end

    define_test "create should be able to set column options" do
      drop_test_table(@create_test_name)
      command(:create, @create_test_name,
            { NAME => 'a',
              CACHE_BLOOMS_ON_WRITE => 'TRUE',
              CACHE_INDEX_ON_WRITE => 'TRUE',
              EVICT_BLOCKS_ON_CLOSE => 'TRUE',
              COMPRESSION_COMPACT => 'GZ'})
      assert_equal(['a:'], table(@create_test_name).get_all_columns.sort)
      assert_match(/CACHE_BLOOMS_ON_WRITE/, admin.describe(@create_test_name))
      assert_match(/CACHE_INDEX_ON_WRITE/, admin.describe(@create_test_name))
      assert_match(/EVICT_BLOCKS_ON_CLOSE/, admin.describe(@create_test_name))
      assert_match(/GZ/, admin.describe(@create_test_name))
    end

    define_test "create should be able to set table options" do
      drop_test_table(@create_test_name)
      command(:create, @create_test_name, 'a', 'b', 'MAX_FILESIZE' => 12345678,
              OWNER => '987654321',
              PRIORITY => '77',
              FLUSH_POLICY => 'org.apache.hadoop.hbase.regionserver.FlushAllLargeStoresPolicy',
              REGION_MEMSTORE_REPLICATION => 'TRUE',
              SPLIT_POLICY => 'org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy',
              COMPACTION_ENABLED => 'false',
              SPLIT_ENABLED => 'false',
              MERGE_ENABLED => 'false')
      assert_equal(['a:', 'b:'], table(@create_test_name).get_all_columns.sort)
      assert_match(/12345678/, admin.describe(@create_test_name))
      assert_match(/987654321/, admin.describe(@create_test_name))
      assert_match(/77/, admin.describe(@create_test_name))
      assert_match(/'COMPACTION_ENABLED' => 'false'/, admin.describe(@create_test_name))
      assert_match(/'SPLIT_ENABLED' => 'false'/, admin.describe(@create_test_name))
      assert_match(/'MERGE_ENABLED' => 'false'/, admin.describe(@create_test_name))
      assert_match(/'REGION_MEMSTORE_REPLICATION' => 'true'/, admin.describe(@create_test_name))
      assert_match(/org.apache.hadoop.hbase.regionserver.FlushAllLargeStoresPolicy/,
        admin.describe(@create_test_name))
      assert_match(/org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy/,
        admin.describe(@create_test_name))
    end

    define_test "create should ignore table_att" do
      drop_test_table(@create_test_name)
      command(:create, @create_test_name, 'a', 'b', METHOD => 'table_att', OWNER => '987654321')
      assert_equal(['a:', 'b:'], table(@create_test_name).get_all_columns.sort)
      assert_match(/987654321/, admin.describe(@create_test_name))
    end

    define_test "create should work with SPLITALGO" do
      drop_test_table(@create_test_name)
      command(:create, @create_test_name, 'a', 'b',
              {NUMREGIONS => 10, SPLITALGO => 'HexStringSplit'})
      assert_equal(['a:', 'b:'], table(@create_test_name).get_all_columns.sort)
    end

    define_test "create should work when attributes value 'false' is not enclosed in single quotation marks" do
      drop_test_table(@create_test_name)
      command(:create, @create_test_name, {NAME => 'a', BLOCKCACHE => false},
              COMPACTION_ENABLED => false,
              SPLIT_ENABLED => false,
              MERGE_ENABLED => false)
      assert_equal(['a:'], table(@create_test_name).get_all_columns.sort)
      assert_match(/BLOCKCACHE => 'false'/, admin.describe(@create_test_name))
      assert_match(/'COMPACTION_ENABLED' => 'false'/, admin.describe(@create_test_name))
      assert_match(/'SPLIT_ENABLED' => 'false'/, admin.describe(@create_test_name))
      assert_match(/'MERGE_ENABLED' => 'false'/, admin.describe(@create_test_name))
    end

    #-------------------------------------------------------------------------------

    define_test "describe should fail for non-existent tables" do
      assert_raise(ArgumentError) do
        admin.describe('NOT.EXISTS')
      end
    end

    define_test 'describe should return a description and quotas' do
      drop_test_table(@create_test_name)
      command(:create, @create_test_name, 'cf1', 'cf2')
      command(:set_quota,
              TYPE => SPACE,
              LIMIT => '1G',
              POLICY => NO_INSERTS,
              TABLE => @create_test_name)
      output = capture_stdout { command(:describe, @create_test_name) }

      assert(output.include?("Table #{@create_test_name} is ENABLED"))
      assert(output.include?('COLUMN FAMILIES DESCRIPTION'))
      assert(output.include?("NAME => 'cf1'"))
      assert(output.include?("NAME => 'cf2'"))
      assert(output.include?('2 row(s)'))

      assert(output.include?('QUOTAS'))
      assert(output.include?('LIMIT => 1.00G'))
      assert(output.include?('VIOLATION_POLICY => NO_INSERTS'))
      assert(output.include?('TYPE => SPACE'))
      assert(output.include?('1 row(s)'))

      command(:set_quota,
              TYPE => SPACE,
              LIMIT => NONE,
              TABLE => @create_test_name)
      output = capture_stdout { command(:describe, @create_test_name) }
      assert(output.include?('0 row(s)'))
    end

    define_test 'describe_namespace should return a description and quotas' do
      ns = @create_test_name
      command(:create_namespace, ns)
      command(:set_quota,
              TYPE => SPACE,
              LIMIT => '1G',
              POLICY => NO_INSERTS,
              NAMESPACE => ns)
      output = capture_stdout { command(:describe_namespace, ns) }
      puts output

      assert(output.include?('DESCRIPTION'))
      assert(output.include?("NAME => '#{ns}'"))

      assert(output.include?('QUOTAS'))
      assert(output.include?('LIMIT => 1.00G'))
      assert(output.include?('VIOLATION_POLICY => NO_INSERTS'))
      assert(output.include?('TYPE => SPACE'))
      assert(output.include?('1 row(s)'))

      command(:set_quota,
              TYPE => SPACE,
              LIMIT => NONE,
              NAMESPACE => ns)
      output = capture_stdout { command(:describe_namespace, ns) }
      assert(output.include?('0 row(s)'))
    end

    define_test 'describe_namespace should return quota disabled' do
      ns = 'ns'
      quota_table = ::HBaseQuotasConstants::QUOTA_TABLE_NAME.to_s
      drop_test_table(quota_table)
      command(:create_namespace, ns)
      output = capture_stdout { command(:describe_namespace, ns) }
      # re-creating quota table otherwise other test case may fail
      command(:create, quota_table, 'q', 'u')
      assert(output.include?('Quota is disabled'))
    end

    #-------------------------------------------------------------------------------

    define_test "truncate should empty a table" do
      table(@test_name).put(1, "x:a", 1)
      table(@test_name).put(2, "x:a", 2)
      assert_equal(2, table(@test_name)._count_internal)
      # This is hacky.  Need to get the configuration into admin instance
      command(:truncate, @test_name)
      assert_equal(0, table(@test_name)._count_internal)
    end

    define_test "truncate should yield log records" do
      output = capture_stdout { command(:truncate, @test_name) }
      assert(!output.empty?)
    end

    #-------------------------------------------------------------------------------

    define_test "truncate_preserve should empty a table" do
      table(@test_name).put(1, "x:a", 1)
      table(@test_name).put(2, "x:a", 2)
      assert_equal(2, table(@test_name)._count_internal)
      # This is hacky.  Need to get the configuration into admin instance
      command(:truncate_preserve, @test_name)
      assert_equal(0, table(@test_name)._count_internal)
    end

    define_test "truncate_preserve should yield log records" do
      output = capture_stdout { command(:truncate_preserve, @test_name) }
      assert(!output.empty?)
    end

    define_test "truncate_preserve should maintain the previous region boundaries" do
      drop_test_table(@create_test_name)
      admin.create(@create_test_name, 'a', {NUMREGIONS => 10, SPLITALGO => 'HexStringSplit'})
      splits = table(@create_test_name)._get_splits_internal()
      command(:truncate_preserve, @create_test_name)
      assert_equal(splits, table(@create_test_name)._get_splits_internal())
    end

    define_test "truncate_preserve should be fine when truncateTable method doesn't support" do
      drop_test_table(@create_test_name)
      admin.create(@create_test_name, 'a', {NUMREGIONS => 10, SPLITALGO => 'HexStringSplit'})
      splits = table(@create_test_name)._get_splits_internal()
      $TEST_CLUSTER.getConfiguration.setBoolean("hbase.client.truncatetable.support", false)
      admin.truncate_preserve(@create_test_name, $TEST_CLUSTER.getConfiguration)
      assert_equal(splits, table(@create_test_name)._get_splits_internal())
    end

    #-------------------------------------------------------------------------------

    define_test 'enable and disable tables by regex' do
      @t1 = 't1'
      @t2 = 't11'
      @regex = 't1.*'
      command(:create, @t1, 'f')
      command(:create, @t2, 'f')
      admin.disable_all(@regex)
      assert(command(:is_disabled, @t1))
      assert(command(:is_disabled, @t2))
      admin.enable_all(@regex)
      assert(command(:is_enabled, @t1))
      assert(command(:is_enabled, @t2))
      admin.disable_all(@regex)
      admin.drop_all(@regex)
      assert(!command(:exists, @t1))
      assert(!command(:exists, @t2))
    end

    #-------------------------------------------------------------------------------

    define_test "list_regions should fail for disabled table" do
      drop_test_table(@create_test_name)
      admin.create(@create_test_name, 'a')
      command(:disable, @create_test_name)
      assert(:is_disabled, @create_test_name)
      assert_raise(RuntimeError) do
        command(:list_regions, @create_test_name)
      end
    end
  end
  # rubocop:enable Metrics/ClassLength

  # Simple administration methods tests
  class AdminCloneTableSchemaTest < Test::Unit::TestCase
    include TestHelpers
    def setup
      setup_hbase
      # Create table test table name
      @source_table_name = 'hbase_shell_tests_source_table_name'
      @destination_table_name = 'hbase_shell_tests_destination_table_name'
    end

    def teardown
      shutdown
    end

    define_test "clone_table_schema should create a new table by cloning the
                 existent table schema." do
      drop_test_table(@source_table_name)
      drop_test_table(@destination_table_name)
      command(:create,
              @source_table_name,
              NAME => 'a',
              CACHE_BLOOMS_ON_WRITE => 'TRUE',
              CACHE_INDEX_ON_WRITE => 'TRUE',
              EVICT_BLOCKS_ON_CLOSE => 'TRUE',
              COMPRESSION_COMPACT => 'GZ')
      command(:clone_table_schema,
              @source_table_name,
              @destination_table_name,
              false)
      assert_equal(['a:'],
                   table(@source_table_name).get_all_columns.sort)
      assert_match(/CACHE_BLOOMS_ON_WRITE/,
                   admin.describe(@destination_table_name))
      assert_match(/CACHE_INDEX_ON_WRITE/,
                   admin.describe(@destination_table_name))
      assert_match(/EVICT_BLOCKS_ON_CLOSE/,
                   admin.describe(@destination_table_name))
      assert_match(/GZ/,
                   admin.describe(@destination_table_name))
    end

    define_test "clone_table_schema should maintain the source table's region
                 boundaries when preserve_splits set to true" do
      drop_test_table(@source_table_name)
      drop_test_table(@destination_table_name)
      command(:create,
              @source_table_name,
              'a',
              NUMREGIONS => 10,
              SPLITALGO => 'HexStringSplit')
      splits = table(@source_table_name)._get_splits_internal
      command(:clone_table_schema,
              @source_table_name,
              @destination_table_name,
              true)
      assert_equal(splits, table(@destination_table_name)._get_splits_internal)
    end

    define_test "clone_table_schema should have failed when source table
                 doesn't exist." do
      drop_test_table(@source_table_name)
      drop_test_table(@destination_table_name)
      assert_raise(RuntimeError) do
        command(:clone_table_schema,
                @source_table_name,
                @destination_table_name)
      end
    end

    define_test "clone_table_schema should have failed when destination
                 table exists." do
      drop_test_table(@source_table_name)
      drop_test_table(@destination_table_name)
      command(:create, @source_table_name, 'a')
      command(:create, @destination_table_name, 'a')
      assert_raise(RuntimeError) do
        command(:clone_table_schema,
                @source_table_name,
                @destination_table_name)
      end
    end
  end
  # rubocop:enable Metrics/ClassLength
end
