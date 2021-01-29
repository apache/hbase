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

require 'hbase_shell'
require 'stringio'
require 'hbase_constants'
require 'hbase/hbase'
require 'hbase/table'


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
    include HBaseConstants
    include HBaseQuotasConstants

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
      deadservers = []
      output = capture_stdout { deadservers = command(:clear_deadservers, 'test.server.com,16020,1574583397867') }
      assert(output.include?('1 row(s)'))
      assert(deadservers[0] == 'test.server.com,16020,1574583397867')
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

    define_test "balance should work" do
      command(:balance_switch, true)
      output = capture_stdout { command(:balancer_enabled) }
      assert(output.include?('true'))

      did_balancer_run = command(:balancer)
      assert(did_balancer_run == true)
      output = capture_stdout { command(:balancer, 'force') }
      assert(output.include?('true'))
    end

    #-------------------------------------------------------------------------------

    define_test "create should fail with non-string table names" do
      assert_raise(ArgumentError) do
        command(:create, 123, 'xxx')
      end
    end

    #-------------------------------------------------------------------------------

    define_test 'snapshot auto cleanup should work' do
      result = nil
      command(:snapshot_cleanup_switch, false)

      # enable snapshot cleanup and check that the previous state is returned
      output = capture_stdout { result = command(:snapshot_cleanup_switch, true) }
      assert(output.include?('false'))
      assert(result == false)

      # check that snapshot_cleanup_enabled returns the current state
      output = capture_stdout { result = command(:snapshot_cleanup_enabled) }
      assert(output.include?('true'))
      assert(result == true)

      # disable snapshot cleanup and check that the previous state is returned
      output = capture_stdout { result = command(:snapshot_cleanup_switch, false) }
      assert(output.include?('true'))
      assert(result == true)

      # check that snapshot_cleanup_enabled returns the current state
      output = capture_stdout { result = command(:snapshot_cleanup_enabled) }
      assert(output.include?('false'))
      assert(result == false)
    end

    #-------------------------------------------------------------------------------

    define_test 'balancer switch should work' do
      result = nil
      command(:balance_switch, false)

      # enable balancer and check that the previous state is returned
      output = capture_stdout { result = command(:balance_switch, true) }
      assert(output.include?('false'))
      assert(result == false)

      # check that balancer_enabled returns the current state
      output = capture_stdout { result = command(:balancer_enabled) }
      assert(output.include?('true'))
      assert(result == true)

      # disable balancer and check that the previous state is returned
      output = capture_stdout { result = command(:balance_switch, false) }
      assert(output.include?('true'))
      assert(result == true)

      # check that balancer_enabled returns the current state
      output = capture_stdout { result = command(:balancer_enabled) }
      assert(output.include?('false'))
      assert(result == false)
    end

    #-------------------------------------------------------------------------------

    define_test 'normalizer switch should work' do
      result = nil
      command(:normalizer_switch, false)

      # enable normalizer and check that the previous state is returned
      output = capture_stdout { result = command(:normalizer_switch, true) }
      assert(output.include?('false'))
      assert(result == false)

      # check that normalizer_enabled returns the current state
      output = capture_stdout { result = command(:normalizer_enabled) }
      assert(output.include?('true'))
      assert(result == true)

      # disable normalizer and check that the previous state is returned
      output = capture_stdout { result = command(:normalizer_switch, false) }
      assert(output.include?('true'))
      assert(result == true)

      # check that normalizer_enabled returns the current state
      output = capture_stdout { result = command(:normalizer_enabled) }
      assert(output.include?('false'))
      assert(result == false)
    end

    #-------------------------------------------------------------------------------

    define_test 'catalogjanitor switch should work' do
      result = nil
      command(:catalogjanitor_switch, false)

      # enable catalogjanitor and check that the previous state is returned
      output = capture_stdout { result = command(:catalogjanitor_switch, true) }
      assert(output.include?('false'))
      assert(result == false)

      # check that catalogjanitor_enabled returns the current state
      output = capture_stdout { result = command(:catalogjanitor_enabled) }
      assert(output.include?('true'))
      assert(result == true)

      # disable catalogjanitor and check that the previous state is returned
      output = capture_stdout { result = command(:catalogjanitor_switch, false) }
      assert(output.include?('true'))
      assert(result == true)

      # check that catalogjanitor_enabled returns the current state
      output = capture_stdout { result = command(:catalogjanitor_enabled) }
      assert(output.include?('false'))
      assert(result == false)
    end

    #-------------------------------------------------------------------------------

    define_test 'cleaner_chore switch should work' do
      result = nil
      command(:cleaner_chore_switch, false)

      # enable cleaner_chore and check that the previous state is returned
      output = capture_stdout { result = command(:cleaner_chore_switch, true) }
      assert(output.include?('false'))
      assert(result == false)

      # check that cleaner_chore_enabled returns the current state
      output = capture_stdout { result = command(:cleaner_chore_enabled) }
      assert(output.include?('true'))
      assert(result == true)

      # disable cleaner_chore and check that the previous state is returned
      output = capture_stdout { result = command(:cleaner_chore_switch, false) }
      assert(output.include?('true'))
      assert(result == true)

      # check that cleaner_chore_enabled returns the current state
      output = capture_stdout { result = command(:cleaner_chore_enabled) }
      assert(output.include?('false'))
      assert(result == false)
    end

    #-------------------------------------------------------------------------------

    define_test 'splitormerge switch should work' do
      # Author's note: All the other feature switches in hbase-shell only toggle one feature. This command operates on
      # both the "SPLIT" and "MERGE", so you will note that both code paths need coverage.
      result = nil
      command(:splitormerge_switch, 'SPLIT', false)
      command(:splitormerge_switch, 'MERGE', true)

      # flip switch and check that the previous state is returned
      output = capture_stdout { result = command(:splitormerge_switch, 'SPLIT', true) }
      assert(output.include?('false'))
      assert(result == false)

      output = capture_stdout { result = command(:splitormerge_switch, 'MERGE', false) }
      assert(output.include?('true'))
      assert(result == true)

      # check that splitormerge_enabled returns the current state
      output = capture_stdout { result = command(:splitormerge_enabled, 'SPLIT') }
      assert(output.include?('true'))
      assert(result == true)

      output = capture_stdout { result = command(:splitormerge_enabled, 'MERGE') }
      assert(output.include?('false'))
      assert(result == false)

      # flip switch and check that the previous state is returned
      output = capture_stdout { result = command(:splitormerge_switch, 'SPLIT', false) }
      assert(output.include?('true'))
      assert(result == true)

      output = capture_stdout { result = command(:splitormerge_switch, 'MERGE', true) }
      assert(output.include?('false'))
      assert(result == false)

      # check that splitormerge_enabled returns the current state
      output = capture_stdout { result = command(:splitormerge_enabled, 'SPLIT') }
      assert(output.include?('false'))
      assert(result == false)

      output = capture_stdout { result = command(:splitormerge_enabled, 'MERGE') }
      assert(output.include?('true'))
      assert(result == true)
    end

    #-------------------------------------------------------------------------------

    define_test 'get slowlog responses should work' do
      output = command(:get_slowlog_responses, '*', {})
      assert(output.nil?)
    end

    #-------------------------------------------------------------------------------

    define_test 'clear slowlog responses should work' do
      output = capture_stdout { command(:clear_slowlog_responses, nil) }
      assert(output.include?('Cleared Slowlog responses from 0/1 RegionServers'))
    end

    #-------------------------------------------------------------------------------

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
              TYPE => ::HBaseQuotasConstants::SPACE,
              LIMIT => '1G',
              POLICY => ::HBaseQuotasConstants::NO_INSERTS,
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
              TYPE => ::HBaseQuotasConstants::SPACE,
              LIMIT => ::HBaseConstants::NONE,
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

    define_test 'truncate should empty a table' do
      table(@test_name).put(1, 'x:a', 1)
      table(@test_name).put(2, 'x:a', 2)
      assert_equal(2, table(@test_name)._count_internal)
      # This is hacky.  Need to get the configuration into admin instance
      command(:truncate, @test_name)
      assert_equal(0, table(@test_name)._count_internal)
    end

    define_test 'truncate should yield log records' do
      output = capture_stdout { command(:truncate, @test_name) }
      assert(!output.empty?)
    end

    define_test 'truncate should work on disabled table' do
      table(@test_name).put(1, 'x:a', 1)
      table(@test_name).put(2, 'x:a', 2)
      assert_equal(2, table(@test_name)._count_internal)
      command(:disable, @test_name)
      command(:truncate, @test_name)
      assert_equal(0, table(@test_name)._count_internal)
    end

    #-------------------------------------------------------------------------------

    define_test 'truncate_preserve should empty a table' do
      table(@test_name).put(1, 'x:a', 1)
      table(@test_name).put(2, 'x:a', 2)
      assert_equal(2, table(@test_name)._count_internal)
      # This is hacky.  Need to get the configuration into admin instance
      command(:truncate_preserve, @test_name)
      assert_equal(0, table(@test_name)._count_internal)
    end

    define_test 'truncate_preserve should yield log records' do
      output = capture_stdout { command(:truncate_preserve, @test_name) }
      assert(!output.empty?)
    end

    define_test 'truncate_preserve should maintain the previous region boundaries' do
      drop_test_table(@create_test_name)
      admin.create(@create_test_name, 'a', {NUMREGIONS => 10, SPLITALGO => 'HexStringSplit'})
      splits = table(@create_test_name)._get_splits_internal()
      command(:truncate_preserve, @create_test_name)
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
      assert(!command(:is_enabled, @t1))
      assert(!command(:is_enabled, @t2))
      admin.enable_all(@regex)
      assert(!command(:is_disabled, @t1))
      assert(!command(:is_disabled, @t2))
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
    include HBaseConstants

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

  # Simple administration methods tests
  class AdminRegionTest < Test::Unit::TestCase
    include TestHelpers
    include HBaseConstants

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      drop_test_table(@test_name)
      create_test_table(@test_name)
    end

    def teardown
      shutdown
    end

    define_test "unassign should allow encoded region names" do
      region = command(:locate_region, @test_name, '')
      regionName = region.getRegionInfo().getRegionNameAsString()
      command(:unassign, regionName, true)
    end

    define_test "unassign should allow non-encoded region names" do
      region = command(:locate_region, @test_name, '')
      encodedRegionName = region.getRegionInfo().getEncodedName()
      command(:unassign, encodedRegionName, true)
    end

    define_test "list regions should allow table name" do
      command(:list_regions, @test_name)
    end

    define_test 'merge regions' do
      @t_name = 'hbase_shell_merge'
      @t_name2 = 'hbase_shell_merge_2'
      drop_test_table(@t_name)
      drop_test_table(@t_name2)
      admin.create(@t_name, 'a', NUMREGIONS => 10, SPLITALGO => 'HexStringSplit')
      r1 = command(:locate_region, @t_name, '1')
      r2 = command(:locate_region, @t_name, '2')
      r3 = command(:locate_region, @t_name, '4')
      r4 = command(:locate_region, @t_name, '5')
      r5 = command(:locate_region, @t_name, '7')
      r6 = command(:locate_region, @t_name, '8')
      region1 = r1.getRegion.getRegionNameAsString
      region2 = r2.getRegion.getRegionNameAsString
      region3 = r3.getRegion.getRegionNameAsString
      region4 = r4.getRegion.getRegionNameAsString
      region5 = r5.getRegion.getRegionNameAsString
      region6 = r6.getRegion.getRegionNameAsString
      # only 1 region
      assert_raise(ArgumentError) do
        command(:merge_region, 'a')
      end
      # only 1 region with force=true
      assert_raise(ArgumentError) do
        command(:merge_region, 'a', true)
      end
      # non-existing region
      assert_raise(RuntimeError) do
        command(:merge_region, 'a','b')
      end
      # duplicate regions
      assert_raise(RuntimeError) do
        command(:merge_region, region1,region1,region1)
      end
      # 3 non-adjacent regions without forcible=true
      assert_raise(RuntimeError) do
        command(:merge_region, region1,region2,region4)
      end
      # 2 adjacent regions
      command(:merge_region, region1,region2)
      # 3 non-adjacent regions with forcible=true
      command(:merge_region, region3,region5,region6, true)

      admin.create(@t_name2, 'a', NUMREGIONS => 5, SPLITALGO => 'HexStringSplit')
      r1 = command(:locate_region, @t_name2, '1')
      r2 = command(:locate_region, @t_name2, '4')
      r3 = command(:locate_region, @t_name2, '7')
      region1 = r1.getRegion.getRegionNameAsString
      region2 = r2.getRegion.getRegionNameAsString
      region3 = r3.getRegion.getRegionNameAsString

      # accept array of regions
      command(:merge_region, [region1,region2,region3])
    end
  end

  # Simple administration methods tests
  # rubocop:disable Metrics/ClassLength
  class AdminAlterTableTest < Test::Unit::TestCase
    include TestHelpers
    include HBaseConstants

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      drop_test_table(@test_name)
      create_test_table(@test_name)
    end

    def teardown
      shutdown
    end

    #-------------------------------------------------------------------------------

    define_test "alter should fail with non-string table names" do
      assert_raise(ArgumentError) do
        command(:alter, 123, METHOD => 'delete', NAME => 'y')
      end
    end

    define_test "alter should fail with non-existing tables" do
      assert_raise(ArgumentError) do
        command(:alter, 'NOT.EXISTS', METHOD => 'delete', NAME => 'y')
      end
    end

    define_test "alter should not fail with enabled tables" do
      command(:enable, @test_name)
      command(:alter, @test_name, METHOD => 'delete', NAME => 'y')
    end

    define_test "alter should be able to delete column families" do
      assert_equal(['x:', 'y:'], table(@test_name).get_all_columns.sort)
      command(:alter, @test_name, METHOD => 'delete', NAME => 'y')
      command(:enable, @test_name)
      assert_equal(['x:'], table(@test_name).get_all_columns.sort)
    end

    define_test "alter should be able to add column families" do
      assert_equal(['x:', 'y:'], table(@test_name).get_all_columns.sort)
      command(:alter, @test_name, NAME => 'z')
      command(:enable, @test_name)
      assert_equal(['x:', 'y:', 'z:'], table(@test_name).get_all_columns.sort)
    end

    define_test "alter should be able to add column families (name-only alter spec)" do
      assert_equal(['x:', 'y:'], table(@test_name).get_all_columns.sort)
      command(:alter, @test_name, 'z')
      command(:enable, @test_name)
      assert_equal(['x:', 'y:', 'z:'], table(@test_name).get_all_columns.sort)
    end

    define_test 'alter should support more than one alteration in one call' do
      assert_equal(['x:', 'y:'], table(@test_name).get_all_columns.sort)
      alter_out_put = capture_stdout do
        command(:alter, @test_name, { NAME => 'z' },
                { METHOD => 'delete', NAME => 'y' },
                'MAX_FILESIZE' => 12_345_678)
      end
      command(:enable, @test_name)
      assert_equal(1, /Updating all regions/.match(alter_out_put).size,
                   "HBASE-15641 - Should only perform one table
                   modification per alter.")
      assert_equal(['x:', 'z:'], table(@test_name).get_all_columns.sort)
      assert_match(/12345678/, admin.describe(@test_name))
    end

    define_test 'alter should be able to set the TargetRegionSize and TargetRegionCount' do
      command(:alter, @test_name, 'NORMALIZER_TARGET_REGION_COUNT' => 156)
      assert_match(/156/, admin.describe(@test_name))
      command(:alter, @test_name, 'NORMALIZER_TARGET_REGION_SIZE' => 234)
      assert_match(/234/, admin.describe(@test_name))
    end

    define_test 'alter should support shortcut DELETE alter specs' do
      assert_equal(['x:', 'y:'], table(@test_name).get_all_columns.sort)
      command(:alter, @test_name, 'delete' => 'y')
      assert_equal(['x:'], table(@test_name).get_all_columns.sort)
    end

    define_test "alter should be able to change table options" do
      command(:alter, @test_name, METHOD => 'table_att', 'MAX_FILESIZE' => 12345678)
      assert_match(/12345678/, admin.describe(@test_name))
    end

    define_test "alter should be able to change table options w/o table_att" do
      command(:alter, @test_name, 'MAX_FILESIZE' => 12345678)
      assert_match(/12345678/, admin.describe(@test_name))
    end

    define_test "alter should be able to change coprocessor attributes" do
      drop_test_table(@test_name)
      create_test_table(@test_name)

      cp_key = "coprocessor"
      class_name = "org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver"

      cp_value = "|" + class_name + "|12|arg1=1,arg2=2"

      # eval() is used to convert a string to regex
      assert_no_match(eval("/" + class_name + "/"), admin.describe(@test_name))
      assert_no_match(eval("/" + cp_key + "/"), admin.describe(@test_name))
      command(:alter, @test_name, 'METHOD' => 'table_att', cp_key => cp_value)
      assert_match(eval("/" + class_name + "/"), admin.describe(@test_name))
      assert_match(eval("/" + cp_key + "\\$(\\d+)/"), admin.describe(@test_name))
    end

    define_test "alter should be able to remove a table attribute" do
      drop_test_table(@test_name)
      create_test_table(@test_name)

      key = "MAX_FILESIZE"
      command(:alter, @test_name, 'METHOD' => 'table_att', key => 12345678)

      # eval() is used to convert a string to regex
      assert_match(eval("/" + key + "/"), admin.describe(@test_name))

      command(:alter, @test_name, 'METHOD' => 'table_att_unset', 'NAME' => key)
      assert_no_match(eval("/" + key + "/"), admin.describe(@test_name))
    end

    define_test "alter should be able to remove a list of table attributes" do
      drop_test_table(@test_name)

      key_1 = "TestAttr1"
      key_2 = "TestAttr2"
      command(:create, @test_name, { NAME => 'i'}, METADATA => { key_1 => 1, key_2 => 2 })

      # eval() is used to convert a string to regex
      assert_match(eval("/" + key_1 + "/"), admin.describe(@test_name))
      assert_match(eval("/" + key_2 + "/"), admin.describe(@test_name))

      command(:alter, @test_name, 'METHOD' => 'table_att_unset', 'NAME' => [ key_1, key_2 ])
      assert_no_match(eval("/" + key_1 + "/"), admin.describe(@test_name))
      assert_no_match(eval("/" + key_2 + "/"), admin.describe(@test_name))
    end

    define_test "alter should be able to remove a list of table attributes when value is empty" do
      drop_test_table(@test_name)

      key_1 = "TestAttr1"
      key_2 = "TestAttr2"
      command(:create, @test_name, { NAME => 'i'}, METADATA => { key_1 => 1, key_2 => 2 })

      # eval() is used to convert a string to regex
      assert_match(eval("/" + key_1 + "/"), admin.describe(@test_name))
      assert_match(eval("/" + key_2 + "/"), admin.describe(@test_name))

      command(:alter, @test_name, METADATA => { key_1 => '', key_2 => '' })
      assert_no_match(eval("/" + key_1 + "/"), admin.describe(@test_name))
      assert_no_match(eval("/" + key_2 + "/"), admin.describe(@test_name))
    end

    define_test "alter should be able to remove a table configuration" do
      drop_test_table(@test_name)
      create_test_table(@test_name)

      key = "TestConf"
      command(:alter, @test_name, CONFIGURATION => {key => 1})

      # eval() is used to convert a string to regex
      assert_match(eval("/" + key + "/"), admin.describe(@test_name))

      command(:alter, @test_name, 'METHOD' => 'table_conf_unset', 'NAME' => key)
      assert_no_match(eval("/" + key + "/"), admin.describe(@test_name))
    end

    define_test "alter should be able to remove a list of table configuration" do
      drop_test_table(@test_name)

      key_1 = "TestConf1"
      key_2 = "TestConf2"
      command(:create, @test_name, { NAME => 'i'}, CONFIGURATION => { key_1 => 1, key_2 => 2 })

      # eval() is used to convert a string to regex
      assert_match(eval("/" + key_1 + "/"), admin.describe(@test_name))
      assert_match(eval("/" + key_2 + "/"), admin.describe(@test_name))

      command(:alter, @test_name, 'METHOD' => 'table_conf_unset', 'NAME' => [ key_1, key_2 ])
      assert_no_match(eval("/" + key_1 + "/"), admin.describe(@test_name))
      assert_no_match(eval("/" + key_2 + "/"), admin.describe(@test_name))
    end

    define_test "alter should be able to remove a list of table configuration  when value is empty" do
      drop_test_table(@test_name)

      key_1 = "TestConf1"
      key_2 = "TestConf2"
      command(:create, @test_name, { NAME => 'i'}, CONFIGURATION => { key_1 => 1, key_2 => 2 })

      # eval() is used to convert a string to regex
      assert_match(eval("/" + key_1 + "/"), admin.describe(@test_name))
      assert_match(eval("/" + key_2 + "/"), admin.describe(@test_name))

      command(:alter, @test_name, CONFIGURATION => { key_1 => '', key_2 => '' })
      assert_no_match(eval("/" + key_1 + "/"), admin.describe(@test_name))
      assert_no_match(eval("/" + key_2 + "/"), admin.describe(@test_name))
    end

    define_test "alter should be able to remove a list of column family configuration when value is empty" do
      drop_test_table(@test_name)

      key_1 = "TestConf1"
      key_2 = "TestConf2"
      command(:create, @test_name, { NAME => 'i', CONFIGURATION => { key_1 => 1, key_2 => 2 }})

      # eval() is used to convert a string to regex
      assert_match(eval("/" + key_1 + "/"), admin.describe(@test_name))
      assert_match(eval("/" + key_2 + "/"), admin.describe(@test_name))

      command(:alter, @test_name, { NAME => 'i', CONFIGURATION => { key_1 => '', key_2 => '' }})
      assert_no_match(eval("/" + key_1 + "/"), admin.describe(@test_name))
      assert_no_match(eval("/" + key_2 + "/"), admin.describe(@test_name))
    end

    define_test "get_table should get a real table" do
      drop_test_table(@test_name)
      create_test_table(@test_name)

      table = table(@test_name)
      assert_not_equal(nil, table)
      table.close
    end
  end
  # rubocop:enable Metrics/ClassLength
end
