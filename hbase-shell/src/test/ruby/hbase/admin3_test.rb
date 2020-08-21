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
  # Simple administration methods tests
  class AdminRegionTest < Test::Unit::TestCase
    include TestHelpers
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
