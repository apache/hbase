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
  class ReopenRegionsTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_reopen_regions_test_table"
      create_test_table(@test_name)
    end

    def teardown
      shutdown
    end

    define_test "reopen_regions should work for all regions" do
      assert_nothing_raised do
        command(:reopen_regions, @test_name)
      end
    end

    define_test "reopen_regions should work for specific regions" do
      # Get Java Admin to fetch regions
      java_admin = admin.instance_variable_get(:@admin)
      regions = java_admin.getRegions(org.apache.hadoop.hbase.TableName.valueOf(@test_name))
      
      assert(regions.size > 0, "Test table should have regions")
      region_encoded_name = regions.get(0).getEncodedName
      
      assert_nothing_raised do
        command(:reopen_regions, @test_name, [region_encoded_name])
      end
    end
    
    define_test "reopen_regions should raise error for non-existent region" do
      assert_raise(ArgumentError) do
        command(:reopen_regions, @test_name, ['non-existent-region'])
      end
    end
  end
end
