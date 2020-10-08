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
require 'hbase_constants'

java_import 'org.apache.hadoop.hbase.HRegionLocation'
java_import 'org.apache.hadoop.hbase.HRegionInfo'
java_import 'org.apache.hadoop.hbase.ServerName'

module Hbase
  class NoClusterListRegionsTest < Test::Unit::TestCase
    include TestHelpers
    include HBaseConstants

    define_test 'valid_locality_values' do
      command = ::Shell::Commands::ListRegions.new(nil)
      # Validation that a float is received is done elsewhere
      assert command.valid_locality_threshold?(0.999)
      assert command.valid_locality_threshold?(0.001)
      assert command.valid_locality_threshold?(1.0)
      assert command.valid_locality_threshold?(0.0)
      assert_equal false, command.valid_locality_threshold?(2.0)
      assert_equal false, command.valid_locality_threshold?(100.0)
    end

    define_test 'acceptable_server_names' do
      command = ::Shell::Commands::ListRegions.new(nil)
      assert command.accept_server_name?('host.domain.com', 'host.domain.com')
      assert command.accept_server_name?('host.domain', 'host.domain.com')
      assert command.accept_server_name?('host.dom', 'host.domain.com')
      assert command.accept_server_name?('host1', 'host1.domain.com')
      assert_equal false, command.accept_server_name?('host2', 'host1.domain.com')
      assert_equal false, command.accept_server_name?('host2.domain', 'host1.domain.com')
      assert_equal false, command.accept_server_name?('host1.niamod', 'host1.domain.com')
    end

    define_test 'valid_region_localities' do
      command = ::Shell::Commands::ListRegions.new(nil)
      assert command.accept_region_for_locality?(0.5, 0.8)
      assert command.accept_region_for_locality?(0.8, 0.8)
      assert command.accept_region_for_locality?(0.0, 1.0)
      assert command.accept_region_for_locality?(1.0, 1.0)
      assert_equal false, command.accept_region_for_locality?(0.01, 0.001)
      assert_equal false, command.accept_region_for_locality?(1.0, 0.8)
      assert_equal false, command.accept_region_for_locality?(1.0, 0.999)
      assert_equal false, command.accept_region_for_locality?(0.5, 0.3)
    end

    define_test 'filter nondesired servers' do
      command = ::Shell::Commands::ListRegions.new(nil)
      server1 = create_region_location('server1,16020,1234')
      server2 = create_region_location('server2,16020,1234')
      server3 = create_region_location('server3,16020,1234')
      assert_equal [server2], command.get_regions_for_server([server1, server2, server3], 'server2')
      assert_equal [server3], command.get_regions_for_server([server1, server2, server3], 'server3')
      assert_equal [], command.get_regions_for_server([server1, server2, server3], 'server5')
    end

    def create_region_location(server_name)
      HRegionLocation.new(HRegionInfo.new(TableName.valueOf('t1')), ServerName.valueOf(server_name))
    end
  end
end
