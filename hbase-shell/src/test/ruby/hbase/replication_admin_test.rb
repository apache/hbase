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
require 'shell/formatter'
require 'hbase'
require 'hbase/hbase'
require 'hbase/table'

include HBaseConstants

module Hbase
  class ReplicationAdminTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      @test_name = "hbase_shell_tests_table"
      @peer_id = '1'

      setup_hbase
      drop_test_table(@test_name)
      create_test_table(@test_name)

      assert_equal(0, replication_admin.list_peers.length)
    end

    def teardown
      assert_equal(0, replication_admin.list_peers.length)

      shutdown
    end

    define_test "add_peer: should fail when args isn't specified" do
      assert_raise(ArgumentError) do
        replication_admin.add_peer(@peer_id, nil)
      end
    end

    define_test "add_peer: fail when neither CLUSTER_KEY nor ENDPOINT_CLASSNAME are specified" do
      assert_raise(ArgumentError) do
        args = {}
        replication_admin.add_peer(@peer_id, args)
      end
    end

    define_test "add_peer: fail when both CLUSTER_KEY and ENDPOINT_CLASSNAME are specified" do
      assert_raise(ArgumentError) do
        args = { CLUSTER_KEY => 'zk1,zk2,zk3:2182:/hbase-prod',
                 ENDPOINT_CLASSNAME => 'org.apache.hadoop.hbase.MyReplicationEndpoint' }
        replication_admin.add_peer(@peer_id, args)
      end
    end

    define_test "add_peer: args must be a string or number" do
      assert_raise(ArgumentError) do
        replication_admin.add_peer(@peer_id, 1)
      end
      assert_raise(ArgumentError) do
        replication_admin.add_peer(@peer_id, ['test'])
      end
    end

    define_test "add_peer: single zk cluster key" do
      cluster_key = "server1.cie.com:2181:/hbase"

      replication_admin.add_peer(@peer_id, cluster_key)

      assert_equal(1, replication_admin.list_peers.length)
      assert(replication_admin.list_peers.key?(@peer_id))
      assert_equal(cluster_key, replication_admin.list_peers.fetch(@peer_id))

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "add_peer: multiple zk cluster key" do
      cluster_key = "zk1,zk2,zk3:2182:/hbase-prod"

      replication_admin.add_peer(@peer_id, cluster_key)

      assert_equal(1, replication_admin.list_peers.length)
      assert(replication_admin.list_peers.key?(@peer_id))
      assert_equal(replication_admin.list_peers.fetch(@peer_id), cluster_key)

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "add_peer: multiple zk cluster key and table_cfs" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      table_cfs_str = "table1;table2:cf1;table3:cf2,cf3"

      replication_admin.add_peer(@peer_id, cluster_key, table_cfs_str)

      assert_equal(1, replication_admin.list_peers.length)
      assert(replication_admin.list_peers.key?(@peer_id))
      assert_equal(cluster_key, replication_admin.list_peers.fetch(@peer_id))
      assert_equal(table_cfs_str, replication_admin.show_peer_tableCFs(@peer_id))

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "add_peer: single zk cluster key - peer config" do
      cluster_key = "server1.cie.com:2181:/hbase"

      args = { CLUSTER_KEY => cluster_key }
      replication_admin.add_peer(@peer_id, args)

      assert_equal(1, replication_admin.list_peers.length)
      assert(replication_admin.list_peers.key?(@peer_id))
      assert_equal(cluster_key, replication_admin.list_peers.fetch(@peer_id))

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "add_peer: multiple zk cluster key - peer config" do
      cluster_key = "zk1,zk2,zk3:2182:/hbase-prod"

      args = { CLUSTER_KEY => cluster_key }
      replication_admin.add_peer(@peer_id, args)

      assert_equal(1, replication_admin.list_peers.length)
      assert(replication_admin.list_peers.key?(@peer_id))
      assert_equal(cluster_key, replication_admin.list_peers.fetch(@peer_id))

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "add_peer: multiple zk cluster key and table_cfs - peer config" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      table_cfs = { "table1" => [], "table2" => ["cf1"], "table3" => ["cf1", "cf2"] }
      table_cfs_str = "table1;table2:cf1;table3:cf1,cf2"

      args = { CLUSTER_KEY => cluster_key, TABLE_CFS => table_cfs }
      replication_admin.add_peer(@peer_id, args)

      assert_equal(1, replication_admin.list_peers.length)
      assert(replication_admin.list_peers.key?(@peer_id))
      assert_equal(cluster_key, replication_admin.list_peers.fetch(@peer_id))
      assert_equal(table_cfs_str, replication_admin.show_peer_tableCFs(@peer_id))

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "add_peer: should fail when args is a hash and peer_tableCFs provided" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      table_cfs_str = "table1;table2:cf1;table3:cf1,cf2"

      assert_raise(ArgumentError) do
        args = { CLUSTER_KEY => cluster_key }
        replication_admin.add_peer(@peer_id, args, table_cfs_str)
      end
    end

    # assert_raise fails on native exceptions - https://jira.codehaus.org/browse/JRUBY-5279
    # Can't catch native Java exception with assert_raise in JRuby 1.6.8 as in the test below.
    # define_test "add_peer: adding a second peer with same id should error" do
    #   replication_admin.add_peer(@peer_id, '')
    #   assert_equal(1, replication_admin.list_peers.length)
    #
    #   assert_raise(java.lang.IllegalArgumentException) do
    #     replication_admin.add_peer(@peer_id, '')
    #   end
    #
    #   assert_equal(1, replication_admin.list_peers.length, 1)
    #
    #   # cleanup for future tests
    #   replication_admin.remove_peer(@peer_id)
    # end
  end
end
