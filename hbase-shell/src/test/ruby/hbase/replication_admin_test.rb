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
require 'hbase_constants'
require 'hbase/hbase'
require 'hbase/table'

include HBaseConstants

module Hbase
  class ReplicationAdminTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      @peer_id = '1'

      setup_hbase

      assert_equal(0, command(:list_peers).length)
    end

    def teardown
      assert_equal(0, command(:list_peers).length)

      shutdown
    end

    define_test "add_peer: should fail when args isn't specified" do
      assert_raise(ArgumentError) do
        command(:add_peer, @peer_id, nil)
      end
    end

    define_test "add_peer: fail when neither CLUSTER_KEY nor ENDPOINT_CLASSNAME are specified" do
      assert_raise(ArgumentError) do
        args = {}
        command(:add_peer, @peer_id, args)
      end
    end

    define_test "add_peer: args must be a hash" do
      assert_raise(ArgumentError) do
        command(:add_peer, @peer_id, 1)
      end
      assert_raise(ArgumentError) do
        command(:add_peer, @peer_id, ['test'])
      end
      assert_raise(ArgumentError) do
        command(:add_peer, @peer_id, 'test')
      end
    end

    define_test "add_peer: single zk cluster key" do
      cluster_key = "server1.cie.com:2181:/hbase"

      command(:add_peer, @peer_id, {CLUSTER_KEY => cluster_key})

      assert_equal(1, command(:list_peers).length)
      assert(command(:list_peers).key?(@peer_id))
      assert_equal(cluster_key, command(:list_peers).fetch(@peer_id).get_cluster_key)

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "add_peer: multiple zk cluster key" do
      cluster_key = "zk1,zk2,zk3:2182:/hbase-prod"

      command(:add_peer, @peer_id, {CLUSTER_KEY => cluster_key})

      assert_equal(1, command(:list_peers).length)
      assert(command(:list_peers).key?(@peer_id))
      assert_equal(cluster_key, command(:list_peers).fetch(@peer_id).get_cluster_key)

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "add_peer: single zk cluster key - peer config" do
      cluster_key = "server1.cie.com:2181:/hbase"

      args = { CLUSTER_KEY => cluster_key }
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      assert(command(:list_peers).key?(@peer_id))
      assert_equal(cluster_key, command(:list_peers).fetch(@peer_id).get_cluster_key)

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "add_peer: multiple zk cluster key - peer config" do
      cluster_key = "zk1,zk2,zk3:2182:/hbase-prod"

      args = { CLUSTER_KEY => cluster_key }
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      assert(command(:list_peers).key?(@peer_id))
      assert_equal(cluster_key, command(:list_peers).fetch(@peer_id).get_cluster_key)

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "add_peer: multiple zk cluster key and namespaces" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2", "ns3"]
      namespaces_str = "ns2;ns1;ns3"

      args = { CLUSTER_KEY => cluster_key, NAMESPACES => namespaces }
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      assert(command(:list_peers).key?(@peer_id))
      peer_config = command(:list_peers).fetch(@peer_id)
      assert_equal(cluster_key, peer_config.get_cluster_key)
      assert_equal(namespaces_str,
        replication_admin.show_peer_namespaces(peer_config))

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "add_peer: multiple zk cluster key and namespaces, table_cfs" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2"]
      table_cfs = { "ns3:table1" => [], "ns3:table2" => ["cf1"],
        "ns3:table3" => ["cf1", "cf2"] }
      namespaces_str = "ns2;ns1"
      table_cfs_str = "ns3.table1;ns3.table3:cf1,cf2;ns3.table2:cf1"

      args = { CLUSTER_KEY => cluster_key, NAMESPACES => namespaces,
        TABLE_CFS => table_cfs }
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      assert(command(:list_peers).key?(@peer_id))
      peer_config = command(:list_peers).fetch(@peer_id)
      assert_equal(cluster_key, peer_config.get_cluster_key)
      assert_equal(namespaces_str,
        replication_admin.show_peer_namespaces(peer_config))
      assert_equal(table_cfs_str, command(:show_peer_tableCFs, @peer_id))

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "add_peer: multiple zk cluster key and table_cfs - peer config" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      table_cfs = { "table1" => [], "table2" => ["cf1"], "table3" => ["cf1", "cf2"] }
      #table_cfs_str = "default.table1;default.table3:cf1,cf2;default.table2:cf1"

      args = { CLUSTER_KEY => cluster_key, TABLE_CFS => table_cfs }
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      assert(command(:list_peers).key?(@peer_id))
      assert_equal(cluster_key, command(:list_peers).fetch(@peer_id).get_cluster_key)

      # Note: below assertion is dependent on the sort order of an unordered
      # map and hence flaky depending on JVM
      # Commenting out until HBASE-16274 is worked.
      # assert_equal(table_cfs_str, command(:show_peer_tableCFs, @peer_id))

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "add_peer: should fail when args is a hash and peer_tableCFs provided" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      table_cfs_str = "table1;table2:cf1;table3:cf1,cf2"

      assert_raise(ArgumentError) do
        args = { CLUSTER_KEY => cluster_key }
        command(:add_peer, @peer_id, args, table_cfs_str)
      end
    end

    define_test "set_peer_namespaces: works with namespaces array" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2"]
      namespaces_str = "ns2;ns1"

      args = { CLUSTER_KEY => cluster_key }
      command(:add_peer, @peer_id, args)

      # Normally the ReplicationSourceManager will call ReplicationPeer#peer_added
      # but here we have to do it ourselves
      replication_admin.peer_added(@peer_id)

      command(:set_peer_namespaces, @peer_id, namespaces)

      assert_equal(1, command(:list_peers).length)
      assert(command(:list_peers).key?(@peer_id))
      peer_config = command(:list_peers).fetch(@peer_id)
      assert_equal(namespaces_str,
        replication_admin.show_peer_namespaces(peer_config))

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "get_peer_config: works with simple clusterKey peer" do
      cluster_key = "localhost:2181:/hbase-test"
      args = { CLUSTER_KEY => cluster_key }
      command(:add_peer, @peer_id, args)
      peer_config = command(:get_peer_config, @peer_id)
      assert_equal(cluster_key, peer_config.get_cluster_key)
      #cleanup
      command(:remove_peer, @peer_id)
    end

    define_test "get_peer_config: works with replicationendpointimpl peer and config params" do
      cluster_key = 'localhost:2181:/hbase-test'
      repl_impl = 'org.apache.hadoop.hbase.replication.ReplicationEndpointForTest'
      config_params = { "config1" => "value1", "config2" => "value2" }
      args = { CLUSTER_KEY => cluster_key, ENDPOINT_CLASSNAME => repl_impl,
               CONFIG => config_params }
      command(:add_peer, @peer_id, args)
      peer_config = command(:get_peer_config, @peer_id)
      assert_equal(cluster_key, peer_config.get_cluster_key)
      assert_equal(repl_impl, peer_config.get_replication_endpoint_impl)
      assert_equal(2, peer_config.get_configuration.size)
      assert_equal("value1", peer_config.get_configuration.get("config1"))
      #cleanup
      command(:remove_peer, @peer_id)
    end

    define_test "list_peer_configs: returns all peers' ReplicationPeerConfig objects" do
      cluster_key = "localhost:2181:/hbase-test"
      args = { CLUSTER_KEY => cluster_key }
      peer_id_second = '2'
      command(:add_peer, @peer_id, args)

      repl_impl = "org.apache.hadoop.hbase.replication.ReplicationEndpointForTest"
      config_params = { "config1" => "value1", "config2" => "value2" }
      args2 = { ENDPOINT_CLASSNAME => repl_impl, CONFIG => config_params}
      command(:add_peer, peer_id_second, args2)

      peer_configs = command(:list_peer_configs)
      assert_equal(2, peer_configs.size)
      assert_equal(cluster_key, peer_configs.get(@peer_id).get_cluster_key)
      assert_equal(repl_impl, peer_configs.get(peer_id_second).get_replication_endpoint_impl)
      #cleanup
      command(:remove_peer, @peer_id)
      command(:remove_peer, peer_id_second)
    end

    define_test "update_peer_config: can update peer config and data" do
      repl_impl = "org.apache.hadoop.hbase.replication.ReplicationEndpointForTest"
      config_params = { "config1" => "value1", "config2" => "value2" }
      data_params = {"data1" => "value1", "data2" => "value2"}
      args = { ENDPOINT_CLASSNAME => repl_impl, CONFIG => config_params, DATA => data_params}
      command(:add_peer, @peer_id, args)

      #Normally the ReplicationSourceManager will call ReplicationPeer#peer_added, but here we have to do it ourselves
      replication_admin.peer_added(@peer_id)

      new_config_params = { "config1" => "new_value1" }
      new_data_params = {"data1" => "new_value1"}
      new_args = {CONFIG => new_config_params, DATA => new_data_params}
      command(:update_peer_config, @peer_id, new_args)

      #Make sure the updated key/value pairs in config and data were successfully updated, and that those we didn't
      #update are still there and unchanged
      peer_config = command(:get_peer_config, @peer_id)
      command(:remove_peer, @peer_id)
      assert_equal("new_value1", peer_config.get_configuration.get("config1"))
      assert_equal("value2", peer_config.get_configuration.get("config2"))
      assert_equal("new_value1", Bytes.to_string(peer_config.get_peer_data.get(Bytes.toBytes("data1"))))
      assert_equal("value2", Bytes.to_string(peer_config.get_peer_data.get(Bytes.toBytes("data2"))))
    end

    # assert_raise fails on native exceptions - https://jira.codehaus.org/browse/JRUBY-5279
    # Can't catch native Java exception with assert_raise in JRuby 1.6.8 as in the test below.
    # define_test "add_peer: adding a second peer with same id should error" do
    #   command(:add_peer, @peer_id, '')
    #   assert_equal(1, command(:list_peers).length)
    #
    #   assert_raise(java.lang.IllegalArgumentException) do
    #     command(:add_peer, @peer_id, '')
    #   end
    #
    #   assert_equal(1, command(:list_peers).length, 1)
    #
    #   # cleanup for future tests
    #   command(:remove_peer, @peer_id)
    # end
  end
end
