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
include Java

java_import org.apache.hadoop.hbase.replication.SyncReplicationState

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
      peer = command(:list_peers).get(0)
      assert_equal(@peer_id, peer.getPeerId)
      assert_equal(cluster_key, peer.getPeerConfig.getClusterKey)
      assert_equal(true, peer.getPeerConfig.replicateAllUserTables)

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "add_peer: multiple zk cluster key" do
      cluster_key = "zk1,zk2,zk3:2182:/hbase-prod"

      command(:add_peer, @peer_id, {CLUSTER_KEY => cluster_key})

      assert_equal(1, command(:list_peers).length)
      peer = command(:list_peers).get(0)
      assert_equal(@peer_id, peer.getPeerId)
      assert_equal(cluster_key, peer.getPeerConfig.getClusterKey)
      assert_equal(true, peer.getPeerConfig.replicateAllUserTables)

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "add_peer: remote wal dir" do
      cluster_key = "server1.cie.com:2181:/hbase"
      remote_wal_dir = "hdfs://srv1:9999/hbase"
      table_cfs = { "ns3:table1" => [], "ns3:table2" => [],
        "ns3:table3" => [] }
      args = { CLUSTER_KEY => cluster_key, REMOTE_WAL_DIR => remote_wal_dir,
        TABLE_CFS => table_cfs}
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      peer = command(:list_peers).get(0)
      assert_equal(@peer_id, peer.getPeerId)
      assert_equal(cluster_key, peer.getPeerConfig.getClusterKey)
      assert_equal(remote_wal_dir, peer.getPeerConfig.getRemoteWALDir)
      assert_tablecfs_equal(table_cfs, peer.getPeerConfig.getTableCFsMap())

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "add_peer: single zk cluster key with enabled/disabled state" do
      cluster_key = "server1.cie.com:2181:/hbase"

      args = { CLUSTER_KEY => cluster_key }
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      assert_equal(@peer_id, command(:list_peers).get(0).getPeerId)
      assert_equal(true, command(:list_peers).get(0).isEnabled)

      command(:remove_peer, @peer_id)

      enable_args = { CLUSTER_KEY => cluster_key, STATE => 'ENABLED' }
      command(:add_peer, @peer_id, enable_args)

      assert_equal(1, command(:list_peers).length)
      assert_equal(@peer_id, command(:list_peers).get(0).getPeerId)
      assert_equal(true, command(:list_peers).get(0).isEnabled)

      command(:remove_peer, @peer_id)

      disable_args = { CLUSTER_KEY => cluster_key, STATE => 'DISABLED' }
      command(:add_peer, @peer_id, disable_args)

      assert_equal(1, command(:list_peers).length)
      assert_equal(@peer_id, command(:list_peers).get(0).getPeerId)
      assert_equal(false, command(:list_peers).get(0).isEnabled)

      command(:remove_peer, @peer_id)
    end

    define_test "add_peer: multiple zk cluster key - peer config" do
      cluster_key = "zk1,zk2,zk3:2182:/hbase-prod"

      args = { CLUSTER_KEY => cluster_key }
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      peer = command(:list_peers).get(0)
      assert_equal(@peer_id, peer.getPeerId)
      assert_equal(cluster_key, peer.getPeerConfig.getClusterKey)
      assert_equal(true, peer.getPeerConfig.replicateAllUserTables)

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "add_peer: multiple zk cluster key and namespaces" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2", "ns3"]
      namespaces_str = "ns1;ns2;ns3"

      args = { CLUSTER_KEY => cluster_key, NAMESPACES => namespaces }
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      peer = command(:list_peers).get(0)
      assert_equal(@peer_id, peer.getPeerId)
      peer_config = peer.getPeerConfig
      assert_equal(false, peer_config.replicateAllUserTables)
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
      namespaces_str = "ns1;ns2"

      args = { CLUSTER_KEY => cluster_key, NAMESPACES => namespaces,
        TABLE_CFS => table_cfs }
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      peer = command(:list_peers).get(0)
      assert_equal(@peer_id, peer.getPeerId)
      peer_config = peer.getPeerConfig
      assert_equal(false, peer_config.replicateAllUserTables)
      assert_equal(cluster_key, peer_config.get_cluster_key)
      assert_equal(namespaces_str,
        replication_admin.show_peer_namespaces(peer_config))
      assert_tablecfs_equal(table_cfs, peer_config.getTableCFsMap())

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    def assert_tablecfs_equal(table_cfs, table_cfs_map)
      assert_equal(table_cfs.length, table_cfs_map.length)
      table_cfs_map.each{|key, value|
        assert(table_cfs.has_key?(key.getNameAsString))
        if table_cfs.fetch(key.getNameAsString).length == 0
          assert_equal(nil, value)
        else
          assert_equal(table_cfs.fetch(key.getNameAsString).length, value.length)
          value.each{|v|
            assert(table_cfs.fetch(key.getNameAsString).include?(v))
          }
        end
      }
    end

    define_test "add_peer: multiple zk cluster key and table_cfs - peer config" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      table_cfs = { "table1" => [], "table2" => ["cf1"], "table3" => ["cf1", "cf2"] }

      args = { CLUSTER_KEY => cluster_key, TABLE_CFS => table_cfs }
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      peer = command(:list_peers).get(0)
      assert_equal(@peer_id, peer.getPeerId)
      assert_equal(cluster_key, peer.getPeerConfig.getClusterKey)
      assert_tablecfs_equal(table_cfs, peer.getPeerConfig.getTableCFsMap)
      assert_equal(false, peer.getPeerConfig.replicateAllUserTables)

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

    define_test "set_peer_tableCFs: works with table-cfs map" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      args = { CLUSTER_KEY => cluster_key}
      command(:add_peer, @peer_id, args)
      command(:set_peer_replicate_all, @peer_id, false)

      assert_equal(1, command(:list_peers).length)
      peer = command(:list_peers).get(0)
      assert_equal(@peer_id, peer.getPeerId)
      assert_equal(cluster_key, peer.getPeerConfig.getClusterKey)

      table_cfs = { "table1" => [], "table2" => ["cf1"], "ns3:table3" => ["cf1", "cf2"] }
      command(:set_peer_tableCFs, @peer_id, table_cfs)
      assert_tablecfs_equal(table_cfs, command(:get_peer_config, @peer_id).getTableCFsMap())

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "append_peer_tableCFs: works with table-cfs map" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      args = { CLUSTER_KEY => cluster_key}
      command(:add_peer, @peer_id, args)
      command(:set_peer_replicate_all, @peer_id, false)

      assert_equal(1, command(:list_peers).length)
      peer = command(:list_peers).get(0)
      assert_equal(@peer_id, peer.getPeerId)
      assert_equal(cluster_key, peer.getPeerConfig.getClusterKey)

      table_cfs = { "table1" => [], "ns2:table2" => ["cf1"] }
      command(:append_peer_tableCFs, @peer_id, table_cfs)
      assert_tablecfs_equal(table_cfs, command(:get_peer_config, @peer_id).getTableCFsMap())

      table_cfs = { "table1" => [], "ns2:table2" => ["cf1"], "ns3:table3" => ["cf1", "cf2"] }
      command(:append_peer_tableCFs, @peer_id, { "ns3:table3" => ["cf1", "cf2"] })
      assert_tablecfs_equal(table_cfs, command(:get_peer_config, @peer_id).getTableCFsMap())

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "remove_peer_tableCFs: works with table-cfs map" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      table_cfs = { "table1" => [], "ns2:table2" => ["cf1"], "ns3:table3" => ["cf1", "cf2"] }
      args = { CLUSTER_KEY => cluster_key, TABLE_CFS => table_cfs }
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      peer = command(:list_peers).get(0)
      assert_equal(@peer_id, peer.getPeerId)
      assert_equal(cluster_key, peer.getPeerConfig.getClusterKey)

      table_cfs = { "table1" => [], "ns2:table2" => ["cf1"] }
      command(:remove_peer_tableCFs, @peer_id, { "ns3:table3" => ["cf1", "cf2"] })
      assert_tablecfs_equal(table_cfs, command(:get_peer_config, @peer_id).getTableCFsMap())

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test 'set_peer_exclude_tableCFs: works with table-cfs map' do
      cluster_key = 'zk4,zk5,zk6:11000:/hbase-test'
      args = { CLUSTER_KEY => cluster_key }
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      peer = command(:list_peers).get(0)
      assert_equal(@peer_id, peer.getPeerId)
      assert_equal(cluster_key, peer.getPeerConfig.getClusterKey)

      table_cfs = { 'table1' => [], 'table2' => ['cf1'],
                    'ns3:table3' => ['cf1', 'cf2'] }
      command(:set_peer_exclude_tableCFs, @peer_id, table_cfs)
      assert_equal(1, command(:list_peers).length)
      peer = command(:list_peers).get(0)
      peer_config = peer.getPeerConfig
      assert_equal(true, peer_config.replicateAllUserTables)
      assert_tablecfs_equal(table_cfs, peer_config.getExcludeTableCFsMap)

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test "set_peer_namespaces: works with namespaces array" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2"]
      namespaces_str = "ns1;ns2"

      args = { CLUSTER_KEY => cluster_key }
      command(:add_peer, @peer_id, args)
      command(:set_peer_replicate_all, @peer_id, false)

      command(:set_peer_namespaces, @peer_id, namespaces)

      assert_equal(1, command(:list_peers).length)
      assert_equal(@peer_id, command(:list_peers).get(0).getPeerId)
      peer_config = command(:list_peers).get(0).getPeerConfig
      assert_equal(namespaces_str,
                   replication_admin.show_peer_namespaces(peer_config))

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "append_peer_namespaces: works with namespaces array" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2"]
      namespaces_str = "ns1;ns2"

      args = { CLUSTER_KEY => cluster_key }
      command(:add_peer, @peer_id, args)
      command(:set_peer_replicate_all, @peer_id, false)

      command(:append_peer_namespaces, @peer_id, namespaces)

      assert_equal(1, command(:list_peers).length)
      assert_equal(@peer_id, command(:list_peers).get(0).getPeerId)
      peer_config = command(:list_peers).get(0).getPeerConfig
      assert_equal(namespaces_str,
                   replication_admin.show_peer_namespaces(peer_config))

      namespaces = ["ns3"]
      namespaces_str = "ns1;ns2;ns3"
      command(:append_peer_namespaces, @peer_id, namespaces)

      assert_equal(1, command(:list_peers).length)
      assert_equal(@peer_id, command(:list_peers).get(0).getPeerId)
      peer_config = command(:list_peers).get(0).getPeerConfig
      assert_equal(namespaces_str,
                   replication_admin.show_peer_namespaces(peer_config))

      # append a namespace which is already in the peer config
      command(:append_peer_namespaces, @peer_id, namespaces)

      assert_equal(1, command(:list_peers).length)
      assert_equal(@peer_id, command(:list_peers).get(0).getPeerId)
      peer_config = command(:list_peers).get(0).getPeerConfig
      assert_equal(namespaces_str,
                   replication_admin.show_peer_namespaces(peer_config))

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test "remove_peer_namespaces: works with namespaces array" do
      cluster_key = "zk4,zk5,zk6:11000:/hbase-test"
      namespaces = ["ns1", "ns2", "ns3"]

      args = { CLUSTER_KEY => cluster_key, NAMESPACES => namespaces }
      command(:add_peer, @peer_id, args)

      namespaces = ["ns1", "ns2"]
      namespaces_str = "ns3"
      command(:remove_peer_namespaces, @peer_id, namespaces)

      assert_equal(1, command(:list_peers).length)
      assert_equal(@peer_id, command(:list_peers).get(0).getPeerId)
      peer_config = command(:list_peers).get(0).getPeerConfig
      assert_equal(namespaces_str,
                   replication_admin.show_peer_namespaces(peer_config))

      namespaces = ["ns3"]
      namespaces_str = nil
      command(:remove_peer_namespaces, @peer_id, namespaces)

      assert_equal(1, command(:list_peers).length)
      assert_equal(@peer_id, command(:list_peers).get(0).getPeerId)
      peer_config = command(:list_peers).get(0).getPeerConfig
      assert_equal(namespaces_str,
                   replication_admin.show_peer_namespaces(peer_config))

      # remove a namespace which is not in peer config
      command(:remove_peer_namespaces, @peer_id, namespaces)

      assert_equal(1, command(:list_peers).length)
      assert_equal(@peer_id, command(:list_peers).get(0).getPeerId)
      peer_config = command(:list_peers).get(0).getPeerConfig
      assert_equal(namespaces_str,
                   replication_admin.show_peer_namespaces(peer_config))

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test 'set_peer_exclude_namespaces: works with namespaces array' do
      cluster_key = 'zk4,zk5,zk6:11000:/hbase-test'
      namespaces = ['ns1', 'ns2']
      namespaces_str = '!ns1;ns2'

      args = { CLUSTER_KEY => cluster_key }
      command(:add_peer, @peer_id, args)
      command(:set_peer_exclude_namespaces, @peer_id, namespaces)

      assert_equal(1, command(:list_peers).length)
      peer_config = command(:list_peers).get(0).getPeerConfig
      assert_equal(true, peer_config.replicateAllUserTables)
      assert_equal(namespaces_str,
                   replication_admin.show_peer_exclude_namespaces(peer_config))

      # cleanup for future tests
      command(:remove_peer, @peer_id)
    end

    define_test 'set_peer_replicate_all' do
      cluster_key = 'zk4,zk5,zk6:11000:/hbase-test'

      args = { CLUSTER_KEY => cluster_key }
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      peer_config = command(:list_peers).get(0).getPeerConfig
      assert_equal(true, peer_config.replicateAllUserTables)

      command(:set_peer_replicate_all, @peer_id, false)
      peer_config = command(:list_peers).get(0).getPeerConfig
      assert_equal(false, peer_config.replicateAllUserTables)

      command(:set_peer_replicate_all, @peer_id, true)
      peer_config = command(:list_peers).get(0).getPeerConfig
      assert_equal(true, peer_config.replicateAllUserTables)

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
    end

    define_test 'set_peer_serial' do
      cluster_key = 'zk4,zk5,zk6:11000:/hbase-test'

      args = { CLUSTER_KEY => cluster_key }
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      peer_config = command(:list_peers).get(0).getPeerConfig
      assert_equal(false, peer_config.isSerial)

      command(:set_peer_serial, @peer_id, true)
      peer_config = command(:list_peers).get(0).getPeerConfig
      assert_equal(true, peer_config.isSerial)

      command(:set_peer_serial, @peer_id, false)
      peer_config = command(:list_peers).get(0).getPeerConfig
      assert_equal(false, peer_config.isSerial)

      # cleanup for future tests
      replication_admin.remove_peer(@peer_id)
      assert_equal(0, command(:list_peers).length)
    end

    define_test "set_peer_bandwidth: works with peer bandwidth upper limit" do
      cluster_key = "localhost:2181:/hbase-test"
      args = { CLUSTER_KEY => cluster_key }
      command(:add_peer, @peer_id, args)

      peer_config = command(:get_peer_config, @peer_id)
      assert_equal(0, peer_config.get_bandwidth)
      command(:set_peer_bandwidth, @peer_id, 2097152)
      peer_config = command(:get_peer_config, @peer_id)
      assert_equal(2097152, peer_config.get_bandwidth)

      #cleanup
      command(:remove_peer, @peer_id)
    end

    define_test "transit_peer_sync_replication_state: test" do
      cluster_key = "server1.cie.com:2181:/hbase"
      remote_wal_dir = "hdfs://srv1:9999/hbase"
      table_cfs = { "ns3:table1" => [], "ns3:table2" => [],
        "ns3:table3" => [] }
      args = { CLUSTER_KEY => cluster_key, REMOTE_WAL_DIR => remote_wal_dir,
        TABLE_CFS => table_cfs}
      command(:add_peer, @peer_id, args)

      assert_equal(1, command(:list_peers).length)
      peer = command(:list_peers).get(0)
      assert_equal(@peer_id, peer.getPeerId)
      assert_equal(SyncReplicationState::DOWNGRADE_ACTIVE, peer.getSyncReplicationState)

      command(:transit_peer_sync_replication_state, @peer_id, 'STANDBY')
      assert_equal(1, command(:list_peers).length)
      peer = command(:list_peers).get(0)
      assert_equal(@peer_id, peer.getPeerId)
      assert_equal(SyncReplicationState::STANDBY, peer.getSyncReplicationState)

      # need to transit back otherwise we can not remove the peer
      command(:transit_peer_sync_replication_state, @peer_id, 'DOWNGRADE_ACTIVE')
      assert_equal(1, command(:list_peers).length)
      peer = command(:list_peers).get(0)
      assert_equal(@peer_id, peer.getPeerId)
      assert_equal(SyncReplicationState::DOWNGRADE_ACTIVE, peer.getSyncReplicationState)

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
