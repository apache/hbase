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

include Java

java_import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil
java_import org.apache.hadoop.hbase.replication.SyncReplicationState
java_import org.apache.hadoop.hbase.replication.ReplicationPeerConfig
java_import org.apache.hadoop.hbase.util.Bytes
java_import org.apache.hadoop.hbase.zookeeper.ZKConfig
java_import org.apache.hadoop.hbase.TableName

# Used for replication administrative operations.

module Hbase
  class RepAdmin
    include HBaseConstants

    def initialize(configuration)
      @configuration = configuration
      @admin = ConnectionFactory.createConnection(configuration).getAdmin
    end

    #----------------------------------------------------------------------------------------------
    # Add a new peer cluster to replicate to
    def add_peer(id, args = {}, peer_tableCFs = nil)
      if args.is_a?(Hash)
        unless peer_tableCFs.nil?
          raise(ArgumentError, 'peer_tableCFs should be specified as TABLE_CFS in args')
        end

        endpoint_classname = args.fetch(ENDPOINT_CLASSNAME, nil)
        cluster_key = args.fetch(CLUSTER_KEY, nil)

        # Handle cases where custom replication endpoint and cluster key are either both provided
        # or neither are provided
        if endpoint_classname.nil? && cluster_key.nil?
          raise(ArgumentError, 'Either ENDPOINT_CLASSNAME or CLUSTER_KEY must be specified.')
        end

        # Cluster Key is required for ReplicationPeerConfig for a custom replication endpoint
        if !endpoint_classname.nil? && cluster_key.nil?
          cluster_key = ZKConfig.getZooKeeperClusterKey(@configuration)
        end

        # Optional parameters
        config = args.fetch(CONFIG, nil)
        data = args.fetch(DATA, nil)
        table_cfs = args.fetch(TABLE_CFS, nil)
        namespaces = args.fetch(NAMESPACES, nil)
        peer_state = args.fetch(STATE, nil)
        remote_wal_dir = args.fetch(REMOTE_WAL_DIR, nil)

        # Create and populate a ReplicationPeerConfig
        builder = ReplicationPeerConfig.newBuilder()
        builder.set_cluster_key(cluster_key)

        unless endpoint_classname.nil?
          builder.set_replication_endpoint_impl(endpoint_classname)
        end

        unless remote_wal_dir.nil?
          builder.setRemoteWALDir(remote_wal_dir)
        end

        unless config.nil?
          builder.putAllConfiguration(config)
        end

        unless data.nil?
          # Convert Strings to Bytes for peer_data
          data.each do |key, val|
            builder.putPeerData(Bytes.to_bytes(key), Bytes.to_bytes(val))
          end
        end

        unless namespaces.nil?
          ns_set = java.util.HashSet.new
          namespaces.each do |n|
            ns_set.add(n)
          end
          builder.setReplicateAllUserTables(false)
          builder.set_namespaces(ns_set)
        end

        unless table_cfs.nil?
          # convert table_cfs to TableName
          map = java.util.HashMap.new
          table_cfs.each do |key, val|
            map.put(org.apache.hadoop.hbase.TableName.valueOf(key), val)
          end
          builder.setReplicateAllUserTables(false)
          builder.set_table_cfs_map(map)
        end

        enabled = true
        unless peer_state.nil?
          enabled = false if peer_state == 'DISABLED'
        end
        @admin.addReplicationPeer(id, builder.build, enabled)
      else
        raise(ArgumentError, 'args must be a Hash')
      end
    end

    #----------------------------------------------------------------------------------------------
    # Remove a peer cluster, stops the replication
    def remove_peer(id)
      @admin.removeReplicationPeer(id)
    end

    #---------------------------------------------------------------------------------------------
    # Show replcated tables/column families, and their ReplicationType
    def list_replicated_tables(regex = '.*')
      pattern = java.util.regex.Pattern.compile(regex)
      list = @admin.listReplicatedTableCFs
      list.select { |t| pattern.match(t.getTable.getNameAsString) }
    end

    #----------------------------------------------------------------------------------------------
    # List all peer clusters
    def list_peers
      @admin.listReplicationPeers
    end

    #----------------------------------------------------------------------------------------------
    # Restart the replication stream to the specified peer
    def enable_peer(id)
      @admin.enableReplicationPeer(id)
    end

    #----------------------------------------------------------------------------------------------
    # Stop the replication stream to the specified peer
    def disable_peer(id)
      @admin.disableReplicationPeer(id)
    end

    #----------------------------------------------------------------------------------------------
    # Show the current tableCFs config for the specified peer
    def show_peer_tableCFs(id)
      rpc = @admin.getReplicationPeerConfig(id)
      show_peer_tableCFs_by_config(rpc)
    end

    def show_peer_tableCFs_by_config(peer_config)
      ReplicationPeerConfigUtil.convertToString(peer_config.getTableCFsMap)
    end

    #----------------------------------------------------------------------------------------------
    # Set new tableCFs config for the specified peer
    def set_peer_tableCFs(id, tableCFs)
      unless tableCFs.nil?
        # convert tableCFs to TableName
        map = java.util.HashMap.new
        tableCFs.each do |key, val|
          map.put(org.apache.hadoop.hbase.TableName.valueOf(key), val)
        end
        rpc = get_peer_config(id)
        unless rpc.nil?
          rpc.setTableCFsMap(map)
          @admin.updateReplicationPeerConfig(id, rpc)
        end
      end
    end

    #----------------------------------------------------------------------------------------------
    # Append a tableCFs config for the specified peer
    def append_peer_tableCFs(id, tableCFs)
      unless tableCFs.nil?
        # convert tableCFs to TableName
        map = java.util.HashMap.new
        tableCFs.each do |key, val|
          map.put(org.apache.hadoop.hbase.TableName.valueOf(key), val)
        end
      end
      @admin.appendReplicationPeerTableCFs(id, map)
    end

    #----------------------------------------------------------------------------------------------
    # Remove some tableCFs from the tableCFs config of the specified peer
    def remove_peer_tableCFs(id, tableCFs)
      unless tableCFs.nil?
        # convert tableCFs to TableName
        map = java.util.HashMap.new
        tableCFs.each do |key, val|
          map.put(org.apache.hadoop.hbase.TableName.valueOf(key), val)
        end
      end
      @admin.removeReplicationPeerTableCFs(id, map)
    end

    # Set new namespaces config for the specified peer
    def set_peer_namespaces(id, namespaces)
      unless namespaces.nil?
        ns_set = java.util.HashSet.new
        namespaces.each do |n|
          ns_set.add(n)
        end
        rpc = get_peer_config(id)
        unless rpc.nil?
          rpc.setNamespaces(ns_set)
          @admin.updateReplicationPeerConfig(id, rpc)
        end
      end
    end

    # Add some namespaces for the specified peer
    def add_peer_namespaces(id, namespaces)
      unless namespaces.nil?
        rpc = get_peer_config(id)
        unless rpc.nil?
          if rpc.getNamespaces.nil?
            ns_set = java.util.HashSet.new
          else
            ns_set = java.util.HashSet.new(rpc.getNamespaces)
          end
          namespaces.each do |n|
            ns_set.add(n)
          end
          builder = ReplicationPeerConfig.newBuilder(rpc)
          builder.setNamespaces(ns_set)
          @admin.updateReplicationPeerConfig(id, builder.build)
        end
      end
    end

    # Remove some namespaces for the specified peer
    def remove_peer_namespaces(id, namespaces)
      unless namespaces.nil?
        rpc = get_peer_config(id)
        unless rpc.nil?
          ns_set = rpc.getNamespaces
          unless ns_set.nil?
            ns_set = java.util.HashSet.new(ns_set)
            namespaces.each do |n|
              ns_set.remove(n)
            end
          end
          builder = ReplicationPeerConfig.newBuilder(rpc)
          builder.setNamespaces(ns_set)
          @admin.updateReplicationPeerConfig(id, builder.build)
        end
      end
    end

    # Show the current namespaces config for the specified peer
    def show_peer_namespaces(peer_config)
      namespaces = peer_config.get_namespaces
      if !namespaces.nil?
        namespaces = java.util.ArrayList.new(namespaces)
        java.util.Collections.sort(namespaces)
        return namespaces.join(';')
      else
        return nil
      end
    end

    # Set new bandwidth config for the specified peer
    def set_peer_bandwidth(id, bandwidth)
      rpc = get_peer_config(id)
      unless rpc.nil?
        rpc.setBandwidth(bandwidth)
        @admin.updateReplicationPeerConfig(id, rpc)
      end
    end

    def set_peer_replicate_all(id, replicate_all)
      rpc = get_peer_config(id)
      return if rpc.nil?
      rpc.setReplicateAllUserTables(replicate_all)
      @admin.updateReplicationPeerConfig(id, rpc)
    end

    def set_peer_serial(id, peer_serial)
      rpc = get_peer_config(id)
      return if rpc.nil?
      rpc_builder = org.apache.hadoop.hbase.replication.ReplicationPeerConfig
                       .newBuilder(rpc)
      new_rpc = rpc_builder.setSerial(peer_serial).build
      @admin.updateReplicationPeerConfig(id, new_rpc)
    end

    # Set exclude namespaces config for the specified peer
    def set_peer_exclude_namespaces(id, exclude_namespaces)
      return if exclude_namespaces.nil?
      exclude_ns_set = java.util.HashSet.new
      exclude_namespaces.each do |n|
        exclude_ns_set.add(n)
      end
      rpc = get_peer_config(id)
      return if rpc.nil?
      rpc.setExcludeNamespaces(exclude_ns_set)
      @admin.updateReplicationPeerConfig(id, rpc)
    end

    # Show the exclude namespaces config for the specified peer
    def show_peer_exclude_namespaces(peer_config)
      namespaces = peer_config.getExcludeNamespaces
      return nil if namespaces.nil?
      namespaces = java.util.ArrayList.new(namespaces)
      java.util.Collections.sort(namespaces)
      '!' + namespaces.join(';')
    end

    # Set exclude tableCFs config for the specified peer
    def set_peer_exclude_tableCFs(id, exclude_tableCFs)
      return if exclude_tableCFs.nil?
      # convert tableCFs to TableName
      map = java.util.HashMap.new
      exclude_tableCFs.each do |key, val|
        map.put(org.apache.hadoop.hbase.TableName.valueOf(key), val)
      end
      rpc = get_peer_config(id)
      return if rpc.nil?
      rpc.setExcludeTableCFsMap(map)
      @admin.updateReplicationPeerConfig(id, rpc)
    end

    # Show the exclude tableCFs config for the specified peer
    def show_peer_exclude_tableCFs(peer_config)
      tableCFs = peer_config.getExcludeTableCFsMap
      return nil if tableCFs.nil?
      '!' + ReplicationPeerConfigUtil.convertToString(tableCFs)
    end

    # Transit current cluster to a new state in the specified synchronous
    # replication peer
    def transit_peer_sync_replication_state(id, state)
      if 'ACTIVE'.eql?(state)
        @admin.transitReplicationPeerSyncReplicationState(id, SyncReplicationState::ACTIVE)
      elsif 'DOWNGRADE_ACTIVE'.eql?(state)
        @admin.transitReplicationPeerSyncReplicationState(id, SyncReplicationState::DOWNGRADE_ACTIVE)
      elsif 'STANDBY'.eql?(state)
        @admin.transitReplicationPeerSyncReplicationState(id, SyncReplicationState::STANDBY)
      else
        raise(ArgumentError, 'synchronous replication state must be ACTIVE, DOWNGRADE_ACTIVE or STANDBY')
      end
    end

    #----------------------------------------------------------------------------------------------
    # Enables a table's replication switch
    def enable_tablerep(table_name)
      tableName = TableName.valueOf(table_name)
      @admin.enableTableReplication(tableName)
    end

    #----------------------------------------------------------------------------------------------
    # Disables a table's replication switch
    def disable_tablerep(table_name)
      tableName = TableName.valueOf(table_name)
      @admin.disableTableReplication(tableName)
    end

    def list_peer_configs
      map = java.util.HashMap.new
      peers = @admin.listReplicationPeers
      peers.each do |peer|
        map.put(peer.getPeerId, peer.getPeerConfig)
      end
      map
    end

    def get_peer_config(id)
      @admin.getReplicationPeerConfig(id)
    end

    def update_peer_config(id, args = {})
      # Optional parameters
      config = args.fetch(CONFIG, nil)
      data = args.fetch(DATA, nil)

      # Create and populate a ReplicationPeerConfig
      replication_peer_config = get_peer_config(id)
      builder = org.apache.hadoop.hbase.replication.ReplicationPeerConfig
                   .newBuilder(replication_peer_config)
      unless config.nil?
        builder.putAllConfiguration(config)
      end

      unless data.nil?
        # Convert Strings to Bytes for peer_data
        data.each do |key, val|
          builder.putPeerData(Bytes.to_bytes(key), Bytes.to_bytes(val))
        end
      end

      @admin.updateReplicationPeerConfig(id, builder.build)
    end
  end
end
