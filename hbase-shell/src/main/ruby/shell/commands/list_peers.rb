#
# Copyright The Apache Software Foundation
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

module Shell
  module Commands
    class ListPeers < Command
      def help
        <<-EOF
  List all replication peer clusters.

  If replicate_all flag is false, the namespaces and table-cfs in peer config
  will be replicated to peer cluster.

  If replicate_all flag is true, all user tables will be replicate to peer
  cluster, except that the namespaces and table-cfs in peer config.

  hbase> list_peers
EOF
      end

      def command
        peers = replication_admin.list_peers

        formatter.header(%w[PEER_ID CLUSTER_KEY ENDPOINT_CLASSNAME
                            REMOTE_ROOT_DIR SYNC_REPLICATION_STATE STATE
                            REPLICATE_ALL NAMESPACES TABLE_CFS BANDWIDTH
                            SERIAL])

        peers.each do |peer|
          id = peer.getPeerId
          state = peer.isEnabled ? 'ENABLED' : 'DISABLED'
          config = peer.getPeerConfig
          if config.replicateAllUserTables
            namespaces = replication_admin.show_peer_exclude_namespaces(config)
            tableCFs = replication_admin.show_peer_exclude_tableCFs(config)
          else
            namespaces = replication_admin.show_peer_namespaces(config)
            tableCFs = replication_admin.show_peer_tableCFs_by_config(config)
          end
          cluster_key = 'nil'
          unless config.getClusterKey.nil?
            cluster_key = config.getClusterKey
          end
          endpoint_classname = 'nil'
          unless config.getReplicationEndpointImpl.nil?
            endpoint_classname = config.getReplicationEndpointImpl
          end
          remote_root_dir = 'nil'
          unless config.getRemoteWALDir.nil?
            remote_root_dir = config.getRemoteWALDir
          end
          formatter.row([id, cluster_key, endpoint_classname,
                         remote_root_dir, peer.getSyncReplicationState, state,
                         config.replicateAllUserTables, namespaces, tableCFs,
                         config.getBandwidth, config.isSerial])
        end

        formatter.footer
        peers
      end
    end
  end
end
