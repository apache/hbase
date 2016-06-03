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

module Shell
    module Commands
        class UpdatePeerConfig< Command
            def help
                return <<-EOF
A peer can either be another HBase cluster or a custom replication endpoint. In either case an id
must be specified to identify the peer. This command does not interrupt processing on an enabled replication peer.

Two optional arguments are DATA and CONFIG which can be specified to set different values for either
the peer_data or configuration for a custom replication endpoint. Any existing values not updated by this command
are left unchanged.

CLUSTER_KEY, REPLICATION_ENDPOINT, and TABLE_CFs cannot be updated with this command.
To update TABLE_CFs, see the append_peer_tableCFs and remove_peer_tableCFs commands.

  hbase> update_peer_config '1', DATA => { "key1" => 1 }
  hbase> update_peer_config '2', CONFIG => { "config1" => "value1", "config2" => "value2" }
  hbase> update_peer_config '3', DATA => { "key1" => 1 }, CONFIG => { "config1" => "value1", "config2" => "value2" },

        EOF
      end

      def command(id, args = {})
        replication_admin.update_peer_config(id, args)
      end
    end
  end
end