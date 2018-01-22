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
    class TransitPeerSyncReplicationState < Command
      def help
        <<-EOF
Transit current cluster to new state in the specified synchronous replication peer.
Examples:

  # Transit cluster state to DOWNGRADE_ACTIVE in a synchronous replication peer
  hbase> transit_peer_sync_replication_state '1', 'DOWNGRADE_ACTIVE'
  # Transit cluster state to ACTIVE in a synchronous replication peer
  hbase> transit_peer_sync_replication_state '1', 'ACTIVE'
  # Transit cluster state to STANDBY in a synchronous replication peer
  hbase> transit_peer_sync_replication_state '1', 'STANDBY'

EOF
      end

      def command(id, state)
        replication_admin.transit_peer_sync_replication_state(id, state)
      end
    end
  end
end
