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
    class SetPeerBandwidth< Command
      def help
        return <<-EOF
Set the replication source per node bandwidth for the specified peer.
Examples:

  # set bandwidth=2MB per regionserver for a peer
  hbase> set_peer_bandwidth '1', 2097152
  # unset bandwidth for a peer to use the default bandwidth configured in server-side
  hbase> set_peer_bandwidth '1'

EOF
      end

      def command(id, bandwidth = 0)
        replication_admin.set_peer_bandwidth(id, bandwidth)
      end
    end
  end
end
