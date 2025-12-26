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
    class SetPeerSleepForRetries < Command
      def help
        <<-EOF
Set the replication source sleep time between retries for the specified peer.
Examples:

  # set sleep time to 2 seconds (2000ms) between retries for a peer
  hbase> set_peer_sleep_for_retries '1', 2000
  # unset sleep time for a peer to use the global default configured in server-side
  hbase> set_peer_sleep_for_retries '1', 0

EOF
      end

      def command(id, sleep_for_retries = 0)
        replication_admin.set_peer_sleep_for_retries(id, sleep_for_retries)
      end
    end
  end
end
