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
    class SetPeerSerial < Command
      def help
        <<~EOF
  Set the serial flag to true or false for the specified peer.

  If serial flag is true, then all logs of user tables (REPLICATION_SCOPE != 0) will be
  replicated to peer cluster serially, which means that each segment of log for replicated
  table will be pushed to peer cluster in order of their log sequence id.

  If serial flag is false, then the source cluster won't ensure that the logs of replicated
  table will be pushed to peer cluster serially.

  Examples:

    # set serial flag to true
    hbase> set_peer_serial '1', true
    # set serial flag to false
    hbase> set_peer_serial '1', false
  EOF
      end

      def command(id, serial)
        replication_admin.set_peer_serial(id, serial)
      end
    end
  end
end
