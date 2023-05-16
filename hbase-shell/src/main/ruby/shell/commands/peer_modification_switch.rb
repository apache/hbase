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

# frozen_string_literal: true

module Shell
  module Commands
    # Enable or disable peer modification operations
    class PeerModificationSwitch < Command
      def help
        <<~EOF
Enable/Disable peer modification. Returns previous state.
Examples:

  hbase> peer_modification_switch true
  hbase> peer_modification_switch false, true

The second boolean parameter means whether you want to wait until all remaining peer modification
finished, before the command returns.
EOF
      end

      def command(enable_or_disable, drain_procs = false)
        prev_state = !!replication_admin.peer_modification_switch(enable_or_disable, drain_procs)
        formatter.row(["Previous peer modification state : #{prev_state}"])
        prev_state
      end
    end
  end
end
