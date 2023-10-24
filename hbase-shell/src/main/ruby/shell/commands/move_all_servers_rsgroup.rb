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
    class MoveAllServersRsgroup < Command
      def help
        <<-EOF
Reassign RegionServers from one group to another. Every region of the
RegionServer will be moved to another RegionServer.

Example:

  hbase> move_all_servers_rsgroup 'dest','src'

EOF
      end

      def command(dest, src)
        rsgroup_admin.move_all_servers(dest, src)
      end
    end
  end
end
