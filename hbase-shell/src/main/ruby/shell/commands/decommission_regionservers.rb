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
    # Decommission a list of region servers, optionally offload corresponding regions
    class DecommissionRegionservers < Command
      def help
        <<~EOF
  Mark region server(s) as decommissioned to prevent additional regions from
  getting assigned to them.

  Optionally, offload the regions on the servers by passing true.
  NOTE: Region offloading is asynchronous.

  If there are multiple servers to be decommissioned, decommissioning them
  at the same time can prevent wasteful region movements.

  Examples:
    hbase> decommission_regionservers 'server'
    hbase> decommission_regionservers 'server,port'
    hbase> decommission_regionservers 'server,port,starttime'
    hbase> decommission_regionservers 'server', false
    hbase> decommission_regionservers ['server1','server2'], true
EOF
      end

      def command(server_names, should_offload = false)
        admin.decommission_regionservers(server_names, should_offload)
      end
    end
  end
end
