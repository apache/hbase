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
    class RemovePeerExcludeNamespaces < Command
      def help
        <<-EOF
Remove the namespaces which not replicated for the specified peer.

Note:
  1. The replicate_all flag need to be true when remove exclude namespaces.
  2. Remove a exclude namespace in the peer config means that all tables in this
     namespace will be replicated to the peer cluster.

Examples:

    # remove ns1 from the not replicable namespaces for peer '2'.
    hbase> remove_peer_exclude_namespaces '2', ["ns1", "ns2"]

        EOF
      end

      def command(id, namespaces)
        replication_admin.remove_peer_exclude_namespaces(id, namespaces)
      end
    end
  end
end
