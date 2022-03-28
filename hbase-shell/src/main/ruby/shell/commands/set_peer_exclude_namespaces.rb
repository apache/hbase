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
    class SetPeerExcludeNamespaces < Command
      def help
        <<~EOF
  Set the namespaces which not replicated for the specified peer.

  Note:
  1. The replicate_all flag need to be true when set exclude namespaces.
  2. Set a exclude namespace in the peer config means that all tables in this
     namespace will not be replicated to the peer cluster. If peer config
     already has a exclude table, then not allow set this table's namespace
     as a exclude namespace.

  Examples:

    # set exclude namespaces config to null
    hbase> set_peer_exclude_namespaces '1', []
    # set namespaces which not replicated for a peer.
    # set a exclude namespace in the peer config means that all tables in this
    # namespace will not be replicated.
    hbase> set_peer_exclude_namespaces '2', ["ns1", "ns2"]

  EOF
      end

      def command(id, exclude_namespaces)
        replication_admin.set_peer_exclude_namespaces(id, exclude_namespaces)
      end
    end
  end
end
