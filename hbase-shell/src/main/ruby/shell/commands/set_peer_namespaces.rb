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
    class SetPeerNamespaces< Command
      def help
        return <<-EOF
  Set the replicable namespaces config for the specified peer.

  Set a namespace in the peer config means that all tables in this
  namespace will be replicated to the peer cluster. So if you already
  have set a namespace in the peer config, then you can't set this
  namespace's tables in the peer config again.

  Examples:

    # set namespaces config is null, then the table-cfs config decide
    # which table to be replicated.
    hbase> set_peer_namespaces '1', []
    # set namespaces to be replicable for a peer.
    # set a namespace in the peer config means that all tables in this
    # namespace (with replication_scope != 0 ) will be replicated.
    hbase> set_peer_namespaces '2', ["ns1", "ns2"]

  EOF
      end

      def command(id, namespaces)
        replication_admin.set_peer_namespaces(id, namespaces)
      end
    end
  end
end
