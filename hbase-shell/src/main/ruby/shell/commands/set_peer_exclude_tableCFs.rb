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
    class SetPeerExcludeTableCFs < Command
      def help
        <<~EOF
  Set the table-cfs which not replicated for the specified peer.

  Note:
  1. The replicate_all flag need to be true when set exclude table-cfs.
  2. If peer config already has a exclude namespace, then not allow set any
     exclude table of this namespace to the peer config.

  Examples:

    # set exclude table-cfs to null
    hbase> set_peer_exclude_tableCFs '1'
    # set table / table-cf which not replicated for a peer, for a table without
    # an explicit column-family list, all column-families will not be replicated
    hbase> set_peer_exclude_tableCFs '2', { "ns1:table1" => [],
                                    "ns2:table2" => ["cf1", "cf2"],
                                    "ns3:table3" => ["cfA", "cfB"]}

  EOF
      end

      def command(id, exclude_peer_table_cfs = nil)
        replication_admin.set_peer_exclude_tableCFs(id, exclude_peer_table_cfs)
      end

      def command_name
        'set_peer_exclude_tableCFs'
      end
    end
  end
end
