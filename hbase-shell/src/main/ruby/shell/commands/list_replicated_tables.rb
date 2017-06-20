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
    class ListReplicatedTables < Command
      def help
        <<-EOF
List all the tables and column families replicated from this cluster

  hbase> list_replicated_tables
  hbase> list_replicated_tables 'abc.*'
EOF
      end

      def command(regex = '.*')
        formatter.header(['TABLE:COLUMNFAMILY', 'ReplicationType'], [32])
        list = replication_admin.list_replicated_tables(regex)
        list.each do |e|
          map = e.getColumnFamilyMap
          map.each do |cf|
            if cf[1] == org.apache.hadoop.hbase.HConstants::REPLICATION_SCOPE_LOCAL
              replicateType = 'LOCAL'
            elsif cf[1] == org.apache.hadoop.hbase.HConstants::REPLICATION_SCOPE_GLOBAL
              replicateType = 'GLOBAL'
            elsif cf[1] == org.apache.hadoop.hbase.HConstants::REPLICATION_SCOPE_SERIAL
              replicateType = 'SERIAL'
            else
              replicateType = 'UNKNOWN'
            end
            formatter.row([e.getTable.getNameAsString + ':' + cf[0], replicateType], true, [32])
          end
        end
        formatter.footer
      end
    end
  end
end
