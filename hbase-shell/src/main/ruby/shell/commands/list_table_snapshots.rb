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

require 'time'

module Shell
  module Commands
    class ListTableSnapshots < Command
      def help
        return <<-EOF
List all completed snapshots matching the table name regular expression and the
snapshot name regular expression (by printing the names and relative information).
Optional snapshot name regular expression parameter could be used to filter the output
by snapshot name.

Examples:
  hbase> list_table_snapshots 'tableName'
  hbase> list_table_snapshots 'tableName.*'
  hbase> list_table_snapshots 'tableName', 'snapshotName'
  hbase> list_table_snapshots 'tableName', 'snapshotName.*'
  hbase> list_table_snapshots 'tableName.*', 'snapshotName.*'
  hbase> list_table_snapshots 'ns:tableName.*', 'snapshotName.*'
EOF
      end

      def command(tableNameRegex, snapshotNameRegex = ".*")
        now = Time.now
        formatter.header([ "SNAPSHOT", "TABLE + CREATION TIME"])

        list = admin.list_table_snapshots(tableNameRegex, snapshotNameRegex)
        list.each do |snapshot|
          creation_time = Time.at(snapshot.getCreationTime() / 1000).to_s
          formatter.row([ snapshot.getName, snapshot.getTable + " (" + creation_time + ")" ])
        end

        formatter.footer(now, list.size)
        return list.map { |s| s.getName() }
      end
    end
  end
end
