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
    class DeleteTableSnapshots < Command
      def help
        return <<-EOF
Delete all of the snapshots matching the given table name regular expression
and snapshot name regular expression.
By default snapshot name regular expression will delete all the snapshots of the
matching table name regular expression.

Examples:
  hbase> delete_table_snapshots 'tableName'
  hbase> delete_table_snapshots 'tableName.*'
  hbase> delete_table_snapshots 'tableName' 'snapshotName'
  hbase> delete_table_snapshots 'tableName' 'snapshotName.*'
  hbase> delete_table_snapshots 'tableName.*' 'snapshotName.*'
  hbase> delete_table_snapshots 'ns:tableName.*' 'snapshotName.*'

EOF
      end

      def command(tableNameregex, snapshotNameRegex = ".*")
        formatter.header([ "SNAPSHOT", "TABLE + CREATION TIME"])
        list = admin.list_table_snapshots(tableNameregex, snapshotNameRegex)
        count = list.size
        list.each do |snapshot|
          creation_time = Time.at(snapshot.getCreationTime() / 1000).to_s
          formatter.row([ snapshot.getName, snapshot.getTable + " (" + creation_time + ")" ])
        end
        puts "\nDelete the above #{count} snapshots (y/n)?" unless count == 0
        answer = 'n'
        answer = gets.chomp unless count == 0
        puts "No snapshots matched the table name regular expression #{tableNameregex.to_s} and the snapshot name regular expression #{snapshotNameRegex.to_s}" if count == 0
        return unless answer =~ /y.*/i

        format_simple_command do
          list.each do |deleteSnapshot|
            begin
              admin.delete_snapshot(deleteSnapshot.getName)
              puts "Successfully deleted snapshot: #{deleteSnapshot.getName}"
              puts "\n"
            rescue RuntimeError
              puts "Failed to delete snapshot: #{deleteSnapshot.getName}, due to below exception,\n" + $!
              puts "\n"
            end
          end
        end
      end
    end
  end
end
