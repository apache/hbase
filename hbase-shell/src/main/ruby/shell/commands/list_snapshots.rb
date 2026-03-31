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

java_import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils

module Shell
  module Commands
    class ListSnapshots < Command
      def help
        <<-EOF
List all snapshots taken (by printing the names and relative information).
Optional regular expression parameter could be used to filter the output
by snapshot name.

Examples:
  hbase> list_snapshots
  hbase> list_snapshots 'abc.*'
EOF
      end

      def command(regex = '.*')
        formatter.header(['SNAPSHOT', 'TABLE + CREATION TIME + TTL(Sec)'])

        list = admin.list_snapshot(regex)
        list.each do |snapshot|
          creation_time = Time.at(snapshot.getCreationTime / 1000).to_s
          ttl = snapshot.getTtl
          if ttl == 0
            ttl_info = 'FOREVER'
          else
            now_timestamp = (Time.now.to_f * 1000).to_i
            expired = SnapshotDescriptionUtils.isExpiredSnapshot(ttl, snapshot.getCreationTime(), now_timestamp)
            if expired
              ttl_info = ttl.to_s + ' (Expired) '
            else
              ttl_info = ttl.to_s
            end
          end
          info = snapshot.getTableNameAsString + ' (' + creation_time + ') ' + ttl_info
          formatter.row([snapshot.getName, info])
        end

        formatter.footer(list.size)
        list.map(&:getName)
      end
    end
  end
end
