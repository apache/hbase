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
    class ListSnapshotSizes < Command
      def help
        return <<-EOF
Lists the size of every HBase snapshot given the space quota size computation
algorithms. An HBase snapshot only "owns" the size of a file when the table
from which the snapshot was created no longer refers to that file.
EOF
      end

      def command(args = {})
        formatter.header(["SNAPSHOT", "SIZE"])
        count = 0
        quotas_admin.list_snapshot_sizes.each do |snapshot,size|
          formatter.row([snapshot.to_s, size.to_s])
          count += 1
        end
        formatter.footer(count)
      end
    end
  end
end
