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
    class ListQuotaTableSizes < Command
      def help
        return <<-EOF
Lists the sizes of the tables in HBase as collected
for the purpose of implementing filesystem utilization
quotas. This information is extracted from the HBase
Master and drives future quota actions in the cluster.

For example:

    hbase> list_quota_table_sizes
EOF
      end

      def command(args = {})
        formatter.header(["TABLE", "SIZE"])
        count = 0
        quotas_admin.get_master_table_sizes().each do |tableName,size|
          formatter.row([tableName.to_s, size.to_s])
          count += 1
        end
        formatter.footer(count)
      end
    end
  end
end
