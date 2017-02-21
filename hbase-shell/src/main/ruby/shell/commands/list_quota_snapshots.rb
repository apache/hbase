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
    class ListQuotaSnapshots < Command
      def help
        return <<-EOF
Lists the current snapshot of quotas on the given RegionServer. This
information filters to each RegionServer from the Master. For each
table, a snapshot includes the filesystem use, the filesystem limit,
and the policy to enact when the limit is exceeded. This command is
useful for debugging the running state of a cluster using filesystem quotas.

For example:

    hbase> list_quota_snapshots 'regionserver1.domain,16020,1483482894742'
EOF
      end

      def command(hostname, args = {})
        formatter.header(["TABLE", "USAGE", "LIMIT", "IN VIOLATION", "POLICY"])
        count = 0
        quotas_admin.get_rs_quota_snapshots(hostname).each do |tableName,snapshot|
          status = snapshot.getQuotaStatus()
          policy = get_policy(status)
          formatter.row([tableName.to_s, snapshot.getUsage().to_s, snapshot.getLimit().to_s, status.isInViolation().to_s, policy])
          count += 1
        end
        formatter.footer(count)
      end

      def get_policy(status)
        # Unwrap the violation policy if it exists
        if status.isInViolation()
          status.getPolicy().name()
        else
          "None"
        end
      end
    end
  end
end
