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
    class ListQuotaViolations < Command
      def help
        return <<-EOF
Lists the current quota violations being enforced by a RegionServer.
Violations are enacted based on the quota snapshot information a RegionServer
holds (see list_quota_snapshots). Each violation contains the action the
RegionServer is taking on the table. This command is useful in debugging
the running state of a cluster using filesystem quotas.

For example:

    hbase> list_quota_violations 'regionserver1.domain,16020,1483482894742'
EOF
      end

      def command(hostname, args = {})
        formatter.header(["TABLE", "POLICY"])
        count = 0
        quotas_admin.get_rs_quota_violations(hostname).each do |tableName,policy|
          formatter.row([tableName.to_s, policy.name])
          count += 1
        end
        formatter.footer(count)
      end
    end
  end
end
