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
        <<-EOF
Lists the current space quota snapshots with optional selection criteria.
Snapshots encapsulate relevant information to space quotas such as space
use, configured limits, and quota violation details. This command is
useful for understanding the current state of a cluster with space quotas.

By default, this command will read all snapshots stored in the system from
the hbase:quota table. A table name or namespace can be provided to filter
the snapshots returned. RegionServers maintain a copy of snapshots, refreshing
at a regular interval; by providing a RegionServer option, snapshots will
be retreived from that RegionServer instead of the quota table.

For example:

    hbase> list_quota_snapshots
    hbase> list_quota_snapshots({TABLE => 'table1'})
    hbase> list_quota_snapshots({NAMESPACE => 'org1'})
    hbase> list_quota_snapshots({REGIONSERVER => 'server1.domain,16020,1483482894742'})
    hbase> list_quota_snapshots({NAMESPACE => 'org1', REGIONSERVER => 'server1.domain,16020,1483482894742'})
EOF
      end

      def command(args = {})
        # All arguments may be nil
        desired_table = args[::HBaseConstants::TABLE]
        desired_namespace = args[::HBaseConstants::NAMESPACE]
        desired_regionserver = args[::HBaseConstants::REGIONSERVER]
        formatter.header(%w[TABLE USAGE LIMIT IN_VIOLATION POLICY])
        count = 0
        quotas_admin.get_quota_snapshots(desired_regionserver).each do |table_name, snapshot|
          # Skip this snapshot if it's for a table/namespace the user did not ask for
          next unless accept? table_name, desired_table, desired_namespace
          status = snapshot.getQuotaStatus
          policy = get_policy(status)
          formatter.row([table_name.to_s, snapshot.getUsage.to_s, snapshot.getLimit.to_s,
                         status.isInViolation.to_s, policy])
          count += 1
        end
        formatter.footer(count)
      end

      def get_policy(status)
        # Unwrap the violation policy if it exists
        if status.isInViolation
          status.getPolicy.get.name
        else
          'None'
        end
      end

      def accept?(table_name, desired_table = nil, desired_namespace = nil)
        # Check the table name if given one
        if desired_table && table_name.getQualifierAsString != desired_table
          return false
        end
        # Check the namespace if given one
        if desired_namespace && table_name.getNamespaceAsString != desired_namespace
          return false
        end
        true
      end
    end
  end
end
