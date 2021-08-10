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
    class BalanceRsgroup < Command
      def help
        <<-EOF
Balance a RegionServer group

Parameter can be "force" or "dry_run":
 - "dry_run" will run the balancer to generate a plan, but will not actually execute that plan.
   This is useful for testing out new balance configurations. See the active HMaster logs for the results of the dry_run.
 - "ignore_rit" tells master whether we should force the balancer to run even if there is region in transition.
   WARNING: For experts only. Forcing a balance may do more damage than repair when assignment is confused

Example:

  hbase> balance_rsgroup 'my_group'
  hbase> balance_rsgroup 'my_group', 'ignore_rit'
  hbase> balance_rsgroup 'my_group', 'dry_run'
  hbase> balance_rsgroup 'my_group', 'dry_run', 'ignore_rit'

EOF
      end

      def command(group_name, *args)
        # Returns true if balancer was run, otherwise false.
        resp = rsgroup_admin.balance_rs_group(group_name, args)
        if resp.isBalancerRan
          formatter.row(["Balancer ran"])
          formatter.row(["Moves calculated: #{resp.getMovesCalculated}, moves executed: #{resp.getMovesExecuted}"])
        else
          formatter.row(["Balancer did not run. See logs for details."])
        end
        resp.isBalancerRan
      end
    end
  end
end
