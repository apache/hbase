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
    class Balancer < Command
      def help
        return <<-EOF
Trigger the cluster balancer. Returns true if balancer ran and was able to
tell the region servers to unassign all the regions to balance  (the re-assignment itself is async). 
Otherwise false (Will not run if regions in transition).
Parameter tells master whether we should force balance even if there is region in transition.

WARNING: For experts only. Forcing a balance may do more damage than repair
when assignment is confused

Examples:

  hbase> balancer
  hbase> balancer "force"
EOF
      end

      def command(force=nil)
        force_balancer = 'false'
        if force == 'force'
          force_balancer = 'true'
        elsif !force.nil?
          raise ArgumentError, "Invalid argument #{force}."
        end
        formatter.row([admin.balancer(force_balancer)? "true": "false"])
      end
    end
  end
end
