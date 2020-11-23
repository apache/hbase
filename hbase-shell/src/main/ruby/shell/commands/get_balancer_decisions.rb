#
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# Retrieve latest balancer decisions maintained in memory by HMaster

module Shell
  module Commands
    # Retrieve latest large log responses
    class GetBalancerDecisions < Command
      def help
        <<-EOF
Retrieve latest balancer decisions made by LoadBalancers.

Examples:

  hbase> get_balancer_decisions                       => Retrieve recent balancer decisions with
                                                         region plans
  hbase> get_balancer_decisions LIMIT => 10           => Retrieve 10 most recent balancer decisions
                                                         with region plans

        EOF
      end

      def command(args = {})
        unless args.is_a? Hash
          raise 'Filter parameters are not Hash'
        end

        balancer_decisions_resp_arr = admin.get_balancer_decisions(args)
        puts 'Retrieved BalancerDecision Responses'
        puts balancer_decisions_resp_arr
      end
    end
  end
end
