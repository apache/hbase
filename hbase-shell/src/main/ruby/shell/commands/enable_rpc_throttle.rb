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
    class EnableRpcThrottle < Command
      def help
        <<-EOF
Enable quota rpc throttle. Returns previous rpc throttle enabled value.
NOTE: if quota is not enabled, this will not work and always return false.

Examples:
    hbase> enable_rpc_throttle
        EOF
      end

      def command
        prev_state = !!quotas_admin.switch_rpc_throttle(true)
        formatter.row(["Previous rpc throttle state : #{prev_state}"])
        prev_state
      end
    end
  end
end
