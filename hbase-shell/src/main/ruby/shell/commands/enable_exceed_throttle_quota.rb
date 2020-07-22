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
    class EnableExceedThrottleQuota < Command
      def help
        <<-EOF
Enable exceed throttle quota. Returns previous exceed throttle quota enabled value.
NOTE: if quota is not enabled, this will not work and always return false.

If enabled, allow requests exceed user/table/namespace throttle quotas when region
server has available quota.

There are two limits if enable exceed throttle quota. First, please set region server
quota. Second, please make sure that all region server throttle quotas are in seconds
time unit, because once previous requests exceed their quota and consume region server
quota, quota in other time units may be refilled in a long time, which may affect later
requests.


Examples:
    hbase> enable_exceed_throttle_quota
        EOF
      end

      def command
        prev_state = !!quotas_admin.switch_exceed_throttle_quota(true)
        formatter.row(["Previous exceed throttle quota enabled : #{prev_state}"])
        prev_state
      end
    end
  end
end
