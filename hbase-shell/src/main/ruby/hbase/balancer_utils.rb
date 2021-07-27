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

include Java

java_import org.apache.hadoop.hbase.client.BalanceRequest

module Hbase
  class BalancerUtils
    def self.create_balance_request(args)
      args = args.first if args.first.is_a?(Array) and args.size == 1
      if args.nil? or args.empty?
        return BalanceRequest.defaultInstance()
      elsif args.size > 2
        raise ArgumentError, "Illegal arguments #{args}. Expected between 0 and 2 arguments, but got #{args.size}."
      end

      builder = BalanceRequest.newBuilder()

      index = 0
      args.each do |arg|
        if !arg.is_a?(String)
          raise ArgumentError, "Illegal argument in index #{index}: #{arg}. All arguments must be strings, but got #{arg.class}."
        end

        case arg
        when 'force', 'ignore_rit'
          builder.setIgnoreRegionsInTransition(true)
        when 'dry_run'
          builder.setDryRun(true)
        else
          raise ArgumentError, "Illegal argument in index #{index}: #{arg}. Unknown option #{arg}, expected 'force', 'ignore_rit', or 'dry_run'."
        end

        index += 1
      end

      return builder.build()
    end
  end
end
