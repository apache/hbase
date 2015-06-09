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
    class SetQuota < Command
      def help
        return <<-EOF
Set a quota for a user, table, or namespace.
Syntax : set_quota TYPE => <type>, <args>

TYPE => THROTTLE
User can either set quota on read, write or on both the requests together(i.e., read+write)
The read, write, or read+write(default throttle type) request limit can be expressed using
the form 100req/sec, 100req/min and the read, write, read+write(default throttle type) limit
can be expressed using the form 100k/sec, 100M/min with (B, K, M, G, T, P) as valid size unit
and (sec, min, hour, day) as valid time unit.
Currently the throttle limit is per machine - a limit of 100req/min
means that each machine can execute 100req/min.

For example:

    hbase> set_quota TYPE => THROTTLE, USER => 'u1', LIMIT => '10req/sec'
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => READ, USER => 'u1', LIMIT => '10req/sec'

    hbase> set_quota TYPE => THROTTLE, USER => 'u1', LIMIT => '10M/sec'
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => WRITE, USER => 'u1', LIMIT => '10M/sec'

    hbase> set_quota TYPE => THROTTLE, USER => 'u1', TABLE => 't2', LIMIT => '5K/min'
    hbase> set_quota TYPE => THROTTLE, USER => 'u1', NAMESPACE => 'ns2', LIMIT => NONE

    hbase> set_quota TYPE => THROTTLE, NAMESPACE => 'ns1', LIMIT => '10req/sec'
    hbase> set_quota TYPE => THROTTLE, TABLE => 't1', LIMIT => '10M/sec'
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => WRITE, TABLE => 't1', LIMIT => '10M/sec'
    hbase> set_quota TYPE => THROTTLE, USER => 'u1', LIMIT => NONE
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => WRITE, USER => 'u1', LIMIT => NONE

    hbase> set_quota USER => 'u1', GLOBAL_BYPASS => true
EOF
      end

      def command(args = {})
        if args.has_key?(TYPE)
          qtype = args.delete(TYPE)
          case qtype
            when THROTTLE
              if args[LIMIT].eql? NONE
                args.delete(LIMIT)
                quotas_admin.unthrottle(args)
              else
                quotas_admin.throttle(args)
              end
          else
            raise "Invalid TYPE argument. got " + qtype
          end
        elsif args.has_key?(GLOBAL_BYPASS)
          quotas_admin.set_global_bypass(args.delete(GLOBAL_BYPASS), args)
        else
          raise "Expected TYPE argument"
        end
      end
    end
  end
end
