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
        <<-EOF
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
    Unthrottle number of requests:
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => REQUEST_NUMBER, USER => 'u1', LIMIT => 'NONE'
    Unthrottle number of read requests:
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => READ_NUMBER, USER => 'u1', LIMIT => NONE
    Unthrottle number of write requests:
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => WRITE_NUMBER, USER => 'u1', LIMIT => NONE

    hbase> set_quota TYPE => THROTTLE, USER => 'u1', LIMIT => '10M/sec'
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => WRITE, USER => 'u1', LIMIT => '10M/sec'
    Unthrottle data size:
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => REQUEST_SIZE, USER => 'u1', LIMIT => 'NONE'
    Unthrottle read data size:
    hbase> set_quota TYPE => THROTTLE, USER => 'u1', THROTTLE_TYPE => READ_SIZE, LIMIT => 'NONE'
    Unthrottle write data size:
    hbase> set_quota TYPE => THROTTLE, USER => 'u1', THROTTLE_TYPE => WRITE_SIZE, LIMIT => 'NONE'

    hbase> set_quota TYPE => THROTTLE, USER => 'u1', TABLE => 't2', LIMIT => '5K/min'
    hbase> set_quota TYPE => THROTTLE, USER => 'u1', NAMESPACE => 'ns2', LIMIT => NONE

    hbase> set_quota TYPE => THROTTLE, NAMESPACE => 'ns1', LIMIT => '10req/sec'
    hbase> set_quota TYPE => THROTTLE, TABLE => 't1', LIMIT => '10M/sec'
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => WRITE, TABLE => 't1', LIMIT => '10M/sec'
    hbase> set_quota TYPE => THROTTLE, USER => 'u1', LIMIT => NONE

    hbase> set_quota USER => 'u1', GLOBAL_BYPASS => true

TYPE => SPACE
Users can either set a quota on a table or a namespace. The quota is a limit on the target's
size on the FileSystem and some action to take when the target exceeds that limit. The limit
is in bytes and can expressed using standard metric suffixes (B, K, M, G, T, P), defaulting
to bytes if not provided. Different quotas can be applied to one table at the table and namespace
level; table-level quotas take priority over namespace-level quotas.

There are a limited number of policies to take when a quota is violation, listed in order of
least strict to most strict.

  NO_INSERTS - No new data is allowed to be ingested (e.g. Put, Increment, Append).
  NO_WRITES - Same as NO_INSERTS but Deletes are also disallowed.
  NO_WRITES_COMPACTIONS - Same as NO_WRITES but compactions are also disallowed.
  DISABLE - The table(s) are disabled.

For example:

  hbase> set_quota TYPE => SPACE, TABLE => 't1', LIMIT => '1G', POLICY => NO_INSERTS
  hbase> set_quota TYPE => SPACE, TABLE => 't2', LIMIT => '50G', POLICY => DISABLE
  hbase> set_quota TYPE => SPACE, TABLE => 't3', LIMIT => '2T', POLICY => NO_WRITES_COMPACTIONS
  hbase> set_quota TYPE => SPACE, NAMESPACE => 'ns1', LIMIT => '50T', POLICY => NO_WRITES

Space quotas can also be removed via this command. To remove a space quota, provide NONE
for the limit.

For example:

  hbase> set_quota TYPE => SPACE, TABLE => 't1', LIMIT => NONE
  hbase> set_quota TYPE => SPACE, NAMESPACE => 'ns1', LIMIT => NONE

EOF
      end

      def command(args = {})
        if args.key?(TYPE)
          qtype = args.delete(TYPE)
          case qtype
          when THROTTLE
            if args[LIMIT].eql? NONE
              args.delete(LIMIT)
              quotas_admin.unthrottle(args)
            else
              quotas_admin.throttle(args)
            end
          when SPACE
            if args[LIMIT].eql? NONE
              args.delete(LIMIT)
              # Table/Namespace argument is verified in remove_space_limit
              quotas_admin.remove_space_limit(args)
            else
              raise(ArgumentError, 'Expected a LIMIT to be provided') unless args.key?(LIMIT)
              raise(ArgumentError, 'Expected a POLICY to be provided') unless args.key?(POLICY)
              quotas_admin.limit_space(args)
            end
          else
            raise 'Invalid TYPE argument. got ' + qtype
          end
        elsif args.key?(GLOBAL_BYPASS)
          quotas_admin.set_global_bypass(args.delete(GLOBAL_BYPASS), args)
        else
          raise 'Expected TYPE argument'
        end
      end
    end
  end
end
