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
Set a quota for a user, table, namespace or region server.
Syntax : set_quota TYPE => <type>, <args>

TYPE => THROTTLE
1. User can set throttle quota for user, namespace, table, region server, user over namespace,
   user over table by USER, NAMESPACE, TABLE, REGIONSERVER keys.
   Note: Setting specified region server quota isn't supported currently and using 'all' to
   represent all region servers.
2. User can set throttle quota type either on read, write or on both the requests together(
   read+write, default throttle type) by THROTTLE_TYPE => READ, WRITE, REQUEST.
3. The request limit can be expressed using the form 100req/sec, 100req/min; or can be expressed
   using the form 100k/sec, 100M/min with (B, K, M, G, T, P) as valid size unit; or can be expressed
   using the form 100CU/sec as capacity unit by LIMIT key.
   The valid time units are (sec, min, hour, day).
4. User can set throttle scope to be either MACHINE(default throttle scope) or CLUSTER by
   SCOPE => MACHINE, CLUSTER. MACHINE scope quota means the throttle limit is used by single
   region server, CLUSTER scope quota means the throttle limit is shared by all region servers.
   Region server throttle quota must be MACHINE scope.
   Note: because currently use [ClusterLimit / RsNum] to divide cluster limit to machine limit,
   so it's better to do not use cluster scope quota when you use rs group feature.

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

    hbase> set_quota TYPE => THROTTLE, USER => 'u1', LIMIT => '10CU/sec'
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => WRITE, USER => 'u1', LIMIT => '10CU/sec'
    Unthrottle capacity unit:
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => REQUEST_CAPACITY_UNIT, USER => 'u1', LIMIT => 'NONE'
    Unthrottle read capacity unit:
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => READ_CAPACITY_UNIT, USER => 'u1', LIMIT => 'NONE'
    Unthrottle write capacity unit:
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => WRITE_CAPACITY_UNIT, USER => 'u1', LIMIT => 'NONE'

    hbase> set_quota TYPE => THROTTLE, USER => 'u1', TABLE => 't2', LIMIT => '5K/min'
    hbase> set_quota TYPE => THROTTLE, USER => 'u1', NAMESPACE => 'ns2', LIMIT => NONE

    hbase> set_quota TYPE => THROTTLE, NAMESPACE => 'ns1', LIMIT => '10req/sec'
    hbase> set_quota TYPE => THROTTLE, TABLE => 't1', LIMIT => '10M/sec'
    hbase> set_quota TYPE => THROTTLE, THROTTLE_TYPE => WRITE, TABLE => 't1', LIMIT => '10M/sec'
    hbase> set_quota TYPE => THROTTLE, USER => 'u1', LIMIT => NONE

    hbase> set_quota TYPE => THROTTLE, REGIONSERVER => 'all', LIMIT => '30000req/sec'
    hbase> set_quota TYPE => THROTTLE, REGIONSERVER => 'all', THROTTLE_TYPE => WRITE, LIMIT => '20000req/sec'
    hbase> set_quota TYPE => THROTTLE, REGIONSERVER => 'all', LIMIT => NONE

    hbase> set_quota TYPE => THROTTLE, NAMESPACE => 'ns1', LIMIT => '10req/sec', SCOPE => CLUSTER
    hbase> set_quota TYPE => THROTTLE, NAMESPACE => 'ns1', LIMIT => '10req/sec', SCOPE => MACHINE

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
        if args.key?(::HBaseConstants::TYPE)
          qtype = args.delete(::HBaseConstants::TYPE)
          case qtype
          when ::HBaseQuotasConstants::THROTTLE
            if args[::HBaseConstants::LIMIT].eql? ::HBaseConstants::NONE
              args.delete(::HBaseConstants::LIMIT)
              quotas_admin.unthrottle(args)
            else
              quotas_admin.throttle(args)
            end
          when ::HBaseQuotasConstants::SPACE
            if args[::HBaseConstants::LIMIT].eql? ::HBaseConstants::NONE
              args.delete(::HBaseConstants::LIMIT)
              # Table/Namespace argument is verified in remove_space_limit
              quotas_admin.remove_space_limit(args)
            else
              unless args.key?(::HBaseConstants::LIMIT)
                raise(ArgumentError, 'Expected a LIMIT to be provided')
              end
              unless args.key?(::HBaseConstants::POLICY)
                raise(ArgumentError, 'Expected a POLICY to be provided')
              end

              quotas_admin.limit_space(args)
            end
          else
            raise 'Invalid TYPE argument. got ' + qtype
          end
        elsif args.key?(::HBaseQuotasConstants::GLOBAL_BYPASS)
          quotas_admin.set_global_bypass(args.delete(::HBaseQuotasConstants::GLOBAL_BYPASS), args)
        else
          raise 'Expected TYPE argument'
        end
      end
    end
  end
end
