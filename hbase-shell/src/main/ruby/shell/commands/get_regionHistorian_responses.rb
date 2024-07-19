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

# Retrieve latest region history responses maintained in memory by RegionServers

module Shell
  module Commands
    # Retrieve latest region history responses
    class GetRegionHistorianResponses < Command
      def help
        <<-EOF
Retrieve latest RegionHistorian Responses maintained by each or specific RegionServers.
Specify '*' to include all RS otherwise array of server names for specific
RS. A server name is the host, port plus startcode of a RegionServer.
e.g.: host187.example.com,60020,1289493121758 (find servername in
master ui or when you do detailed status in shell)

Provide optional filter parameters as Hash.
Default Limit of each server for providing no of slow log records is 10. User can specify
more limit by 'LIMIT' param in case more than 10 records should be retrieved.

Examples:

  hbase> get_regionHistorian_responses '*'                                 => get regionHistorian responses from all RS
  hbase> get_regionHistorian_responses '*', {'LIMIT' => 50}                => get regionHistorian responses from all RS with 50
                                                                              records limit (default limit: 10)
  hbase> get_regionHistorian_responses '*', {'REGION_NAME' => 'hbase:meta,,'}
                                                                           => get regionHistorian responses only related to meta region
  hbase> get_regionHistorian_responses '*', {'TABLE_NAME' => 't1'}         => get regionHistorian responses only related to t1 table
  hbase> get_regionHistorian_responses '*', {'PID' => 102}                 => get regionHistorian responses with specified pid
  hbase> get_regionHistorian_responses '*', {'PPID' => 102}                => get regionHistorian responses with specified ppid
  hbase> get_regionHistorian_responses '*', {'REGION_NAME' => 'hbase:meta,,1', 'TABLE_NAME' => 't1'}
                                                                           => get regionHistorian responses with given region name
                                                                              or table name
  hbase> get_regionHistorian_responses '*', {'REGION_NAME' => 'hbase:meta,,1', 'PID' => 102} =
                                                                           => getregionHistorian responses with given region name or pid
  hbase> get_regionHistorian_responses '*', {'REGION_NAME' => 'hbase:meta,,1', 'PPID' => 102} =
                                                                           => getregionHistorian responses with given region name or ppid
  hbase> get_regionHistorian_responses '*', {'TABLE_NAME' => 't1', 'PID' => 102} =
                                                                           => getregionHistorian responses with given table name or pid
  hbase> get_regionHistorian_responses '*', {'TABLE_NAME' => 't1', 'PID' => 102} =
                                                                           => getregionHistorian responses with given table name or pid

All of above queries with filters have default OR operation applied i.e. all
records with any of the provided filters applied will be returned. However,
we can also apply AND operator i.e. all records that match all (not any) of
the provided filters should be returned.

  hbase> get_regionHistorian_responses '*', {'REGION_NAME' => 'hbase:meta,,1', 'TABLE_NAME' => 't1', 'FILTER_BY_OP' => 'AND'}
                                                                   => get regionHistorian responses with given region name
                                                                      and table name, both should match
  hbase> get_regionHistorian_responses '*', {'REGION_NAME' => 'hbase:meta,,1', 'PID' => 103, 'FILTER_BY_OP' => 'AND'}
                                                                   => get regionHistorian responses with given region name
                                                                      and pid, both should match
  hbase> get_regionHistorian_responses '*', {'REGION_NAME' => 'hbase:meta,,1', 'PPID' => 115, 'FILTER_BY_OP' => 'AND'}
                                                                   => get regionHistorian responses with given region name
                                                                      and ppid, both should match

  hbase> get_regionHistorian_responses '*', {'REGION_NAME' => 'hbase:meta,,1', 'TABLE_NAME' => 't1', 'FILTER_BY_OP' => 'OR'}
                                                                   => get regionHistorian responses with given region name
                                                                      or table name, any one can match
  hbase> get_regionHistorian_responses '*', {'PID' => 114, 'TABLE_NAME' => 't1', 'FILTER_BY_OP' => 'OR'}
                                                                   => get regionHistorian responses with given pid
                                                                      or table name, any one can match
  hbase> get_regionHistorian_responses '*', {'PPID' => 114, 'TABLE_NAME' => 't1', 'FILTER_BY_OP' => 'OR'}
                                                                     => get regionHistorian responses with given ppid
                                                                        or table name, any one can match
Since OR is the default filter operator, without providing 'FILTER_BY_OP', query will have
same result as providing 'FILTER_BY_OP' => 'OR'.

Sometimes output can be long pretty printed json for user to scroll in
a single screen and hence user might prefer
redirecting output of get_regionHistorian_responses to a file.

Example:

echo "get_regionHistorian_responses '*'" | hbase shell > xyz.out 2>&1

        EOF
      end

      def command(server_names, args = {})
        unless args.is_a? Hash
          raise 'Filter parameters are not Hash'
        end
        region_Historian_responses_arr = admin.get_regionHistorian_responses(server_names, args)
        puts 'Retrieved RegionHistorian Responses from RegionServers'
        puts region_Historian_responses_arr
      end
    end
  end
end
