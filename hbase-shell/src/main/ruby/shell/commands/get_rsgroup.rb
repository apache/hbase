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
    class GetRsgroup < Command
      def help
        <<-EOF
Get a RegionServer group's information.

Example:

  hbase> get_rsgroup 'default'

EOF
      end

      def command(group_name)
        group = rsgroup_admin.get_rsgroup(group_name)

        formatter.header(['SERVERS'])
        group.getServers.each do |server|
          formatter.row([server.toString])
        end
        formatter.footer

        formatter.header(['TABLES'])
        tables = rsgroup_admin.list_tables_in_rs_group(group_name)
        tables.each do |table|
          formatter.row([table.getNameAsString])
        end
        formatter.footer
      end
    end
  end
end
