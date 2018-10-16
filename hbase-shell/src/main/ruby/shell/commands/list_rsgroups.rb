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
    class ListRsgroups < Command
      def help
        <<-EOF
List all RegionServer groups. Optional regular expression parameter can
be used to filter the output.

Example:

  hbase> list_rsgroups
  hbase> list_rsgroups 'abc.*'

EOF
      end

      def command(regex = '.*')
        formatter.header(['NAME', 'SERVER / TABLE'])

        regex = /#{regex}/ unless regex.is_a?(Regexp)
        list = rsgroup_admin.list_rs_groups
        groups = 0

        list.each do |group|
          next unless group.getName.match(regex)

          groups += 1
          group_name_printed = false

          group.getServers.each do |server|
            if group_name_printed
              group_name = ''
            else
              group_name = group.getName
              group_name_printed = true
            end

            formatter.row([group_name, 'server ' + server.toString])
          end

          group.getTables.each do |table|
            if group_name_printed
              group_name = ''
            else
              group_name = group.getName
              group_name_printed = true
            end

            formatter.row([group_name, 'table ' + table.getNameAsString])
          end

          formatter.row([group.getName, '']) unless group_name_printed
        end

        formatter.footer(groups)
      end
    end
  end
end
