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
    class ShowRsgroupConfig < Command
      def help
        <<-EOF
Show the configuration of a special RSGroup.
Example:
    hbase> show_rsgroup_config 'group'
EOF
      end

      def command(group)
        formatter.header(%w['KEY' 'VALUE'])
        config = rsgroup_admin.get_rsgroup(group).getConfiguration
        config.each { |key, val|
          formatter.row([key, val])
        }
        formatter.footer(config.size)
      end
    end
  end
end