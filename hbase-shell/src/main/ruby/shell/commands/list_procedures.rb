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

require 'json'

module Shell
  module Commands
    class ListProcedures < Command
      def help
        <<-EOF
List all procedures in hbase. For example:

  hbase> list_procedures
EOF
      end

      def command
        formatter.header(%w[PID Name State Submitted Last_Update Parameters])
        list = JSON.parse(admin.list_procedures)
        list.each do |proc|
          submitted_time = Time.at(Integer(proc['submittedTime']) / 1000).to_s
          last_update = Time.at(Integer(proc['lastUpdate']) / 1000).to_s
          formatter.row([proc['procId'], proc['className'], proc['state'],
                         submitted_time, last_update, proc['stateMessage']])
        end

        formatter.footer(list.size)
      end
    end
  end
end
