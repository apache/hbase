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
    class ListProcedures < Command
      def help
        return <<-EOF
List all procedures in hbase. Examples:

  hbase> list_procedures
EOF
      end

      def command()
        now = Time.now
        formatter.header([ "Id", "Name", "State", "Start_Time", "Last_Update" ])

        list = admin.list_procedures()
        list.each do |proc|
          start_time = Time.at(proc.getStartTime / 1000).to_s
          last_update = Time.at(proc.getLastUpdate / 1000).to_s
          formatter.row([ proc.getProcId, proc.getProcName, proc.getProcState, start_time, last_update ])
        end

        formatter.footer(now, list.size)
      end
    end
  end
end
