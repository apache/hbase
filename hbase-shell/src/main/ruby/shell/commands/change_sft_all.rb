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
    class ChangeSftAll < Command
      def help
        <<-EOF
Change all of the tables's sft matching the given regex:

  hbase> change_sft_all 't.*','FILE'
  hbase> change_sft_all 'ns:.*','FILE'
  hbase> change_sft_all 'ns:t.*','FILE'
EOF
      end

      def command(*args)
        arg_length = args.length
        if arg_length == 2
          tableRegex = args[0]
          tableList = admin.list(tableRegex)
          count = tableList.size
          sft = args[1]
          tableList.each do |table|
            formatter.row([table])
          end
          puts "\nChange the above #{count} tables's sft (y/n)?" unless count == 0
          answer = 'n'
          answer = gets.chomp unless count == 0
          puts "No tables matched the regex #{tableRegex}" if count == 0
          return unless answer =~ /y.*/i
          tableList.each do |table|
            tableName = TableName.valueOf(table)
            admin.modify_table_sft(tableName, sft)
          end
        else
          raise(ArgumentError, 'Argument length should be two.')
        end
      end
    end
  end
end
