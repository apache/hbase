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
    class ChangeSft < Command
      def help
        <<-EOF
Change table's or table column family's sft. Examples:

  hbase> change_sft 't1','FILE'
  hbase> change_sft 't2','cf1','FILE'
EOF
      end

      def command(*args)
        arg_length = args.length
        if arg_length == 2
          tableName = TableName.valueOf(args[0])
          sft = args[1]
          admin.modify_table_sft(tableName, sft)
        elsif arg_length == 3
          tableName = TableName.valueOf(args[0])
          family = args[1]
          family_bytes = family.to_java_bytes
          sft = args[2]
          admin.modify_table_family_sft(tableName, family_bytes, sft)
        else
          raise(ArgumentError, 'Argument length should be two or three.')
        end
      end
    end
  end
end
