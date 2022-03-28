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
    # create a new table by cloning the existent table schema.
    class CloneTableSchema < Command
      def help
        <<~HELP
          Create a new table by cloning the existent table schema.
          There're no copies of data involved.
          Just copy the table descriptor and split keys.

          Passing 'false' as the optional third parameter will
          not preserve split keys.
          Examples:
            hbase> clone_table_schema 'table_name', 'new_table_name'
            hbase> clone_table_schema 'table_name', 'new_table_name', false
        HELP
      end

      def command(table_name, new_table_name, preserve_splits = true)
        admin.clone_table_schema(table_name, new_table_name, preserve_splits)
      end
    end
  end
end
