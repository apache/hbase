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
    class ClearBlockCache < Command
      def help
        <<-EOF
Clear all the blocks corresponding to this table from BlockCache. For expert-admins.
Calling this API will drop all the cached blocks specific to a table from BlockCache.
This can significantly impact the query performance as the subsequent queries will
have to retrieve the blocks from underlying filesystem.
For example:

  hbase> clear_block_cache 'TABLENAME'
EOF
      end

      def command(table_name)
        formatter.row([admin.clear_block_cache(table_name)])
        nil
      end
    end
  end
end
