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
    class CompactMob < Command
      def help
        return <<-EOF
          Run compaction on a mob enabled column family
          or all mob enabled column families within a table
          Examples:
          Compact a column family within a table:
          hbase> compact_mob 't1', 'c1'
          Compact all mob enabled column families
          hbase> compact_mob 't1'
        EOF
      end

      def command(table_name, family = nil)
        format_simple_command do
          admin.compact_mob(table_name, family)
        end
      end
    end
  end
end
