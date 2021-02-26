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
    class GetSplits < Command
      def help
        <<-EOF
Get the splits of the named table:
  hbase> get_splits 't1'
  hbase> get_splits 'ns1:t1'

The same commands also can be run on a table reference. Suppose you had a reference
t to table 't1', the corresponding command would be:

  hbase> t.get_splits
EOF
      end

      def command(table)
        get_splits(table(table))
      end

      def get_splits(table)
        splits = table._get_splits_internal
        puts(format('Total number of splits = %<numsplits>d',
                    numsplits: (splits.size + 1)))
        puts splits
        splits
      end
    end
  end
end

::Hbase::Table.add_shell_command('get_splits')
