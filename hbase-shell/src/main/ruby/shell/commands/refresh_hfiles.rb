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
    class RefreshHfiles < Command
      def help
        return <<-EOF
Refresh HFiles/storefiles for table(s) or namespaces.

Allowed Syntax:
hbase> refresh_hfiles
hbase> refresh_hfiles 'TABLE_NAME' => 'test_table'
hbase> refresh_hfiles 'TABLE_NAME' => 'namespace:test_table'
hbase> refresh_hfiles 'NAMESPACE' => 'test_namespace'

Behavior:
- Without any argument, it refreshes HFiles for all user tables in HBase.
- If 'TABLE_NAME' is provided: Refreshes HFiles for that specific table.
   - If provided without a namespace qualifier (e.g., 'TABLE_NAME' => 'test_table'),
    it refreshes HFiles for that table in the default namespace.
   - If provided with a namespace qualifier (e.g., 'TABLE_NAME' => 'namespace:test_table'),
    it refreshes HFiles for the table in the specified namespace.
- With 'NAMESPACE', it refreshes HFiles for all tables in the given namespace.
On successful submission, it returns the procedure ID (procId). Otherwise, it throws an exception.

Important Note:
This command should ideally be run on a read-replica cluster,
 and only after successfully executing refresh_meta.

Not Allowed:
hbase> refresh_hfiles 'TABLE_NAME' => 'test_table', 'NAMESPACE' => 'test_namespace'

Passing both 'TABLE_NAME' and 'NAMESPACE' is not allowed to avoid ambiguity.
Otherwise, it is unclear whether the user intends to:
1. Refresh HFiles for 'test_table' under 'test_namespace',
or
2. Refresh HFiles for 'test_table' under the default namespace and
   all tables under 'test_namespace'.
To prevent such confusion, only one argument should be provided per command.

EOF
      end
      def command(args = {})
        admin.refresh_hfiles(args)
      end
    end
  end
end
