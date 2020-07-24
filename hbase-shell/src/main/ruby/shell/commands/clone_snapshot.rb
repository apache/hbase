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
    class CloneSnapshot < Command
      def help
        <<-EOF
Create a new table by cloning the snapshot content.
There're no copies of data involved.
And writing on the newly created table will not influence the snapshot data.

Examples:
  hbase> clone_snapshot 'snapshotName', 'tableName'
  hbase> clone_snapshot 'snapshotName', 'namespace:tableName'

Following command will restore all acl from origin snapshot table into the
newly created table.

  hbase> clone_snapshot 'snapshotName', 'namespace:tableName', {RESTORE_ACL=>true}
EOF
      end

      def command(snapshot_name, table, args = {})
        raise(ArgumentError, 'Arguments should be a Hash') unless args.is_a?(Hash)
        restore_acl = args.delete(::HBaseConstants::RESTORE_ACL) || false
        admin.clone_snapshot(snapshot_name, table, restore_acl)
      end

      def handle_exceptions(cause, *args)
        if cause.is_a?(org.apache.hadoop.hbase.TableExistsException)
          tableName = args[1]
          raise "Table already exists: #{tableName}!"
        end
        if cause.is_a?(org.apache.hadoop.hbase.NamespaceNotFoundException)
          namespace_name = args[1].split(':')[0]
          raise "Unknown namespace: #{namespace_name}!"
        end
      end
    end
  end
end
