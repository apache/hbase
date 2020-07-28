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
    class RestoreSnapshot < Command
      def help
        <<-EOF
Restore a specified snapshot.
The restore will replace the content of the original table,
bringing back the content to the snapshot state.
The table must be disabled.

Examples:
  hbase> restore_snapshot 'snapshotName'

Following command will restore all acl from snapshot table into the table.

  hbase> restore_snapshot 'snapshotName', {RESTORE_ACL=>true}
EOF
      end

      def command(snapshot_name, args = {})
        raise(ArgumentError, 'Arguments should be a Hash') unless args.is_a?(Hash)
        restore_acl = args.delete(::HBaseConstants::RESTORE_ACL) || false
        admin.restore_snapshot(snapshot_name, restore_acl)
      end
    end
  end
end
