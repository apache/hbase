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
    class RefreshMeta < Command
      def help
        <<-EOF
Refresh the hbase:meta table by syncing with backing storage.
This command is used in Read Replica clusters to pick up new
tables and regions from the shared storage.
Examples:

  hbase> refresh_meta

The command returns a procedure ID that can be used to track the progress
of the meta table refresh operation.
EOF
      end

      def command
        proc_id = admin.refresh_meta
        formatter.row(["Refresh meta procedure submitted. Procedure ID: #{proc_id}"])
        proc_id
      end
    end
  end
end
