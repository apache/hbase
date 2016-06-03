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
    # Command for set switch for split and merge
    class SplitormergeSwitch < Command
      def help
        print <<-EOF
Enable/Disable one switch. You can set switch type 'SPLIT' or 'MERGE'. Returns previous split state.
Examples:

  hbase> splitormerge_switch 'SPLIT', true
  hbase> splitormerge_switch 'SPLIT', false
EOF
      end

      def command(switch_type, enabled)
        formatter.row(
          [admin.splitormerge_switch(switch_type, enabled) ? 'true' : 'false']
        )
      end
    end
  end
end
