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
    class HbckChoreRun < Command
      def help
        <<-EOF
Request HBCK chore to run at master side. It will try to find the orphan
regions on RegionServer or FileSystem and find the inconsistent regions.
You can check the HBCK report at Master web UI. Returns true if HBCK chore
ran, or false if HBCK chore is already running.

  hbase> hbck_chore_run

EOF
      end

      def command
        admin.hbck_chore_run
      end
    end
  end
end
