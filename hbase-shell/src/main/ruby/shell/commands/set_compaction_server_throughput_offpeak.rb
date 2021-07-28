#
# Copyright The Apache Software Foundation
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
    class SetCompactionServerThroughputOffpeak < Command
      def help
        <<-EOF
Set the compaction server total throughput offpeak.

Examples:

  # set bandwidth=2MB for total throughput offpeak.
  hbase> set_compaction_server_throughput_offpeak 2097152

        EOF
      end

      def command(bandwidth = 0)
        formatter.header(%w(['BOUND' 'BANDWIDTH']))
        throughtput = admin.update_compaction_server_total_throughput(0, 0, bandwidth)
        throughtput.each { |k, v| formatter.row([k, java.lang.String.valueOf(v)]) }
      end
    end
  end
end
