#!/usr/bin/env hbase-jruby
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# Prints the current split or merge status
module Shell
  module Commands
    # Command for check split or merge switch status
    class SplitormergeEnabled < Command
      def help
        print <<-EOF
Query the switch's state. You can set switch type, 'SPLIT' or 'MERGE'
Examples:

  hbase> splitormerge_enabled 'SPLIT'
EOF
      end

      def command(switch_type)
        formatter.row(
          [admin.splitormerge_enabled(switch_type) ? 'true' : 'false']
        )
      end
    end
  end
end
