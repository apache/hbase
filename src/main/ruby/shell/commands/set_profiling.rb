#
# Copyright 2010 The Apache Software Foundation
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
    class SetProfiling < Command
      def help
        return <<-EOF
          Set profiling on or off. Profiling data can be printed
          using get_profiling after an RPC call. Examples:

            hbase> set_profiling 'on'
            hbase> get 't1', 'r1'
            hbase> get_profiling
            hbase> set_profiling 'off'
        EOF
      end

      def command(prof)
        if prof == 'on'
          shell.profiling = true
          puts "Profiling is on"
        elsif prof == 'off'
          shell.profiling = false
          puts "Profiling is off"
        else
          puts "Command ignored"
        end
      end
    end
  end
end
