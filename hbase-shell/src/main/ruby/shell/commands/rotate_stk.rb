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
# frozen_string_literal: true

require 'shell/commands/keymeta_command_base'

module Shell
  module Commands
    # RotateStk is a class that provides a Ruby interface to rotate the System Key (STK)
    # via HBase Key Management API.
    class RotateStk < KeymetaCommandBase
      def help
        <<-EOF
Rotate the System Key (STK) if a new key is detected.
This command checks for a new system key and propagates it to all region servers.
Returns true if a new key was detected and rotated, false otherwise.

Example:
  hbase> rotate_stk
        EOF
      end

      def command
        result = keymeta_admin.rotate_stk
        if result
          formatter.row(['System Key rotation was performed successfully and cache was refreshed ' \
            'on all region servers'])
        else
          formatter.row(['No System Key change was detected'])
        end
        result
      end
    end
  end
end

