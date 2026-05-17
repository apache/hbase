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
    # ShowKeyStatus is a class that provides a Ruby interface to show key statuses via
    # HBase Key Management API.
    class ShowKeyStatus < KeymetaCommandBase
      def help
        <<-EOF
Show key statuses for a given cust:namespace (cust in Base64 format).
If no namespace is specified, the global namespace (*) is used.

Example:
  hbase> show_key_status 'cust:namespace'
  hbase> show_key_status 'cust'
        EOF
      end

      def command(key_info)
        statuses = keymeta_admin.get_key_statuses(key_info)
        print_key_statuses(statuses)
      end
    end
  end
end
