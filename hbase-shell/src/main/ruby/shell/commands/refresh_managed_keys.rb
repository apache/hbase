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
    # RefreshManagedKeys is a class that provides a Ruby interface to refresh managed keys via
    # HBase Key Management API.
    class RefreshManagedKeys < KeymetaCommandBase
      def help
        <<-EOF
Refresh all managed keys for a given cust:namespace (cust in Base64 encoded).
If no namespace is specified, the global namespace (*) is used.

Example:
  hbase> refresh_managed_keys 'cust:namespace'
  hbase> refresh_managed_keys 'cust'
        EOF
      end

      def command(key_info)
        keymeta_admin.refresh_managed_keys(key_info)
        puts "Managed keys refreshed successfully"
      end
    end
  end
end
