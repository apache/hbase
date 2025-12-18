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
    # DisableManagedKey is a class that provides a Ruby interface to disable a managed key via
    # HBase Key Management API.
    class DisableManagedKey < KeymetaCommandBase
      def help
        <<-EOF
Disable a managed key for a given cust:namespace (cust in Base64 encoded) and key metadata hash
(Base64 encoded). If no namespace is specified, the global namespace (*) is used.

Example:
  hbase> disable_managed_key 'cust:namespace key_metadata_hash_base64'
  hbase> disable_managed_key 'cust key_metadata_hash_base64'
        EOF
      end

      def command(key_info, key_metadata_hash_base64)
        statuses = [keymeta_admin.disable_managed_key(key_info, key_metadata_hash_base64)]
        print_key_statuses(statuses)
      end
    end
  end
end
