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
    class PbeGetStatuses < Command
      def help
        <<-EOF
Get key statuses for a given prefix:namespace (prefix in Base64 format).
If no namespace is specified, the global namespace (*) is used.
EOF
      end

      def command(pbe_prefix)
        formatter.header(['ENCODED-KEY', 'NAMESPACE', 'STATUS', 'METADATA', 'METADATA-HASH', 'REFRESH-TIMESTAMP'])
        statuses = pbe_admin.show_pbe_status(pbe_prefix)
        statuses.each { |status|
          formatter.row([
            status.getPBEPrefixEncoded(),
            status.getKeyNamespace(),
            status.getKeyStatus().toString(),
            status.getKeyMetadata(),
            status.getKeyMetadataHashEncoded(),
            status.getRefreshTimestamp()
          ])
        }
        formatter.footer(statuses.size())
      end
    end
  end
end
