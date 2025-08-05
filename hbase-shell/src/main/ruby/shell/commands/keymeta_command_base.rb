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

module Shell
  module Commands
    # KeymetaCommandBase is a base class for all key management commands.
    class KeymetaCommandBase < Command
      def print_key_statuses(statuses)
        formatter.header(%w[ENCODED-KEY NAMESPACE STATUS METADATA METADATA-HASH REFRESH-TIMESTAMP])
        statuses.each { |status| formatter.row(format_status_row(status)) }
        formatter.footer(statuses.size)
      end

      private

      def format_status_row(status)
        [
          status.getKeyCustodianEncoded,
          status.getKeyNamespace,
          status.getKeyStatus.toString,
          status.getKeyMetadata,
          status.getKeyMetadataHashEncoded,
          status.getRefreshTimestamp
        ]
      end
    end
  end
end
