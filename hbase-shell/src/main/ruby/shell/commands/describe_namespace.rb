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

module Shell
  module Commands
    class DescribeNamespace < Command
      def help
        <<-EOF
Describe the named namespace. For example:
  hbase> describe_namespace 'ns1'
EOF
      end

      # rubocop:disable Metrics/AbcSize
      def command(namespace)
        desc = admin.describe_namespace(namespace)

        formatter.header(['DESCRIPTION'], [64])
        formatter.row([desc], true, [64])
        ns = namespace.to_s
        if admin.exists?(::HBaseQuotasConstants::QUOTA_TABLE_NAME.to_s)
          puts formatter.header(%w[QUOTAS])
          count = quotas_admin.list_quotas(::HBaseConstants::NAMESPACE => ns) do |_, quota|
            formatter.row([quota])
          end
          formatter.footer(count)
        else
          puts 'Quota is disabled'
        end
      end
      # rubocop:enable Metrics/AbcSize
    end
  end
end
