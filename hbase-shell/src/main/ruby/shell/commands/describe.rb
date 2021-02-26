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
    class Describe < Command
      def help
        <<-EOF
Describe the named table. For example:
  hbase> describe 't1'
  hbase> describe 'ns1:t1'

Alternatively, you can use the abbreviated 'desc' for the same thing.
  hbase> desc 't1'
  hbase> desc 'ns1:t1'
EOF
      end

      # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
      def command(table)
        column_families = admin.get_column_families(table)

        formatter.header(['Table ' + table.to_s + ' is ' + (admin.enabled?(table) ? 'ENABLED' : 'DISABLED')])
        formatter.row([table.to_s + admin.get_table_attributes(table)], true)
        formatter.header(['COLUMN FAMILIES DESCRIPTION'])
        column_families.each do |column_family|
          formatter.row([column_family.to_s], true)
          puts
        end
        formatter.footer
        if admin.exists?(::HBaseQuotasConstants::QUOTA_TABLE_NAME.to_s)
          if table.to_s != 'hbase:meta'
            # No QUOTAS if hbase:meta table
            puts
            formatter.header(%w[QUOTAS])
            count = quotas_admin.list_quotas(::HBaseConstants::TABLE => table.to_s) do |_, quota|
              formatter.row([quota])
            end
            formatter.footer(count)
          end
        else
          puts 'Quota is disabled'
        end
      end
      # rubocop:enable Metrics/AbcSize, Metrics/MethodLength
    end
  end
end
