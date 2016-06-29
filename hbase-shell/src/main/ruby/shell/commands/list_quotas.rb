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
    class ListQuotas < Command
      def help
        return <<-EOF
List the quota settings added to the system.
You can filter the result based on USER, TABLE, or NAMESPACE.

For example:

    hbase> list_quotas
    hbase> list_quotas USER => 'bob.*'
    hbase> list_quotas USER => 'bob.*', TABLE => 't1'
    hbase> list_quotas USER => 'bob.*', NAMESPACE => 'ns.*'
    hbase> list_quotas TABLE => 'myTable'
    hbase> list_quotas NAMESPACE => 'ns.*'
EOF
      end

      def command(args = {})
        now = Time.now
        formatter.header(["OWNER", "QUOTAS"])

        #actually do the scanning
        count = quotas_admin.list_quotas(args) do |row, cells|
          formatter.row([ row, cells ])
        end

        formatter.footer(now, count)
      end
    end
  end
end
