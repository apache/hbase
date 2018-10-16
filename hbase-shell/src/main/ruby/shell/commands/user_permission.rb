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
    class UserPermission < Command
      def help
        <<-EOF
Show all permissions for the particular user.
Syntax : user_permission <table>

Note: A namespace must always precede with '@' character.

For example:

    hbase> user_permission
    hbase> user_permission '@ns1'
    hbase> user_permission '@.*'
    hbase> user_permission '@^[a-c].*'
    hbase> user_permission 'table1'
    hbase> user_permission 'namespace1:table1'
    hbase> user_permission '.*'
    hbase> user_permission '^[A-C].*'
EOF
      end

      def command(table_regex = nil)
        # admin.user_permission(table_regex)
        formatter.header(['User', 'Namespace,Table,Family,Qualifier:Permission'])

        count = security_admin.user_permission(table_regex) do |user, permission|
          formatter.row([user, permission])
        end

        formatter.footer(count)
      end
    end
  end
end
