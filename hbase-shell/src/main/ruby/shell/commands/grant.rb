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
    class Grant < Command
      def help
        <<-EOF
Grant users specific rights.
Syntax: grant <user or @group>, <permissions> [, <table> [, <column family> [, <column qualifier>]]]
Syntax: grant <user or @group>, <permissions>, <@namespace>

permissions is either zero or more letters from the set "RWXCA".
READ('R'), WRITE('W'), EXEC('X'), CREATE('C'), ADMIN('A')

Note: Groups and users are granted access in the same way, but groups are prefixed with an '@'
      character. Tables and namespaces are specified the same way, but namespaces are
      prefixed with an '@' character.

For example:

    hbase> grant 'bobsmith', 'RWXCA'
    hbase> grant '@admins', 'RWXCA'
    hbase> grant 'bobsmith', 'RWXCA', '@ns1'
    hbase> grant 'bobsmith', 'RW', 't1', 'f1', 'col1'
    hbase> grant 'bobsmith', 'RW', 'ns1:t1', 'f1', 'col1'
EOF
      end

      def command(*args)
        # command form is ambiguous at first argument
        table_name = user = args[0]
        raise(ArgumentError, 'First argument should be a String') unless user.is_a?(String)

        if args[1].is_a?(String)

          # Original form of the command
          #     user in args[0]
          #     permissions in args[1]
          #     table_name in args[2]
          #     family in args[3] or nil
          #     qualifier in args[4] or nil

          permissions = args[1]
          raise(ArgumentError, 'Permissions are not of String type') unless permissions.is_a?(
            String
          )
          table_name = family = qualifier = nil
          table_name = args[2] # will be nil if unset
          unless table_name.nil?
            raise(ArgumentError, 'Table name is not of String type') unless table_name.is_a?(
              String
            )
            family = args[3] # will be nil if unset
            unless family.nil?
              raise(ArgumentError, 'Family is not of String type') unless family.is_a?(String)
              qualifier = args[4] # will be nil if unset
              unless qualifier.nil?
                raise(ArgumentError, 'Qualifier is not of String type') unless qualifier.is_a?(
                  String
                )
              end
            end
          end
          @start_time = Time.now
          security_admin.grant(user, permissions, table_name, family, qualifier)

        elsif args[1].is_a?(Hash)

          # New form of the command, a cell ACL update
          #    table_name in args[0], a string
          #    a Hash mapping users (or groups) to permisisons in args[1]
          #    a Hash argument suitable for passing to Table#_get_scanner in args[2]
          # Useful for feature testing and debugging.

          permissions = args[1]
          raise(ArgumentError, 'Permissions are not of Hash type') unless permissions.is_a?(Hash)
          scan = args[2]
          raise(ArgumentError, 'Scanner specification is not a Hash') unless scan.is_a?(Hash)

          t = table(table_name)
          @start_time = Time.now
          scanner = t._get_scanner(scan)
          count = 0
          iter = scanner.iterator
          while iter.hasNext
            row = iter.next
            row.listCells.each do |cell|
              put = org.apache.hadoop.hbase.client.Put.new(row.getRow)
              put.add(cell)
              t.set_cell_permissions(put, permissions)
              t.table.put(put)
            end
            count += 1
          end
          formatter.footer(count)

        else
          raise(ArgumentError, 'Second argument should be a String or Hash')
        end
      end
    end
  end
end
