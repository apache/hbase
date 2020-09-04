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
    class SetFormatter < Command
      def help
        <<-EOF
Sets the table formatter for the shell. Not all commands use the table
formatter, but many do.

The :old_school formatter is provided for compatibility. This will use the
formatter added in HBase 1.x instead of the table formatter.

The :aligned formatter will make sure all the output columns line up. By
default, it allows the contents of each cell to spill into the next cell, but
you can disable this with OVERFLOW.TRUNCATE as seen in the examples below.

The :unaligned formatter simply prints delimits cells and rows with a
specified character.

The :json formatter outputs valid JSON.

Examples:

  hbase> set_formatter :old_school
  hbase> set_formatter :aligned
  hbase> set_formatter :aligned, border: BORDER.FULL, overflow: OVERFLOW.TRUNCATE
  hbase> set_formatter :unaligned
  hbase> set_formatter :unaligned, padding: 0, row_separator: "\\n", cell_separator: ','
  hbase> set_formatter :json
        EOF
      end

      def command(kind, **kwargs)
        @shell.old_school = false
        case kind
        when :old_school
          @shell.old_school = true
          @shell.table_formatter = ::Shell::Formatter::AlignedTableFormatter.new(**kwargs)
        when :aligned then @shell.table_formatter = ::Shell::Formatter::AlignedTableFormatter.new(**kwargs)
        when :json then @shell.table_formatter = ::Shell::Formatter::JsonTableFormatter.new(**kwargs)
        when :unaligned then @shell.table_formatter = ::Shell::Formatter::UnalignedTableFormatter.new(**kwargs)
        else raise ArgumentError, 'unexpected kind of formatter'
        end
        nil
      end
    end
  end
end
