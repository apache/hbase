#
# Copyright The Apache Software Foundation
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
require 'shell/formatter/util'

module Shell
  module Formatter
    ##
    # Mixin providing helper methods for snapshot commands
    module SnapshotMixin
      ##
      # Print snapshots in a table
      #
      # Intended for reuse by other snapshot commands such as list_table_snapshots
      def print_snapshot_table(snapshots)
        if @shell.old_school
          formatter.header(['SNAPSHOT', 'TABLE + CREATION TIME'])
          snapshots.each do |snapshot|
            creation_time = Time.at(snapshot.getCreationTime / 1000).to_s
            formatter.row([snapshot.getName, snapshot.getTableNameAsString + ' (' + creation_time + ')'])
            formatter.footer
          end
          formatter.footer(snapshots.size)
        else
          table_formatter.start_table(
              headers: %w[SNAPSHOT TABLE CREATION_TIME],
              widths: [nil, nil, ::Shell::Formatter::Util::ISO8601_WIDTH]
          )
          snapshots.each do |snapshot|
            creation_time = ::Shell::Formatter::Util.to_iso_8601 snapshot.getCreationTime
            table_formatter.row([snapshot.getName, snapshot.getTableNameAsString, creation_time])
          end
          table_formatter.close_table(num_rows: snapshots.size)
        end

        nil
      end
    end
  end
end
