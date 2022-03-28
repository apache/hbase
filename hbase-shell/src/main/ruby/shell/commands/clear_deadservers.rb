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
    class ClearDeadservers < Command
      def help
        <<~EOF
          Clear the dead region servers that are never used. Returns an array containing any
          deadservers that could not be cleared.

          Examples:
          Clear all dead region servers:
          hbase> clear_deadservers
          Clear the specified dead region servers:
          hbase> clear_deadservers 'host187.example.com,60020,1289493121758'
          or
          hbase> clear_deadservers 'host187.example.com,60020,1289493121758',
                                   'host188.example.com,60020,1289493121758'
        EOF
      end

      # rubocop:disable Metrics/AbcSize
      def command(*dead_servers)
        servers = admin.clear_deadservers(dead_servers)
        if servers.size <= 0
          formatter.row(['true'])
          []
        else
          formatter.row(['Some dead server clear failed'])
          formatter.row(['SERVERNAME'])
          server_names = servers.map { |server| server.toString }
          server_names.each do |server|
            formatter.row([server])
          end
          formatter.footer(servers.size)
          server_names
        end
      end
      # rubocop:enable Metrics/AbcSize
      # rubocop:enable Metrics/MethodLength
    end
  end
end
