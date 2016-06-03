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
    class ListPeerConfigs < Command
      def help
        return <<-EOF
          No-argument method that outputs the replication peer configuration for each peer defined on this cluster.
        EOF
      end

      def command
        peer_configs = replication_admin.list_peer_configs
        unless peer_configs.nil?
          peer_configs.each do |peer_config_entry|
            peer_id = peer_config_entry[0]
            peer_config = peer_config_entry[1]
            formatter.row(["PeerId", peer_id])
            GetPeerConfig.new(@shell).format_peer_config(peer_config)
            formatter.row([" "])
          end
        end
      end
    end
  end
end
