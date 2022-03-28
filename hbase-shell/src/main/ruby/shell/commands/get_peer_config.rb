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
    class GetPeerConfig < Command
      def help
        <<~EOF
          Outputs the cluster key, replication endpoint class (if present), and any replication configuration parameters
        EOF
      end

      def command(id)
        peer_config = replication_admin.get_peer_config(id)
        @start_time = Time.now
        format_peer_config(peer_config)
        peer_config
      end

      def format_peer_config(peer_config)
        cluster_key = peer_config.get_cluster_key
        endpoint = peer_config.get_replication_endpoint_impl

        formatter.row(['Cluster Key', cluster_key]) unless cluster_key.nil?
        formatter.row(['Replication Endpoint', endpoint]) unless endpoint.nil?
        unless peer_config.get_configuration.nil?
          peer_config.get_configuration.each do |config_entry|
            formatter.row(config_entry)
          end
        end
      end
    end
  end
end
