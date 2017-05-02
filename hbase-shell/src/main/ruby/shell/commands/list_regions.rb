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
    class ListRegions < Command
      def help

        return<<EOF
        List all regions for a particular table as an array and also filter them by server name (optional) as prefix
        and maximum locality (optional). By default, it will return all the regions for the table with any locality.

        Examples:
        hbase> list_regions 'table_name'
        hbase> list_regions 'table_name', 'server_name'
        hbase> list_regions 'table_name', {SERVER_NAME => 'server_name', LOCALITY_THRESHOLD => 0.8}

EOF
        return
      end

      def command(table_name, options = nil)
        if options.nil?
          options = {}
        elsif not options.is_a? Hash
          # When options isn't a hash, assume it's the server name
          # and create the hash internally
          options = {SERVER_NAME => options}
        end
        admin_instance = admin.instance_variable_get("@admin")
        conn_instance = admin_instance.getConnection()
        cluster_status = admin_instance.getClusterStatus()
        hregion_locator_instance = conn_instance.getRegionLocator(TableName.valueOf(table_name))
        hregion_locator_list = hregion_locator_instance.getAllRegionLocations().to_a
        results = Array.new
        desired_server_name = options[SERVER_NAME]

        begin
          # Filter out region servers which we don't want, default to all RS
          regions = get_regions_for_table_and_server(table_name, conn_instance, desired_server_name)
          # A locality threshold of "1.0" would be all regions (cannot have greater than 1 locality)
          # Regions which have a `dataLocality` less-than-or-equal to this value are accepted
          locality_threshold = 1.0
          if options.has_key? LOCALITY_THRESHOLD
            value = options[LOCALITY_THRESHOLD]
            # Value validation. Must be a Float, and must be between [0, 1.0]
            raise "#{LOCALITY_THRESHOLD} must be a float value" unless value.is_a? Float
            raise "#{LOCALITY_THRESHOLD} must be between 0 and 1.0, inclusive" unless valid_locality_threshold? value
            locality_threshold = value
          end
          regions.each do |hregion|
            hregion_info = hregion.getRegionInfo()
            server_name = hregion.getServerName()
            region_load_map = cluster_status.getLoad(server_name).getRegionsLoad()
            region_load = region_load_map.get(hregion_info.getRegionName())
            # Ignore regions which exceed our locality threshold
            if accept_region_for_locality? region_load.getDataLocality(), locality_threshold
              startKey = Bytes.toString(hregion_info.getStartKey())
              endKey = Bytes.toString(hregion_info.getEndKey())
              region_store_file_size = region_load.getStorefileSizeMB()
              region_requests = region_load.getRequestsCount()
              results << { "server" => hregion.getServerName().toString(), "name" => hregion_info.getRegionNameAsString(), "startkey" => startKey, "endkey" => endKey,
                 "size" => region_store_file_size, "requests" => region_requests, "locality" => region_load.getDataLocality() }
            end
          end
        ensure
          hregion_locator_instance.close()
        end

        @end_time = Time.now

        printf("%-60s | %-60s | %-15s | %-15s | %-20s | %-20s | %-20s", "SERVER_NAME", "REGION_NAME", "START_KEY", "END_KEY", "SIZE", "REQ", "LOCALITY");
        printf("\n")
        for result in results
          printf("%-60s | %-60s | %-15s | %-15s | %-20s | %-20s | %-20s", result["server"], result["name"], result["startkey"], result["endkey"], result["size"], result["requests"], result['locality']);
            printf("\n")
        end
        printf("%d rows", results.size)

      end

      def valid_locality_threshold?(value)
        value >= 0 and value <= 1.0
      end

      def get_regions_for_table_and_server(table_name, conn, server_name)
        get_regions_for_server(get_regions_for_table(table_name, conn), server_name)
      end

      def get_regions_for_server(regions_for_table, server_name)
        regions_for_table.select do |hregion|
          accept_server_name? server_name, hregion.getServerName().toString()
        end
      end

      def get_regions_for_table(table_name, conn)
        conn.getRegionLocator(TableName.valueOf(table_name)).getAllRegionLocations().to_a
      end

      def accept_server_name?(desired_server_name, actual_server_name)
        desired_server_name.nil? or actual_server_name.start_with? desired_server_name
      end

      def accept_region_for_locality?(actual_locality, locality_threshold)
        actual_locality <= locality_threshold
      end
    end
  end
end
