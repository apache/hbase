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
        List all regions for a particular table as an array and also filter them by server name (optional) as prefix.
        By default, it will return all the regions for the table

        Examples:
        hbase> list_regions 'table_name'
        hbase> list_regions 'table_name', 'server_name'

EOF
        return
      end

      def command(table_name, region_server_name = "")
        admin_instance = admin.instance_variable_get("@admin")
        conn_instance = admin_instance.getConnection()
        cluster_status = admin_instance.getClusterStatus()
        hregion_locator_instance = conn_instance.getRegionLocator(TableName.valueOf(table_name))
        hregion_locator_list = hregion_locator_instance.getAllRegionLocations()
        results = Array.new

        begin
          hregion_locator_list.each do |hregion|
            hregion_info = hregion.getRegionInfo()
            server_name = hregion.getServerName()
            if hregion.getServerName().toString.start_with? region_server_name
              startKey = Bytes.toString(hregion.getRegionInfo().getStartKey())
              endKey = Bytes.toString(hregion.getRegionInfo().getEndKey())
              region_load_map = cluster_status.getLoad(server_name).getRegionsLoad()
              region_load = region_load_map.get(hregion_info.getRegionName())
              region_store_file_size = region_load.getStorefileSizeMB()
              region_requests = region_load.getRequestsCount()
              results << { "server" => hregion.getServerName().toString(), "name" => hregion_info.getRegionNameAsString(), "startkey" => startKey, "endkey" => endKey, "size" => region_store_file_size, "requests" => region_requests }
            end
          end
        ensure
          hregion_locator_instance.close()
        end

        @end_time = Time.now

        printf("%-60s | %-60s | %-15s | %-15s | %-20s | %-20s", "SERVER_NAME", "REGION_NAME", "START_KEY", "END_KEY", "SIZE", "REQ");
        printf("\n")
        for result in results
          printf("%-60s | %-60s | %-15s | %-15s | %-20s | %-20s", result["server"], result["name"], result["startkey"], result["endkey"], result["size"], result["requests"]);
            printf("\n")
        end
        printf("%d rows", results.size)

      end
    end
  end
end
