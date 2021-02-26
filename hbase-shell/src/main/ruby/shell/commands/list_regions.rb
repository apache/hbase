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
        <<~EOF
        List all regions for a particular table as an array and also filter them by server name (optional) as prefix
        and maximum locality (optional). By default, it will return all the regions for the table with any locality.
        The command displays server name, region name, start key, end key, size of the region in MB, number of requests
        and the locality. The information can be projected out via an array as third parameter. By default all these information
        is displayed. Possible array values are SERVER_NAME, REGION_NAME, START_KEY, END_KEY, SIZE, REQ and LOCALITY. Values
        are not case sensitive. If you don't want to filter by server name, pass an empty hash / string as shown below.

        Examples:
        hbase> list_regions 'table_name'
        hbase> list_regions 'table_name', 'server_name'
        hbase> list_regions 'table_name', {SERVER_NAME => 'server_name', LOCALITY_THRESHOLD => 0.8}
        hbase> list_regions 'table_name', {SERVER_NAME => 'server_name', LOCALITY_THRESHOLD => 0.8}, ['SERVER_NAME']
        hbase> list_regions 'table_name', {}, ['SERVER_NAME', 'start_key']
        hbase> list_regions 'table_name', '', ['SERVER_NAME', 'start_key']

        EOF
      end

      def command(table_name, options = nil, cols = nil)
        if options.nil?
          options = {}
        elsif !options.is_a? Hash
          # When options isn't a hash, assume it's the server name
          # and create the hash internally
          options = { ::HBaseConstants::SERVER_NAME => options }
        end

        raise "Table #{table_name} must be enabled." unless admin.enabled?(table_name)

        size_hash = {}
        if cols.nil?
          size_hash = { 'SERVER_NAME' => 12, 'REGION_NAME' => 12, 'START_KEY' => 10, 'END_KEY' => 10, 'SIZE' => 5, 'REQ' => 5, 'LOCALITY' => 10 }
        elsif cols.is_a?(Array)
          cols.each do |col|
            if col.casecmp('SERVER_NAME').zero?
              size_hash.store('SERVER_NAME', 12)
            elsif col.casecmp('REGION_NAME').zero?
              size_hash.store('REGION_NAME', 12)
            elsif col.casecmp('START_KEY').zero?
              size_hash.store('START_KEY', 10)
            elsif col.casecmp('END_KEY').zero?
              size_hash.store('END_KEY', 10)
            elsif col.casecmp('SIZE').zero?
              size_hash.store('SIZE', 5)
            elsif col.casecmp('REQ').zero?
              size_hash.store('REQ', 5)
            elsif col.casecmp('LOCALITY').zero?
              size_hash.store('LOCALITY', 10)
            else
              raise "#{col} is not a valid column. Possible values are SERVER_NAME, REGION_NAME, START_KEY, END_KEY, SIZE, REQ, LOCALITY."
            end
          end
        else
          raise "#{cols} must be an array of strings. Possible values are SERVER_NAME, REGION_NAME, START_KEY, END_KEY, SIZE, REQ, LOCALITY."
        end

        admin_instance = admin.instance_variable_get('@admin')
        conn_instance = admin_instance.getConnection
        cluster_status = org.apache.hadoop.hbase.ClusterStatus.new(admin_instance.getClusterMetrics)
        hregion_locator_instance = conn_instance.getRegionLocator(TableName.valueOf(table_name))
        hregion_locator_list = hregion_locator_instance.getAllRegionLocations.to_a
        results = []
        desired_server_name = options[::HBaseConstants::SERVER_NAME]

        begin
          # Filter out region servers which we don't want, default to all RS
          regions = get_regions_for_table_and_server(table_name, conn_instance, desired_server_name)
          # A locality threshold of "1.0" would be all regions (cannot have greater than 1 locality)
          # Regions which have a `dataLocality` less-than-or-equal to this value are accepted
          locality_threshold = 1.0
          if options.key? ::HBaseConstants::LOCALITY_THRESHOLD
            value = options[::HBaseConstants::LOCALITY_THRESHOLD]
            # Value validation. Must be a Float, and must be between [0, 1.0]
            unless value.is_a? Float
              raise "#{::HBaseConstants::LOCALITY_THRESHOLD} must be a float value"
            end
            unless valid_locality_threshold? value
              raise "#{::HBaseConstants::LOCALITY_THRESHOLD} must be between 0 and 1.0, inclusive"
            end

            locality_threshold = value
          end

          regions.each do |hregion|
            hregion_info = hregion.getRegion
            server_name = hregion.getServerName
            server_load = cluster_status.getLoad(server_name)
            if server_load.nil?
              region_load_map = java.util.HashMap.new
            else
              region_load_map = server_load.getRegionsLoad
            end
            region_name = hregion_info.getRegionNameAsString
            region_load = region_load_map.get(hregion_info.getRegionName)

            if region_load.nil?
              puts "Can not find all details for region: " \
                   "#{region_name.strip} ," \
                   " it may be disabled or in transition\n"
            else
              # Ignore regions which exceed our locality threshold
              next unless accept_region_for_locality? region_load.getDataLocality,
                                                      locality_threshold
            end
            result_hash = {}

            if size_hash.key?('SERVER_NAME')
              result_hash.store('SERVER_NAME', server_name.toString.strip)
              size_hash['SERVER_NAME'] = [size_hash['SERVER_NAME'], server_name.toString.strip.length].max
            end

            if size_hash.key?('REGION_NAME')
              result_hash.store('REGION_NAME', region_name.strip)
              size_hash['REGION_NAME'] = [size_hash['REGION_NAME'], region_name.length].max
            end

            if size_hash.key?('START_KEY')
              start_key = Bytes.toStringBinary(hregion_info.getStartKey).strip
              result_hash.store('START_KEY', start_key)
              size_hash['START_KEY'] = [size_hash['START_KEY'], start_key.length].max
            end

            if size_hash.key?('END_KEY')
              end_key = Bytes.toStringBinary(hregion_info.getEndKey).strip
              result_hash.store('END_KEY', end_key)
              size_hash['END_KEY'] = [size_hash['END_KEY'], end_key.length].max
            end

            if size_hash.key?('SIZE')
              if region_load.nil?
                region_store_file_size = ''
              else
                region_store_file_size = region_load.getStorefileSizeMB.to_s.strip
              end
              result_hash.store('SIZE', region_store_file_size)
              size_hash['SIZE'] = [size_hash['SIZE'], region_store_file_size.length].max
            end

            if size_hash.key?('REQ')
              if region_load.nil?
                region_requests = ''
              else
                region_requests = region_load.getRequestsCount.to_s.strip
              end
              result_hash.store('REQ', region_requests)
              size_hash['REQ'] = [size_hash['REQ'], region_requests.length].max
            end

            if size_hash.key?('LOCALITY')
              if region_load.nil?
                locality = ''
              else
                locality = region_load.getDataLocality.to_s.strip
              end
              result_hash.store('LOCALITY', locality)
              size_hash['LOCALITY'] = [size_hash['LOCALITY'], locality.length].max
            end

            results << result_hash
          end
        ensure
          hregion_locator_instance.close
        end

        @end_time = Time.now

        size_hash.each do |param, length|
          printf(" %#{length}s |", param)
        end
        printf("\n")

        size_hash.each_value do |length|
          str = '-' * length
          printf(" %#{length}s |", str)
        end
        printf("\n")

        results.each do |result|
          size_hash.each do |param, length|
            printf(" %#{length}s |", result[param])
          end
          printf("\n")
        end

        printf(" %d rows\n", results.size)
      end

      def valid_locality_threshold?(value)
        value >= 0 && value <= 1.0
      end

      def get_regions_for_table_and_server(table_name, conn, server_name)
        get_regions_for_server(get_regions_for_table(table_name, conn), server_name)
      end

      def get_regions_for_server(regions_for_table, server_name)
        regions_for_table.select do |hregion|
          actual_server_name = hregion.getServerName
          if actual_server_name == nil
            raise "Some regions might be splitting or merging or transitioning due to other" \
            " reasons"
          end
          accept_server_name? server_name, actual_server_name.toString
        end
      end

      def get_regions_for_table(table_name, conn)
        conn.getRegionLocator(TableName.valueOf(table_name)).getAllRegionLocations.to_a
      end

      def accept_server_name?(desired_server_name, actual_server_name)
        desired_server_name.nil? || actual_server_name.start_with?(desired_server_name)
      end

      def accept_region_for_locality?(actual_locality, locality_threshold)
        actual_locality <= locality_threshold
      end
    end
  end
end
