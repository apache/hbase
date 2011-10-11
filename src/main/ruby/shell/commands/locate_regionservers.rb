#
# Copyright 2011 The Apache Software Foundation
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

java_import org.apache.hadoop.hbase.client.HTable
java_import org.apache.hadoop.hbase.util.Bytes

require 'set'

module Shell
  module Commands
    class LocateRegionservers < Command
      def help
        return <<-EOF
Lists the regionservers hosting the table along with their start and end keys.
For example:
  hbase> locate_regionservers 't1'
If provided with a row key, it lists the regionserver hosting that key.
For example:
  hbase> locate_regionservers 't1', 'row_key'
EOF
      end

      def command(table, key=nil)
        now = Time.now

        htable = HTable.new(table)
        if key != nil then
          regionLocation = htable.getRegionLocation(key).serverAddress.toString
          formatter.row([regionLocation])
        else
          regionsInfo = htable.getRegionsInfo
          hosting_regionservers = Array.new

          regionsInfo.each do |hregioninfo, hserveraddress|
            array = Array.new
            address_name = hserveraddress.toString
            start_key = Bytes::toStringBinary(hregioninfo.getStartKey)
            end_key = Bytes::toStringBinary(hregioninfo.getEndKey)
            array = "#{address_name}\t#{start_key}\t#{end_key}"
            hosting_regionservers << array
          end

          hosting_regionservers.each do |regionserver|
            formatter.row([regionserver])
          end
        end

        formatter.footer(now)
      end
    end
  end
end
