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

module Shell
  module Commands
    class LocateRegion < Command
      def help
        <<-EOF
Locate the region given a table name and a row-key

  hbase> locate_region 'tableName', 'key0'
EOF
      end

      def command(table, row_key)
        region_location = admin.locate_region(table, row_key)
        hri = region_location.getRegionInfo

        formatter.header(%w[HOST REGION])
        formatter.row([region_location.getHostnamePort, hri.toString])
        formatter.footer(1)
        region_location
      end
    end
  end
end
