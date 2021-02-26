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
    class Flush < Command
      def help
        <<-EOF
Flush all regions in passed table or pass a region row to
flush an individual region or a region server name whose format
is 'host,port,startcode', to flush all its regions.
You can also flush a single column family for all regions within a table,
or for an specific region only.
For example:

  hbase> flush 'TABLENAME'
  hbase> flush 'TABLENAME','FAMILYNAME'
  hbase> flush 'REGIONNAME'
  hbase> flush 'REGIONNAME','FAMILYNAME'
  hbase> flush 'ENCODED_REGIONNAME'
  hbase> flush 'ENCODED_REGIONNAME','FAMILYNAME'
  hbase> flush 'REGION_SERVER_NAME'
EOF
      end

      def command(table_or_region_name, family = nil)
        admin.flush(table_or_region_name, family)
      end
    end
  end
end
