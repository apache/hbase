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
    # Recommission a region server, optionally load a list of passed regions
    class RecommissionRegionserver < Command
      def help
        <<~EOF
  Remove decommission marker from a region server to allow regions assignments.

  Optionally, load regions onto the server by passing a list of encoded region names.
  NOTE: Region loading is asynchronous.

  Examples:
    hbase> recommission_regionserver 'server'
    hbase> recommission_regionserver 'server,port'
    hbase> recommission_regionserver 'server,port,starttime'
    hbase> recommission_regionserver 'server,port,starttime', ['encoded_region_name1', 'encoded_region_name1']
EOF
      end

      def command(server_name, encoded_region_names = [])
        admin.recommission_regionserver(server_name, encoded_region_names)
      end
    end
  end
end
