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
    # Reassign tables of specified namespaces
    # from one RegionServer group to another.
    class MoveNamespacesRsgroup < Command
      def help
        <<~CMD

  Example:
  hbase> move_namespaces_rsgroup 'dest',['ns1','ns2']

CMD
      end

      def command(dest, namespaces)
        rsgroup_admin.move_namespaces(dest, namespaces)
        namespaces.each do |ns|
          arg = {'METHOD' => 'set', 'hbase.rsgroup.name' => dest}
          admin.alter_namespace(ns, arg)
        end
      end
    end
  end
end
