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
    class GetNamespaceRsgroup < Command
      def help
        <<-EOF
Get the group name the given NameSpace is a member of.

Example:

  hbase> get_namespace_rsgroup 'namespace_name'

EOF
      end

      def command(namespace_name)
        group_name = admin.get_namespace_rsgroup(namespace_name)
        unless group_name.nil?
          formatter.row([group_name])
        end
        formatter.footer(1)
      end
    end
  end
end