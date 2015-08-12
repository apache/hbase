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
    class ListSecurityCapabilities < Command
      def help
        return <<-EOF
List supported security capabilities

Example:
    hbase> list_security_capabilities
EOF
      end

      def command()
        begin
          list = admin.get_security_capabilities
          list.each do |s|
            puts s.getName
          end
          return list.map { |s| s.getName() }
        rescue Exception => e
          if e.to_s.include? "UnsupportedOperationException"
            puts "ERROR: Master does not support getSecurityCapabilities"
            return []
          end
          raise e
        end
      end
    end
  end
end
