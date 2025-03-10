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
    class PbeEnable < Command
      def help
        <<-EOF
Enable PBE for a given prefix:namespace (prefix in Base64 format).
If no namespace is specified, the global namespace (*) is used.
EOF
      end

      def command(pbe_prefix)
        formatter.header(['KEY', 'STATUS'])
        status = pbe_admin.pbe_enable(pbe_prefix)
        formatter.row([pbe_prefix, status.toString()])
      end
    end
  end
end
