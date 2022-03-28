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
    class Normalize < Command
      def help
        <<-EOF
Trigger the region normalizer. Without arguments, invokes the normalizer without a table filter.
Any arguments are used to limit table selection. Returns true if the normalize request was
submitted successfully, false otherwise. Note that this command has no effect if region normalizer
is disabled (make sure it's turned on using 'normalizer_switch' command).

Examples:

  hbase> normalize
  hbase> normalize TABLE_NAME => 'my_table'
  hbase> normalize TABLE_NAMES => ['foo', 'bar', 'baz']
  hbase> normalize REGEX => 'my_.*'
  hbase> normalize NAMESPACE => 'ns1'
  hbase> normalize NAMESPACE => 'ns', REGEX => '*._BIG_.*'
EOF
      end

      def command(*args)
        did_normalize_run = !!admin.normalize(*args)
        formatter.row([did_normalize_run.to_s])
        did_normalize_run
      end
    end
  end
end
