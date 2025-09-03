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

require 'hbase_shell'
require 'stringio'
require 'hbase_constants'
require 'hbase/hbase'
require 'hbase/table'


module Hbase
  class KeymetaAdminTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
    end

    define_test 'Test enable key management' do
      custAndNamespace = $CUST1_ENCODED + ':*'
      # Repeat the enable twice in a loop and ensure multiple enables succeed and return the same output.
      (0..1).each do |i|
        output = capture_stdout { @shell.command('enable_key_management', custAndNamespace) }
        puts "enable_key_management #{i} output: #{output}"
        assert(output.include?($CUST1_ENCODED +' * ACTIVE'))
      end
      output = capture_stdout { @shell.command('show_key_status', custAndNamespace) }
      puts "show_key_status output: #{output}"
      assert(output.include?($CUST1_ENCODED +' * ACTIVE'))

      # The ManagedKeyStoreKeyProvider doesn't support specific namespaces, so it will return the global key.
      custAndNamespace = $CUST1_ENCODED + ':' + 'test_table/f'
      output = capture_stdout { @shell.command('enable_key_management', custAndNamespace) }
      puts "enable_key_management output: #{output}"
      assert(output.include?($CUST1_ENCODED +' * ACTIVE'))
      output = capture_stdout { @shell.command('show_key_status', custAndNamespace) }
      puts "show_key_status output: #{output}"
      assert(output.include?('0 row(s)'))
    end
  end
end