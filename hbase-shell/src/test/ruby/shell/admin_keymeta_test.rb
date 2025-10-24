# frozen_string_literal: true

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
  # Test class for keymeta admin functionality
  class KeymetaAdminTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
    end

    define_test 'Test enable key management' do
      test_key_management($CUST1_ENCODED, '*')
      test_key_management($CUST1_ENCODED, 'test_table/f')
      test_key_management($CUST1_ENCODED, 'test_namespace')
      test_key_management($GLOB_CUST_ENCODED, '*')

      puts 'Testing that cluster can be restarted when key management is enabled'
      $TEST.restartMiniCluster
      puts 'Cluster restarted, testing key management again'
      setup_hbase
      test_key_management($GLOB_CUST_ENCODED, '*')
      puts 'Key management test complete'
    end

    def test_key_management(cust, namespace)
      # Repeat the enable twice in a loop and ensure multiple enables succeed and return the
      # same output.
      2.times do
        cust_and_namespace = "#{cust}:#{namespace}"
        output = capture_stdout { @shell.command('enable_key_management', cust_and_namespace) }
        puts "enable_key_management output: #{output}"
        assert(output.include?("#{cust} #{namespace} ACTIVE"))
        output = capture_stdout { @shell.command('show_key_status', cust_and_namespace) }
        puts "show_key_status output: #{output}"
        assert(output.include?("#{cust} #{namespace} ACTIVE"))
        assert(output.include?('1 row(s)'))
      end
    end

    define_test 'Decode failure raises friendly error' do
      assert_raises(ArgumentError) do
        @shell.command('enable_key_management', '!!!:namespace')
      end

      error = assert_raises(ArgumentError) do
        @shell.command('show_key_status', '!!!:namespace')
      end
      assert_match(/Failed to decode key custodian/, error.message)
    end
  end
end
