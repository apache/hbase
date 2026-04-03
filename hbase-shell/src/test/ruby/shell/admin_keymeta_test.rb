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
      assert_match(/Failed to decode Base64 encoded string '!!!'/, error.message)
    end

    define_test 'Test key management operations without rotation' do
      test_key_operations($CUST1_ENCODED, '*')
      test_key_operations($CUST1_ENCODED, 'test_namespace')
      test_key_operations($GLOB_CUST_ENCODED, '*')
    end

    def test_key_operations(cust, namespace)
      cust_and_namespace = "#{cust}:#{namespace}"
      puts "Testing key management operations for #{cust_and_namespace}"

      # 1. Enable key management
      output = capture_stdout { @shell.command('enable_key_management', cust_and_namespace) }
      puts "enable_key_management output: #{output}"
      assert(output.include?("#{cust} #{namespace} ACTIVE"),
             "Expected ACTIVE key after enable, got: #{output}")

      # 2. Get the initial KEY-IDENTITY (partial identity encoded) for use in disable_managed_key test
      output = capture_stdout { @shell.command('show_key_status', cust_and_namespace) }
      puts "show_key_status output: #{output}"
      # Output format: ENCODED-KEY NAMESPACE STATUS KEY-IDENTITY REFRESH-TIMESTAMP
      lines = output.split("\n")
      key_line = lines.find { |line| line.include?(cust) && line.include?(namespace) }
      assert_not_nil(key_line, "Could not find key line in output")
      # KEY-IDENTITY is the 4th column (index 3)
      key_identity_encoded = key_line.split[3]
      assert_not_nil(key_identity_encoded, "Could not extract KEY-IDENTITY column value")
      puts "Extracted KEY-IDENTITY (partial identity encoded): #{key_identity_encoded}"

      # 3. Refresh managed keys
      output = capture_stdout { @shell.command('refresh_managed_keys', cust_and_namespace) }
      puts "refresh_managed_keys output: #{output}"
      assert(output.include?('Managed keys refreshed successfully'),
             "Expected success message, got: #{output}")
      # Verify keys still exist after refresh
      output = capture_stdout { @shell.command('show_key_status', cust_and_namespace) }
      puts "show_key_status after refresh: #{output}"
      assert(output.include?('ACTIVE'), "Expected ACTIVE key after refresh, got: #{output}")

      # 4. Disable a specific managed key (use KEY-IDENTITY column value)
      output = capture_stdout do
        @shell.command('disable_managed_key', cust_and_namespace, key_identity_encoded)
      end
      puts "disable_managed_key output: #{output}"
      assert(output.include?("#{cust} #{namespace} DISABLED"),
             "Expected INACTIVE key, got: #{output}")
      # Verify the key is now INACTIVE
      output = capture_stdout { @shell.command('show_key_status', cust_and_namespace) }
      puts "show_key_status after disable_managed_key: #{output}"
      assert(output.include?('DISABLED'), "Expected DISABLED state, got: #{output}")

      # 5. Re-enable key management for next step
      @shell.command('enable_key_management', cust_and_namespace)

      # 6. Disable all key management
      output = capture_stdout { @shell.command('disable_key_management', cust_and_namespace) }
      puts "disable_key_management output: #{output}"
      assert(output.include?("#{cust} #{namespace} DISABLED"),
             "Expected DISABLED marker, got: #{output}")
      # Verify all keys are now DISABLED
      output = capture_stdout { @shell.command('show_key_status', cust_and_namespace) }
      puts "show_key_status after disable_key_management: #{output}"
      # All rows should show DISABLED state
      lines = output.split("\n")
      key_lines = lines.select { |line| line.include?(cust) && line.include?(namespace) }
      key_lines.each do |line|
        assert(
          (line.include?('DISABLED') || line.include?('INACTIVE')) &&
          !line.match?(/\bACTIVE\b/) &&
          !line.include?('FAILED'),
          "Expected all keys to be INACTIVE or DISABLED, but found: #{line}"
        )
      end

      # 7. Refresh shouldn't do anything since the key management is disabled.
      output = capture_stdout do
        @shell.command('refresh_managed_keys', cust_and_namespace)
      end
      puts "refresh_managed_keys output: #{output}"
      output = capture_stdout { @shell.command('show_key_status', cust_and_namespace) }
      puts "show_key_status after refresh_managed_keys: #{output}"
      assert(!output.include?(' ACTIVE '), "Expected all keys to be INACTIVE, but found: #{output}")

      # 7. Enable key management again
      @shell.command('enable_key_management', cust_and_namespace)

      # 8. Get the KEY-IDENTITY for the enabled key
      output = capture_stdout { @shell.command('show_key_status', cust_and_namespace) }
      puts "show_key_status after enable_key_management: #{output}"
      assert(output.include?('ACTIVE'), "Expected ACTIVE key after enable_key_management, got: #{output}")
      assert(output.include?('1 row(s)'))
    end

    define_test 'Test refresh error handling' do
      # Test refresh on non-existent key management (should not fail, just no-op)
      cust_and_namespace = "#{$CUST1_ENCODED}:nonexistent_namespace"
      output = capture_stdout do
        @shell.command('refresh_managed_keys', cust_and_namespace)
      end
      puts "refresh_managed_keys on non-existent namespace: #{output}"
      assert(output.include?('Managed keys refreshed successfully'),
             "Expected success message even for non-existent namespace, got: #{output}")
    end

    define_test 'Test disable operations error handling' do
      # Test disable_managed_key with invalid KEY-IDENTITY (partial identity encoded)
      cust_and_namespace = "#{$CUST1_ENCODED}:*"
      error = assert_raises(ArgumentError) do
        @shell.command('disable_managed_key', cust_and_namespace, '!!!invalid!!!')
      end
      assert_match(/Failed to decode Base64 encoded string '!!!invalid!!!'/, error.message)

      # Test disable_key_management on non-existent namespace (should succeed, no-op)
      cust_and_namespace = "#{$CUST1_ENCODED}:nonexistent_for_disable"
      output = capture_stdout { @shell.command('disable_key_management', cust_and_namespace) }
      puts "disable_key_management on non-existent namespace: #{output}"
      # Should show 0 rows since no keys exist
      assert(output.include?('1 row(s)'))
      assert(output.include?(" DISABLED "), "Expected DISABLED key, got: #{output}")
    end
  end
end
