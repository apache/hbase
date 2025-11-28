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

java_import org.apache.hadoop.hbase.io.crypto.Encryption
java_import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider
java_import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider
java_import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException

module Hbase
  # Test class for keymeta admin functionality with MockManagedKeyProvider
  class KeymetaAdminMockProviderTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      @key_provider = Encryption.getManagedKeyProvider($TEST_CLUSTER.getConfiguration)
      # Enable multikey generation mode for dynamic key creation on rotate
      @key_provider.setMultikeyGenMode(true)

      # Set up custodian variables
      @glob_cust = '*'
      @glob_cust_encoded = ManagedKeyProvider.encodeToStr(@glob_cust.bytes.to_a)
    end

    define_test 'Test rotate managed key operation' do
      test_rotate_key(@glob_cust_encoded, '*')
      test_rotate_key(@glob_cust_encoded, 'test_namespace')
    end

    def test_rotate_key(cust, namespace)
      cust_and_namespace = "#{cust}:#{namespace}"
      puts "Testing rotate_managed_key for #{cust_and_namespace}"

      # 1. Enable key management first
      output = capture_stdout { @shell.command('enable_key_management', cust_and_namespace) }
      puts "enable_key_management output: #{output}"
      assert(output.include?("#{cust} #{namespace} ACTIVE"),
             "Expected ACTIVE key after enable, got: #{output}")

      # Verify initial state - should have 1 ACTIVE key
      output = capture_stdout { @shell.command('show_key_status', cust_and_namespace) }
      puts "show_key_status before rotation: #{output}"
      assert(output.include?('1 row(s)'), "Expected 1 key before rotation, got: #{output}")

      # 2. Rotate the managed key (mock provider will generate a new key due to multikeyGenMode)
      output = capture_stdout { @shell.command('rotate_managed_key', cust_and_namespace) }
      puts "rotate_managed_key output: #{output}"
      assert(output.include?("#{cust} #{namespace}"),
             "Expected key info in rotation output, got: #{output}")

      # 3. Verify we now have both ACTIVE and INACTIVE keys
      output = capture_stdout { @shell.command('show_key_status', cust_and_namespace) }
      puts "show_key_status after rotation: #{output}"
      assert(output.include?('ACTIVE'),
             "Expected ACTIVE key after rotation, got: #{output}")
      assert(output.include?('INACTIVE'),
             "Expected INACTIVE key after rotation, got: #{output}")
      assert(output.include?('2 row(s)'),
             "Expected 2 keys after rotation, got: #{output}")

      # 4. Rotate again to test multiple rotations
      output = capture_stdout { @shell.command('rotate_managed_key', cust_and_namespace) }
      puts "rotate_managed_key (second) output: #{output}"
      assert(output.include?("#{cust} #{namespace}"),
             "Expected key info in second rotation output, got: #{output}")

      # Should now have 3 keys: 1 ACTIVE, 2 INACTIVE
      output = capture_stdout { @shell.command('show_key_status', cust_and_namespace) }
      puts "show_key_status after second rotation: #{output}"
      assert(output.include?('3 row(s)'),
             "Expected 3 keys after second rotation, got: #{output}")

      # Cleanup - disable all keys
      @shell.command('disable_key_management', cust_and_namespace)
    end

    define_test 'Test rotate without active key fails' do
      cust_and_namespace = "#{@glob_cust_encoded}:nonexistent_namespace"
      puts "Testing rotate_managed_key on non-existent namespace"

      # Attempt to rotate when no key management is enabled should fail
      e = assert_raises(RemoteWithExtrasException) do
        @shell.command('rotate_managed_key', cust_and_namespace)
      end
      assert_true(e.is_do_not_retry)
    end

    define_test 'Test refresh managed keys with mock provider' do
      cust_and_namespace = "#{@glob_cust_encoded}:test_refresh"
      puts "Testing refresh_managed_keys for #{cust_and_namespace}"

      # 1. Enable key management
      output = capture_stdout { @shell.command('enable_key_management', cust_and_namespace) }
      puts "enable_key_management output: #{output}"
      assert(output.include?("#{@glob_cust_encoded} test_refresh ACTIVE"))

      # 2. Rotate to create multiple keys
      output = capture_stdout { @shell.command('rotate_managed_key', cust_and_namespace) }
      puts "rotate_managed_key output: #{output}"
      assert(output.include?("#{@glob_cust_encoded} test_refresh"),
             "Expected key info in rotation output, got: #{output}")

      # 3. Refresh managed keys - should succeed without changing state
      output = capture_stdout { @shell.command('refresh_managed_keys', cust_and_namespace) }
      puts "refresh_managed_keys output: #{output}"
      assert(output.include?('Managed keys refreshed successfully'),
             "Expected success message, got: #{output}")

      # Verify keys still exist after refresh
      output = capture_stdout { @shell.command('show_key_status', cust_and_namespace) }
      assert(output.include?('ACTIVE'), "Expected ACTIVE key after refresh")
      assert(output.include?('INACTIVE'), "Expected INACTIVE key after refresh")

      # Cleanup
      @shell.command('disable_key_management', cust_and_namespace)
    end
  end
end

