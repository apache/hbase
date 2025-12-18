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
module Hbase
  # Test class for rotate_stk command
  class RotateSTKKeymetaTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
    end

    define_test 'Test rotate_stk command' do
      puts 'Testing rotate_stk command'

      # this should return false (no rotation performed)
      output = capture_stdout { @shell.command(:rotate_stk) }
      puts "rotate_stk output: #{output}"
      assert(output.include?('No System Key change was detected'),
             "Expected output to contain rotation status message, but got: #{output}")

      key_provider = Encryption.getManagedKeyProvider($TEST_CLUSTER.getConfiguration)
      # Once we enable multikeyGenMode on MockManagedKeyProvider, every call should return a new key
      # which should trigger a rotation.
      key_provider.setMultikeyGenMode(true)
      output = capture_stdout { @shell.command(:rotate_stk) }
      puts "rotate_stk output: #{output}"
      assert(output.include?('System Key rotation was performed successfully and cache was ' \
                             'refreshed on all region servers'),
             "Expected output to contain rotation status message, but got: #{output}")
    end
  end
end
