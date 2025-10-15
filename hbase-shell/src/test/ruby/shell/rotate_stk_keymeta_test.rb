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
  # Test class for rotate_stk command
  class RotateSTKKeymetaTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
    end

    define_test 'Test rotate_stk command' do
      puts 'Testing rotate_stk command'

      # Call rotate_stk - since no actual system key change has occurred,
      # this should return false (no rotation performed)
      output = capture_stdout { @shell.command('rotate_stk') }
      puts "rotate_stk output: #{output}"

      # Verify the method executes successfully
      # In a real deployment with actual key changes, this would return true
      # if a new system key was detected and successfully propagated to all region servers
      assert(output.include?('No System Key change detected') ||
             output.include?('System Key rotation initiated successfully'),
             "Expected output to contain rotation status message, but got: #{output}")

      # Call it again to ensure idempotency
      output2 = capture_stdout { @shell.command('rotate_stk') }
      puts "rotate_stk second call output: #{output2}"
      assert(output2.include?('No System Key change detected') ||
             output2.include?('System Key rotation initiated successfully'),
             "Expected output to contain rotation status message on second call, but got: #{output2}")

      puts 'rotate_stk command test complete'
    end

    define_test 'Test rotate_stk after cluster restart' do
      puts 'Testing rotate_stk after cluster restart'

      # First call before restart
      output = capture_stdout { @shell.command('rotate_stk') }
      puts "rotate_stk output before restart: #{output}"

      # Restart the cluster
      puts 'Restarting cluster...'
      $TEST.restartMiniCluster
      puts 'Cluster restarted'

      # Reinitialize shell after restart
      setup_hbase

      # Call rotate_stk again after restart
      output = capture_stdout { @shell.command('rotate_stk') }
      puts "rotate_stk output after restart: #{output}"

      # Verify the method still executes successfully after restart
      assert(output.include?('No System Key change detected') ||
             output.include?('System Key rotation initiated successfully'),
             "Expected output to contain rotation status message after restart, but got: #{output}")

      puts 'rotate_stk after cluster restart test complete'
    end
  end
end

