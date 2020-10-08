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
  # rubocop:disable Metrics/ClassLength
  class SpaceQuotasTest < Test::Unit::TestCase
    include TestHelpers
    include HBaseConstants
    include HBaseQuotasConstants

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_quota_tests_table"
      create_test_table(@test_name)
    end

    def teardown
      shutdown
    end

    define_test 'limit_space errors on non Hash argument' do
      qa = quotas_admin()
      assert_raise(ArgumentError) do
        qa.limit_space('foo')
      end
      assert_raise(ArgumentError) do
        qa.limit_space()
      end
    end

    define_test 'remove_space_limit errors on non Hash argument' do
      qa = quotas_admin()
      assert_raise(ArgumentError) do
        qa.remove_space_limit('foo')
      end
      assert_raise(ArgumentError) do
        qa.remove_space_limit()
      end
    end

    # rubocop:disable Metrics/BlockLength
    define_test 'set quota with an invalid limit fails' do
      # Space Quota
      assert_raise(ArgumentError) do
        command(:set_quota,
                TYPE => SPACE,
                LIMIT => 'asdf',
                POLICY => NO_INSERTS,
                TABLE => @test_name)
      end
      assert_raise(ArgumentError) do
        command(:set_quota,
                TYPE => SPACE,
                LIMIT => '1.3G',
                POLICY => NO_INSERTS,
                TABLE => @test_name)
      end
      assert_raise(ArgumentError) do
        command(:set_quota,
                TYPE => SPACE,
                LIMIT => 'G1G',
                POLICY => NO_INSERTS,
                TABLE => @test_name)
      end
      assert_raise(ArgumentError) do
        command(:set_quota,
                TYPE => SPACE,
                LIMIT => '1GG',
                POLICY => NO_INSERTS,
                TABLE => @test_name)
      end
      assert_raise(ArgumentError) do
        command(:set_quota,
                TYPE => SPACE,
                LIMIT => '1H',
                POLICY => NO_INSERTS,
                TABLE => @test_name)
      end
      assert_raise(ArgumentError) do
        command(:set_quota,
                TYPE => SPACE,
                LIMIT => '0G',
                POLICY => NO_INSERTS,
                TABLE => @test_name)
      end

      # Throttle Quota
      assert_raise(ArgumentError) do
        command(:set_quota,
                TYPE => THROTTLE,
                LIMIT => 'asdf',
                TABLE => @test_name)
      end
      assert_raise(ArgumentError) do
        command(:set_quota,
                TYPE => THROTTLE,
                LIMIT => '1.3G/hour',
                TABLE => @test_name)
      end
      assert_raise(ArgumentError) do
        command(:set_quota,
                TYPE => THROTTLE,
                LIMIT => 'G1G/hour',
                TABLE => @test_name)
      end
      assert_raise(ArgumentError) do
        command(:set_quota,
                TYPE => THROTTLE,
                LIMIT => '1GG/hour',
                TABLE => @test_name)
      end
      assert_raise(ArgumentError) do
        command(:set_quota,
                TYPE => THROTTLE,
                LIMIT => '1H/hour',
                TABLE => @test_name)
      end
      assert_raise(ArgumentError) do
        command(:set_quota,
                TYPE => THROTTLE,
                LIMIT => '0G/hour',
                TABLE => @test_name)
      end
    end
    # rubocop:enable Metrics/BlockLength

    define_test 'set quota without a limit fails' do
      assert_raise(ArgumentError) do
        command(:set_quota, TYPE => SPACE, POLICY => NO_INSERTS, TABLE => @test_name)
      end
    end

    define_test 'set quota without a policy fails' do
      assert_raise(ArgumentError) do
        command(:set_quota, TYPE => SPACE, LIMIT => '1G', TABLE => @test_name)
      end
    end

    define_test 'set quota without a table or namespace fails' do
      assert_raise(ArgumentError) do
        command(:set_quota, TYPE => SPACE, LIMIT => '1G', POLICY => NO_INSERTS)
      end
    end

    define_test 'invalid violation policy specified' do
      assert_raise(NameError) do
        command(:set_quota, TYPE => SPACE, LIMIT => '1G', POLICY => FOO_BAR, TABLE => @test_name)
      end
    end

    define_test 'table and namespace are mutually exclusive in set quota' do
      assert_raise(ArgumentError) do
        command(:set_quota, TYPE => SPACE, LIMIT => '1G', POLICY => NO_INSERTS, TABLE => @test_name, NAMESPACE => "foo")
      end
    end

    define_test 'can set and remove quota' do
      command(:set_quota, TYPE => SPACE, LIMIT => '1G', POLICY => NO_INSERTS, TABLE => @test_name)
      output = capture_stdout{ command(:list_quotas) }
      assert(output.include?("LIMIT => 1.00G"))
      assert(output.include?("VIOLATION_POLICY => NO_INSERTS"))
      assert(output.include?("TYPE => SPACE"))
      assert(output.include?("TABLE => #{@test_name}"))

      command(:set_quota, TYPE => SPACE, LIMIT => NONE, TABLE => @test_name)
      output = capture_stdout{ command(:list_quotas) }
      assert(output.include?("0 row(s)"))
    end

    define_test 'can view size of snapshots' do
      snapshot1 = "#{@test_name}_1"
      snapshot2 = "#{@test_name}_2"
      # Set a quota on our table
      command(:set_quota, TYPE => SPACE, LIMIT => '1G', POLICY => NO_INSERTS, TABLE => @test_name)
      (1..10).each{|i| command(:put, @test_name, 'a', "x:#{i}", "#{i}")}
      command(:flush, @test_name)
      command(:snapshot, @test_name, snapshot1)
      (1..10).each{|i| command(:put, @test_name, 'b', "x:#{i}", "#{i}")}
      command(:flush, @test_name)
      command(:snapshot, @test_name, snapshot2)
      duration_to_check = 1000 * 30
      start = current = Time.now.to_i
      # Poor man's Waiter from Java test classes
      while current - start < duration_to_check
        output = capture_stdout{ command(:list_snapshot_sizes) }
        if output.include? snapshot1 and output.include? snapshot2
          break
        end
        sleep 5
        current = Time.now.to_i
      end
      output = capture_stdout{ command(:list_snapshot_sizes) }
      assert(output.include? snapshot1)
      assert(output.include? snapshot2)
    end

    define_test 'can set and remove user CU quota' do
      command(:set_quota, TYPE => THROTTLE, USER => 'user1', LIMIT => '1CU/sec')
      output = capture_stdout{ command(:list_quotas) }
      assert(output.include?('USER => user1'))
      assert(output.include?('TYPE => THROTTLE'))
      assert(output.include?('THROTTLE_TYPE => REQUEST_CAPACITY_UNIT'))
      assert(output.include?('LIMIT => 1CU/sec'))

      command(:set_quota, TYPE => THROTTLE, USER => 'user1', LIMIT => NONE)
      output = capture_stdout{ command(:list_quotas) }
      assert(output.include?('0 row(s)'))
    end

    define_test 'can set and remove table CU quota' do
      command(:set_quota, TYPE => THROTTLE, TABLE => @test_name,
              THROTTLE_TYPE => WRITE, LIMIT => '2CU/min')
      output = capture_stdout{ command(:list_quotas) }
      assert(output.include?('TABLE => hbase_shell_quota_tests_table'))
      assert(output.include?('TYPE => THROTTLE'))
      assert(output.include?('THROTTLE_TYPE => WRITE_CAPACITY_UNIT'))
      assert(output.include?('LIMIT => 2CU/min'))

      command(:set_quota, TYPE => THROTTLE, TABLE => @test_name, LIMIT => NONE)
      output = capture_stdout{ command(:list_quotas) }
      assert(output.include?('0 row(s)'))
    end

    define_test 'switch rpc throttle' do
      result = nil
      output = capture_stdout { result = command(:disable_rpc_throttle) }
      assert(output.include?('Previous rpc throttle state : true'))
      assert(result == true)

      result = nil
      output = capture_stdout { result = command(:enable_rpc_throttle) }
      assert(output.include?('Previous rpc throttle state : false'))
      assert(result == false)
    end

    define_test 'can set and remove region server quota' do
      command(:set_quota, TYPE => THROTTLE, REGIONSERVER => 'all', LIMIT => '1CU/sec')
      output = capture_stdout{ command(:list_quotas) }
      assert(output.include?('REGIONSERVER => all'))
      assert(output.include?('TYPE => THROTTLE'))
      assert(output.include?('THROTTLE_TYPE => REQUEST_CAPACITY_UNIT'))
      assert(output.include?('LIMIT => 1CU/sec'))

      command(:set_quota, TYPE => THROTTLE, REGIONSERVER => 'all', THROTTLE_TYPE => WRITE, LIMIT => '2req/sec')
      output = capture_stdout{ command(:list_quotas) }
      assert(output.include?('REGIONSERVER => all'))
      assert(output.include?('TYPE => THROTTLE'))
      assert(output.include?('THROTTLE_TYPE => WRITE'))
      assert(output.include?('THROTTLE_TYPE => REQUEST_CAPACITY_UNIT'))
      assert(output.include?('LIMIT => 2req/sec'))

      command(:set_quota, TYPE => THROTTLE, REGIONSERVER => 'all', LIMIT => NONE)
      output = capture_stdout{ command(:list_quotas) }
      assert(output.include?('0 row(s)'))
    end

    define_test 'switch exceed throttle quota' do
      command(:set_quota, TYPE => THROTTLE, REGIONSERVER => 'all', LIMIT => '1CU/sec')

      result = nil
      output = capture_stdout { result = command(:enable_exceed_throttle_quota) }
      assert(output.include?('Previous exceed throttle quota enabled : false'))
      assert(result == false)

      result = nil
      output = capture_stdout { result = command(:disable_exceed_throttle_quota) }
      assert(output.include?('Previous exceed throttle quota enabled : true'))
      assert(result == true)

      command(:set_quota, TYPE => THROTTLE, REGIONSERVER => 'all', LIMIT => NONE)
    end

    define_test 'can set and remove CLUSTER scope quota' do
      command(:set_quota, TYPE => THROTTLE, TABLE => @test_name, LIMIT => '100req/sec', SCOPE => CLUSTER)
      output = capture_stdout { command(:list_quotas) }
      assert(output.include?('LIMIT => 100req/sec'))
      assert(output.include?('SCOPE => CLUSTER'))

      command(:set_quota, TYPE => THROTTLE, TABLE => @test_name, LIMIT => '200req/sec', SCOPE => MACHINE)
      output = capture_stdout { command(:list_quotas) }
      assert(output.include?('1 row(s)'))
      assert(output.include?('LIMIT => 200req/sec'))
      assert(output.include?('SCOPE => MACHINE'))

      command(:set_quota, TYPE => THROTTLE, TABLE => @test_name, LIMIT => NONE)
      output = capture_stdout { command(:list_quotas) }
      assert(output.include?('0 row(s)'))
    end
  end
  # rubocop:enable Metrics/ClassLength
end
