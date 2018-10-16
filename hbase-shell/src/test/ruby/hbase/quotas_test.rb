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

require 'shell'
require 'stringio'
require 'hbase_constants'
require 'hbase/hbase'
require 'hbase/table'

include HBaseConstants

module Hbase
  class SpaceQuotasTest < Test::Unit::TestCase
    include TestHelpers

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

    define_test 'set quota with a non-numeric limit fails' do
      assert_raise(ArgumentError) do
        command(:set_quota, TYPE => SPACE, LIMIT => 'asdf', POLICY => NO_INSERTS, TABLE => @test_name)
      end
    end

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
      size = 1024 * 1024 * 1024
      assert(output.include?("LIMIT => #{size}"))
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
  end
end
