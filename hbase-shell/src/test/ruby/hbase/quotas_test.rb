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
      @test_name = "hbase_shell_tests_table"
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

    define_test '_parse_size accepts various forms of byte shorthand' do
      qa = quotas_admin()
      KILO = 1024
      MEGA = KILO * KILO
      GIGA = MEGA * KILO
      TERA = GIGA * KILO
      PETA = TERA * KILO
      assert_equal(1, qa._parse_size("1"))
      assert_equal(1, qa._parse_size("1b"))
      assert_equal(1, qa._parse_size("1B"))
      assert_equal(KILO * 2, qa._parse_size("2k"))
      assert_equal(KILO * 2, qa._parse_size("2K"))
      assert_equal(MEGA * 5, qa._parse_size("5m"))
      assert_equal(MEGA * 5, qa._parse_size("5M"))
      assert_equal(GIGA * 3, qa._parse_size("3g"))
      assert_equal(GIGA * 3, qa._parse_size("3G"))
      assert_equal(TERA * 4, qa._parse_size("4t"))
      assert_equal(TERA * 4, qa._parse_size("4T"))
      assert_equal(PETA * 32, qa._parse_size("32p"))
      assert_equal(PETA * 32, qa._parse_size("32P"))
      assert_equal(GIGA * 4, qa._parse_size("4096m"))
      assert_equal(GIGA * 4, qa._parse_size("4096M"))
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
  end
end
