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
require 'hbase'
require 'hbase/hbase'
require 'hbase/table'

include HBaseConstants

module Hbase
  # Simple secure administration methods tests
  class SecureAdminMethodsTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      create_test_table(@test_name)

      # Create table test table name
      @create_test_name = 'hbase_create_table_test_table'
    end

    def teardown
      shutdown
    end

    define_test "Revoke should rid access rights appropriately" do
      drop_test_table(@test_name)
      create_test_table(@test_name)
      table = table(@test_name)
      user = org.apache.hadoop.hbase.security.User.getCurrent().getName();
      assert_equal(1, security_admin.user_permission(@test_name).length)
      security_admin.revoke(user, @test_name)
      assert_equal(0, security_admin.user_permission(@test_name).length)
    end

    define_test "Grant should set access rights appropriately" do
      drop_test_table(@test_name)
      create_test_table(@test_name)
      table = table(@test_name)
      user = org.apache.hadoop.hbase.security.User.getCurrent().getName();
      security_admin.user_permission(@test_name) do |user, permission|
         assert_match(eval("/WRITE/"), permission.to_s)
      end
      security_admin.grant(user,"RXCA", @test_name)
      security_admin.user_permission(@test_name) do |user, permission|
         assert_no_match(eval("/WRITE/"), permission.to_s)
      end
    end
  end
end
