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
require 'hbase_constants'
require 'hbase/hbase'
require 'hbase/table'

module Hbase
  # Simple secure administration methods tests
  class SecureAdminMethodsTest < Test::Unit::TestCase
    include TestHelpers
    include HBaseConstants

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
      test_grant_revoke_user = org.apache.hadoop.hbase.security.User.createUserForTesting(
          $TEST_CLUSTER.getConfiguration, "test_grant_revoke", []).getName()
      security_admin.grant(test_grant_revoke_user,"W", @test_name)
      security_admin.user_permission(@test_name) do |user, permission|
         assert_match(eval("/WRITE/"), permission.to_s)
      end

      security_admin.grant(test_grant_revoke_user,"RX", @test_name)
      found_permission = false
      security_admin.user_permission(@test_name) do |user, permission|
         if user == "test_grant_revoke"
           assert_match(eval("/READ/"), permission.to_s)
           assert_match(eval("/WRITE/"), permission.to_s)
           assert_match(eval("/EXEC/"), permission.to_s)
           assert_no_match(eval("/CREATE/"), permission.to_s)
           assert_no_match(eval("/ADMIN/"), permission.to_s)
           found_permission = true
         end
      end
      assert(found_permission, "Permission for user test_grant_revoke was not found.")
    end

    define_test 'Grant and revoke global permission should set access rights appropriately' do
      global_user_name = 'test_grant_revoke_global'
      security_admin.grant(global_user_name, 'W')
      found_permission = false
      security_admin.user_permission do |user, permission|
        if user == global_user_name
          assert_match(/WRITE/, permission.to_s)
          found_permission = true
        end
      end
      assert(found_permission, 'Permission for user ' + global_user_name + ' was not found.')

      found_permission = false
      security_admin.user_permission('.*') do |user, permission|
        if user == global_user_name
          assert_match(/WRITE/, permission.to_s)
          found_permission = true
        end
      end
      assert(found_permission, 'Permission for user ' + global_user_name + ' was not found.')

      found_permission = false
      security_admin.revoke(global_user_name)
      security_admin.user_permission do |user, _|
        found_permission = true if user == global_user_name
      end
      assert(!found_permission, 'Permission for user ' + global_user_name + ' was found.')
    end
  end
end
