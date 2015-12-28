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
require 'hbase_constants'
require 'hbase/hbase'
require 'hbase/table'

include HBaseConstants

module Hbase
  # Simple secure administration methods tests
  class VisibilityLabelsAdminMethodsTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      @test_name = "hbase_shell_tests_table"
      @test_table = table(@test_name)
      # Create table test table name
      create_test_table(@test_name)
    end

    def teardown
      @test_table.close
      shutdown
    end

    define_test "Labels should be created as specified" do
      label = 'TEST_LABELS'
      count = table('hbase:labels')._count_internal
      visibility_admin.add_labels('test_label')
      assert_equal(count + 1, table('hbase:labels')._count_internal)
    end

    define_test "The set/clear methods should work with authorizations" do
      label = 'TEST_AUTHS'
      user = org.apache.hadoop.hbase.security.User.getCurrent().getName();
      visibility_admin.add_labels(label)
      $TEST_CLUSTER.waitLabelAvailable(10000, label)
      count = visibility_admin.get_auths(user).length

      # verifying the set functionality
      visibility_admin.set_auths(user, label)
      assert_equal(count + 1, visibility_admin.get_auths(user).length)
      assert_block do
        visibility_admin.get_auths(user).any? {
          |auth| org.apache.hadoop.hbase.util.Bytes::toStringBinary(auth.toByteArray) == label
        }
      end

      # verifying the clear functionality
      visibility_admin.clear_auths(user, label)
      assert_equal(count, visibility_admin.get_auths(user).length)
    end

    define_test "The get/put methods should work for data written with Visibility" do
      label = 'TEST_VISIBILITY'
      user = org.apache.hadoop.hbase.security.User.getCurrent().getName();
      visibility_admin.add_labels(label)
      $TEST_CLUSTER.waitLabelAvailable(10000, label)
      visibility_admin.set_auths(user, label)

      # verifying put functionality
      @test_table.put(1, "x:a", 31, {VISIBILITY=>label})

      # verifying get functionality
      res = @test_table._get_internal('1', {AUTHORIZATIONS=>[label]})
      assert_not_nil(res)
      assert_kind_of(Hash, res)
      assert_not_nil(res['x:a'])
    end

  end
end
