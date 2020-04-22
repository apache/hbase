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

require 'hbase'
require 'shell'

module Hbase
  class RSGroupShellTest < Test::Unit::TestCase
    def setup
      @hbase = ::Hbase::Hbase.new($TEST_CLUSTER.getConfiguration)
      @shell = Shell::Shell.new(@hbase)
      connection = $TEST_CLUSTER.getConnection
      @rsgroup_admin =
          org.apache.hadoop.hbase.rsgroup.RSGroupAdminClient.new(connection)
    end

    define_test 'Test Basic RSGroup Commands' do
      group_name = 'test_group'
      table_name = 'test_table'

      @shell.command('create', table_name, 'f')

      @shell.command('add_rsgroup', group_name)
      assert_not_nil(@rsgroup_admin.getRSGroupInfo(group_name))

      @shell.command('remove_rsgroup', group_name)
      assert_nil(@rsgroup_admin.getRSGroupInfo(group_name))

      @shell.command('add_rsgroup', group_name)
      group = @rsgroup_admin.getRSGroupInfo(group_name)
      assert_not_nil(group)
      assert_equal(0, group.getServers.count)

      address = @rsgroup_admin.getRSGroupInfo('default').getServers.iterator.next
      @shell.command('get_rsgroup', 'default')
      addressStr = address.toString
      @shell.command('get_server_rsgroup', [addressStr])
      @shell.command('move_servers_rsgroup',
                     group_name,
                     [addressStr])
      assert_equal(1, @rsgroup_admin.getRSGroupInfo(group_name).getServers.count)
      assert_equal(group_name, @rsgroup_admin.getRSGroupOfServer(address).getName)

      @shell.command('move_tables_rsgroup',
                     group_name,
                     [table_name])
      assert_equal(1, @rsgroup_admin.getRSGroupInfo(group_name).getTables.count)

      count = 0
      @hbase.rsgroup_admin.get_rsgroup(group_name) do |line|
        case count
        when 1
          assert_equal(addressStr, line)
        when 3
          assert_equal(table_name, line)
        end
        count += 1
      end
      assert_equal(4, count)

      assert_equal(2, @hbase.rsgroup_admin.list_rs_groups.count)

      # just run it to verify jruby->java api binding
      @hbase.rsgroup_admin.balance_rs_group(group_name)
    end

    # we test exceptions that could be thrown by the ruby wrappers
    define_test 'Test bogus arguments' do
      assert_raise(ArgumentError) do
        @hbase.rsgroup_admin.get_rsgroup('foobar')
      end
      assert_raise(ArgumentError) do
        @hbase.rsgroup_admin.get_rsgroup_of_server('foobar:123')
      end
      assert_raise(ArgumentError) do
        @hbase.rsgroup_admin.get_rsgroup_of_table('foobar')
      end
    end

    define_test 'Test rsgroup rename' do
      old_rs_group_name = 'test_group'
      new_rs_group_name = 'renamed_test_group'
      table_name = 'test_table'

      @hbase.rsgroup_admin.rename_rsgroup(old_rs_group_name, new_rs_group_name)
      assert_not_nil(@rsgroup_admin.getRSGroupInfo(new_rs_group_name))
      assert_nil(@rsgroup_admin.getRSGroupInfo(old_rs_group_name))
      assert_equal(1, @rsgroup_admin.getRSGroupInfo(new_rs_group_name).getServers.count)
      assert_equal(1, @rsgroup_admin.getRSGroupInfo(new_rs_group_name).getTables.count)
      assert_equal(table_name, @rsgroup_admin.getRSGroupInfo(new_rs_group_name).getTables.iterator.next.toString)
    end
  end
end
