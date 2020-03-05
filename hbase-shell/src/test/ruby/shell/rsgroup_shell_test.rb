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

require 'hbase_constants'
require 'shell'

module Hbase
  class RSGroupShellTest < Test::Unit::TestCase
    def setup
      @hbase = ::Hbase::Hbase.new($TEST_CLUSTER.getConfiguration)
      @shell = Shell::Shell.new(@hbase)
      connection = $TEST_CLUSTER.getConnection
      @admin = connection.getAdmin
    end

    define_test 'Test Basic RSGroup Commands' do
      group_name = 'test_group'
      table_name = 'test_table'

      @shell.command('create', table_name, 'f')

      @shell.command('add_rsgroup', group_name)
      assert_not_nil(@admin.getRSGroup(group_name))

      @shell.command('remove_rsgroup', group_name)
      assert_nil(@admin.getRSGroup(group_name))

      @shell.command('add_rsgroup', group_name)
      group = @admin.getRSGroup(group_name)
      assert_not_nil(group)
      assert_equal(0, group.getServers.count)

      hostport = @admin.getRSGroup('default').getServers.iterator.next
      @shell.command('get_rsgroup', 'default')
      hostPortStr = hostport.toString
      @shell.command('get_server_rsgroup', hostPortStr)
      @shell.command('move_servers_rsgroup',
                     group_name,
                     [hostPortStr])
      assert_equal(1, @admin.getRSGroup(group_name).getServers.count)
      assert_equal(group_name, @admin.getRSGroup(hostport).getName)

      @shell.command('move_tables_rsgroup',
                     group_name,
                     [table_name])
      assert_equal(1, @admin.listTablesInRSGroup(group_name).count)

      group = @hbase.rsgroup_admin.get_rsgroup(group_name)
      assert_not_nil(group)
      assert_equal(1, group.getServers.count)
      assert_equal(hostPortStr, group.getServers.iterator.next.toString)
      assert_equal(table_name, @admin.listTablesInRSGroup(group_name).iterator.next.toString)

      assert_equal(2, @hbase.rsgroup_admin.list_rs_groups.count)

      # just run it to verify jruby->java api binding
      @hbase.rsgroup_admin.balance_rs_group(group_name)
    end

    define_test 'Test RSGroup Move Namespace RSGroup Commands' do
      group_name = 'test_group'
      namespace_name = 'test_namespace'
      ns_table_name = 'test_namespace:test_ns_table'

      @shell.command('create_namespace', namespace_name)
      @shell.command('create', ns_table_name, 'f')

      @shell.command('move_namespaces_rsgroup',
                     group_name,
                     [namespace_name])
      assert_equal(2, @admin.listTablesInRSGroup(group_name).count)

      group = @hbase.rsgroup_admin.get_rsgroup(group_name)
      assert_not_nil(group)
      assert_true(@admin.listTablesInRSGroup(group_name).contains(org.apache.hadoop.hbase.TableName.valueOf(ns_table_name)))
    end

    define_test 'Test RSGroup Move Server Namespace RSGroup Commands' do
      ns_group_name = 'test_ns_group'
      namespace_name = 'test_namespace'
      ns_table_name = 'test_namespace:test_ns_table'

      @shell.command('add_rsgroup', ns_group_name)
      assert_not_nil(@admin.getRSGroup(ns_group_name))

      @shell.command('move_tables_rsgroup',
                     'default',
                     [ns_table_name])

      group_servers = @admin.getRSGroup('default').getServers
      hostport_str = group_servers.iterator.next.toString
      @shell.command('move_servers_namespaces_rsgroup',
                     ns_group_name,
                     [hostport_str],
                     [namespace_name])
      ns_group = @hbase.rsgroup_admin.get_rsgroup(ns_group_name)
      assert_not_nil(ns_group)
      assert_equal(hostport_str, ns_group.getServers.iterator.next.toString)
      assert_equal(ns_table_name, @admin.listTablesInRSGroup(ns_group_name).iterator.next.toString)
    end

    # we test exceptions that could be thrown by the ruby wrappers
    define_test 'Test bogus arguments' do
      assert_raise(ArgumentError) do
        @hbase.rsgroup_admin().get_rsgroup('foobar')
      end
      assert_raise(ArgumentError) do
        @hbase.rsgroup_admin().get_rsgroup_of_server('foobar:123')
      end
      assert_raise(ArgumentError) do
        @hbase.rsgroup_admin().get_rsgroup_of_table('foobar')
      end
    end
  end
end
