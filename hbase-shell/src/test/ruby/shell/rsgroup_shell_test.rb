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
require 'shell/formatter'

module Hbase
  class RSGroupShellTest < Test::Unit::TestCase
    def setup
      @formatter = ::Shell::Formatter::Console.new
      @hbase = ::Hbase::Hbase.new($TEST_CLUSTER.getConfiguration)
      @shell = Shell::Shell.new(@hbase, @formatter)
      connection = $TEST_CLUSTER.getConnection
      @rsgroup_admin =
          org.apache.hadoop.hbase.rsgroup.RSGroupAdmin.newClient(connection)
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

      hostport =
          @rsgroup_admin.getRSGroupInfo('default').getServers.iterator.next.toString
      @shell.command('move_rsgroup_servers',
                     group_name,
                     [hostport])
      assert_equal(1, @rsgroup_admin.getRSGroupInfo(group_name).getServers.count)

      @shell.command('move_rsgroup_tables',
                     group_name,
                     [table_name])
      assert_equal(1, @rsgroup_admin.getRSGroupInfo(group_name).getTables.count)

      count = 0
      @hbase.rsgroup_admin(@formatter).get_rsgroup(group_name) do |line|
        case count
        when 1
          assert_equal(hostport, line)
        when 3
          assert_equal(table_name, line)
        end
        count += 1
      end
      assert_equal(4, count)

      assert_equal(2,
                   @hbase.rsgroup_admin(@formatter).list_rs_groups.count)

      # just run it to verify jruby->java api binding
      @hbase.rsgroup_admin(@formatter).balance_rs_group(group_name)
    end

    # we test exceptions that could be thrown by the ruby wrappers
    define_test 'Test bogus arguments' do
      assert_raise(ArgumentError) do
        @hbase.rsgroup_admin(@formatter).get_rsgroup('foobar')
      end
      assert_raise(ArgumentError) do
        @hbase.rsgroup_admin(@formatter).get_rsgroup_of_server('foobar:123')
      end
      assert_raise(ArgumentError) do
        @hbase.rsgroup_admin(@formatter).get_rsgroup_of_table('foobar')
      end
    end
  end
end
