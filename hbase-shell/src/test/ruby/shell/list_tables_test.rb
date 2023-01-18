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
require 'hbase_shell'

class ListTablesTest < Test::Unit::TestCase
  def setup
    @hbase = ::Hbase::Hbase.new($TEST_CLUSTER.getConfiguration)
    @shell = Shell::Shell.new(@hbase)
    connection = $TEST_CLUSTER.getConnection
    @admin = connection.getAdmin
  end

  define_test "List enabled tables" do
    table = 'test_table1'
    family = 'f1'
    @shell.command('create', table, family)
    assert_equal [table],  @shell.command('list_enabled_tables')
    @shell.command(:disable, table)
    @shell.command(:drop, table)
  end

  define_test "List disabled tables" do
    table = 'test_table2'
    family = 'f1'
    @shell.command('create', table, family)
    @shell.command(:disable, table)
    assert_equal [table],  @shell.command('list_disabled_tables')
    @shell.command(:drop, table)
  end
end
