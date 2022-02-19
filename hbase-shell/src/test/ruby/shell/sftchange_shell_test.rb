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

class SftChangeTest < Test::Unit::TestCase
  def setup
    @hbase = ::Hbase::Hbase.new($TEST_CLUSTER.getConfiguration)
    @shell = Shell::Shell.new(@hbase)
    connection = $TEST_CLUSTER.getConnection
    @admin = connection.getAdmin
  end

  define_test "Change table's sft" do
    table = 'test_table1'
    family = 'f1'
    change_sft = 'FILE'
    @shell.command('create', table, family)
    @shell.command('change_sft', table, change_sft)
    table_sft =  @admin.getDescriptor(TableName.valueOf(table)).getValue('hbase.store.file-tracker.impl')
    assert_equal(change_sft, table_sft)
    @shell.command(:disable, table)
    @shell.command(:drop, table)
  end

  define_test "Change table column family's sft" do
    table = 'test_table2'
    family = 'f1'
    change_sft = 'FILE'
    @shell.command('create', table, family)
    @shell.command('change_sft', table, family, change_sft)
    family_bytes = family.to_java_bytes
    cfd =  @admin.getDescriptor(TableName.valueOf(table)).getColumnFamily(family_bytes)
    table_family_sft = cfd.getConfigurationValue('hbase.store.file-tracker.impl')
    assert_equal(change_sft, table_family_sft)
    @shell.command(:disable, table)
    @shell.command(:drop, table)
  end
end
