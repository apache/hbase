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

class ShellTest < Test::Unit::TestCase
  def setup
    @hbase = ::Hbase::Hbase.new($TEST_CLUSTER.getConfiguration)
    @shell = Shell::Shell.new(@hbase)
  end

  define_test "Shell::Shell#admin should return an admin instance" do
    assert_kind_of(Hbase::Admin, @shell.admin)
  end

  define_test "Shell::Shell#admin should cache admin instances" do
    assert_same(@shell.admin, @shell.admin)
  end

  #-------------------------------------------------------------------------------

  define_test "Shell::Shell#hbase_table should return a table instance" do
    assert_kind_of(Hbase::Table, @shell.hbase_table('hbase:meta'))
  end

  define_test "Shell::Shell#hbase_table should not cache table instances" do
    assert_not_same(@shell.hbase_table('hbase:meta'), @shell.hbase_table('hbase:meta'))
  end

  define_test "Shell::Shell#hbase attribute is a HBase instance" do
    assert_kind_of(Hbase::Hbase, @shell.hbase)
  end

  #-------------------------------------------------------------------------------

  define_test "Shell::Shell#export_commands should export command methods to specified object" do
    module Foo; end
    assert(!Foo.respond_to?(:version))
    @shell.export_commands(Foo)
    assert(Foo.respond_to?(:version))
  end

  #-------------------------------------------------------------------------------

  define_test "Shell::Shell#command_instance should return a command class" do
    assert_kind_of(Shell::Commands::Command, @shell.command_instance('version'))
  end

  #-------------------------------------------------------------------------------

  define_test "Shell::Shell#command should execute a command" do
    @shell.command('version')
  end

  #-------------------------------------------------------------------------------

  define_test "Shell::Shell interactive mode should not throw" do
    # incorrect number of arguments
    @shell.command('create', 'foo')
    @shell.command('create', 'foo', 'family_1')
    # create a table that exists
    @shell.command('create', 'foo', 'family_1')
  end
end
