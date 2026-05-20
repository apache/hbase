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
require 'irb/hirb'
require 'stringio'

class ShellTest < Test::Unit::TestCase
  include Hbase::TestHelpers

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

  define_test 'Shell::Shell#export_all export commands, constants, and variables' do
    module FooM; end
    class FooC; end
    foo = FooC.new

    # export_all should reject classes and modules as targets
    assert_raise(ArgumentError) do
      @shell.export_all(FooM)
    end
    assert_raise(ArgumentError) do
      @shell.export_all(FooC)
    end

    # For potency, verify that none of the commands, variables or constants exist before export
    assert(!foo.respond_to?(:version))
    assert(foo.instance_variable_get(:'@shell').nil?)
    assert(foo.instance_variable_get(:'@hbase').nil?)
    assert(!foo.class.const_defined?(:IN_MEMORY_COMPACTION)) # From HBaseConstants
    assert(!foo.class.const_defined?(:QUOTA_TABLE_NAME)) # From HBaseQuotasConstants

    @shell.export_all(foo)

    # Now verify that all the commands, variables, and constants are installed
    assert(foo.respond_to?(:version))
    assert(foo.instance_variable_get(:'@shell') == @shell)
    assert(foo.instance_variable_get(:'@hbase') == @hbase)
    assert(foo.class.const_defined?(:IN_MEMORY_COMPACTION)) # From HBaseConstants
    assert(foo.class.const_defined?(:QUOTA_TABLE_NAME)) # From HBaseQuotasConstants

    # commands should not exist on the class of target
    assert_raise(NameError) do
      FooC.method :version
    end
    assert_raise(NameError) do
      FooC.instance_method :version
    end
  end

  #-------------------------------------------------------------------------------

  define_test "Shell::Shell#command_instance should return a command class" do
    assert_kind_of(Shell::Commands::Command, @shell.command_instance('version'))
  end

  #-------------------------------------------------------------------------------

  define_test "Shell::Shell#command should execute a command" do
    @shell.command('version')
  end

  #-----------------------------------------------------------------------------

  define_test 'Shell::Shell#exception_handler should hide traceback' do
    class TestException < RuntimeError; end
    # When hide_traceback is true, exception_handler should replace exceptions
    # with SystemExit so that the traceback is not printed.
    assert_raises(SystemExit) do
      ::Shell::Shell.exception_handler(true) { raise TestException, 'Custom Exception' }
    end
  end

  define_test 'Shell::Shell#exception_handler should show traceback' do
    class TestException < RuntimeError; end
    # When hide_traceback is false, exception_handler should re-raise Exceptions
    assert_raises(TestException) do
      ::Shell::Shell.exception_handler(false) { raise TestException, 'Custom Exception' }
    end
  end

  #-----------------------------------------------------------------------------

  define_test 'Shell::Shell#print_banner should display Reference Guide link' do
    @shell.interactive = true
    output = capture_stdout { @shell.print_banner }
    @shell.interactive = false
    link_regex = %r{For Reference, please visit: https://hbase.apache.org/docs/shell}
    assert_match(link_regex, output)
  end

  #-----------------------------------------------------------------------------

  define_test 'Shell::Shell interactive mode should not throw' do
    # incorrect number of arguments
    @shell.command('create', 'nothrow_table')
    @shell.command('create', 'nothrow_table', 'family_1')
    # create a table that exists
    @shell.command('create', 'nothrow_table', 'family_1')
  end

  #-----------------------------------------------------------------------------

  class MockInputMethod < IRB::InputMethod
    def initialize(lines)
      super()
      @lines = lines
    end
    def gets
      @lines.shift
    end
    def eof?
      @lines.empty?
    end
    def encoding
      Encoding::UTF_8
    end
    def readable_after_eof?
      false
    end
  end

  define_test 'Shell::Shell should prevent HBase commands from being shadowed by local variables (HBASE-28660)' do
    workspace = @shell.get_workspace
    IRB.setup(__FILE__) unless IRB.conf[:IRB_NAME]

    lines = [
      "list = 10\n",
      "list_namespace, 'ns.*'\n",
      "list_snapshots, 'snap01'\n",
      "scan = 20\n",
      "processlist = 30\n",
      "my_var = 5\n"
    ]

    input_method = MockInputMethod.new(lines)
    hirb = IRB::HIRB.new(workspace, true, input_method)

    hirb.context.prompt_i = ""
    hirb.context.prompt_s = ""
    hirb.context.prompt_c = ""
    hirb.context.prompt_n = ""
    hirb.context.return_format = ""
    hirb.context.echo = false

    old_stderr = $stderr
    $stderr = StringIO.new
    err_output = ""
    begin
      capture_stdout do
        hirb.eval_input
      end
    ensure
      err_output = $stderr.string
      $stderr = old_stderr
    end

    final_workspace = hirb.context.workspace
    final_vars = final_workspace.binding.local_variables

    assert(final_vars.include?(:my_var), "Valid variables should be preserved")
    assert_equal(5, final_workspace.binding.local_variable_get(:my_var))

    assert(!final_vars.include?(:list), "Command 'list' should not be shadowed")
    assert(!final_vars.include?(:list_namespace), "Command 'list_namespace' should not be shadowed")
    assert(!final_vars.include?(:list_snapshots), "Command 'list_snapshots' should not be shadowed")
    assert(!final_vars.include?(:scan), "Command 'scan' should not be shadowed")
    assert(!final_vars.include?(:processlist), "Command 'processlist' should not be shadowed")

    assert_match(/WARN: 'list' is a reserved HBase command/, err_output)
    assert_match(/WARN: 'list_namespace' is a reserved HBase command/, err_output)
    assert_match(/WARN: 'list_snapshots' is a reserved HBase command/, err_output)
    assert_match(/WARN: 'scan' is a reserved HBase command/, err_output)
    assert_match(/WARN: 'processlist' is a reserved HBase command/, err_output)
  end
end
