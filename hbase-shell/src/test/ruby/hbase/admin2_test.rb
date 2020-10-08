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
require 'stringio'
require 'hbase_constants'
require 'hbase/hbase'
require 'hbase/table'

module Hbase
  # Tests for the `status` shell command
  class StatusTest < Test::Unit::TestCase
    include TestHelpers
    include HBaseConstants

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = 'hbase_shell_admin2_test_table'
      drop_test_table(@test_name)
      create_test_table(@test_name)
    end

    def teardown
      shutdown
    end

    define_test 'Get replication status' do
      output = capture_stdout { replication_status('replication', 'both') }
      puts "Status output:\n#{output}"
      assert output.include? 'SOURCE'
      assert output.include? 'SINK'
    end

    define_test 'Get replication source metrics information' do
      output = capture_stdout { replication_status('replication', 'source') }
      puts "Status output:\n#{output}"
      assert output.include? 'SOURCE'
    end

    define_test 'Get replication sink metrics information' do
      output = capture_stdout { replication_status('replication', 'sink') }
      puts "Status output:\n#{output}"
      assert output.include? 'SINK'
    end

    define_test 'Get simple status' do
      output = capture_stdout { admin.status('simple', '') }
      puts "Status output:\n#{output}"
      assert output.include? 'active master'
    end

    define_test 'Get detailed status' do
      output = capture_stdout { admin.status('detailed', '') }
      puts "Status output:\n#{output}"
      # Some text which isn't in the simple output
      assert output.include? 'regionsInTransition'
    end

    define_test 'hbck_chore_run' do
      command(:hbck_chore_run)
    end
  end

  # Simple administration methods tests
  # rubocop:disable ClassLength
  class AdminSnapshotTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = "hbase_shell_tests_table"
      drop_test_table(@test_name)
      create_test_table(@test_name)
      #Test snapshot name
      @create_test_snapshot = 'hbase_shell_tests_snapshot'
    end

    def teardown
      shutdown
    end

    #-------------------------------------------------------------------------------
    define_test "Snapshot should fail with non-string table name" do
      assert_raise(ArgumentError) do
        command(:snapshot, 123, 'xxx')
      end
    end

    define_test "Snapshot should fail with non-string snapshot name" do
      assert_raise(ArgumentError) do
        command(:snapshot, @test_name, 123)
      end
    end

    define_test "Snapshot should fail without snapshot name" do
      assert_raise(ArgumentError) do
        command(:snapshot, @test_name)
      end
    end

    define_test "Snapshot should work with string args" do
      drop_test_snapshot()
      command(:snapshot, @test_name, @create_test_snapshot)
      list = command(:list_snapshots, @create_test_snapshot)
      assert_equal(1, list.size)
    end

    define_test "Snapshot should work when SKIP_FLUSH args" do
      drop_test_snapshot()
      command(:snapshot, @test_name, @create_test_snapshot, {::HBaseConstants::SKIP_FLUSH => true})
      list = command(:list_snapshots, @create_test_snapshot)
      assert_equal(1, list.size)
    end

    define_test "List snapshot without any args" do
      drop_test_snapshot()
      command(:snapshot, @test_name, @create_test_snapshot)
      list = command(:list_snapshots)
      assert_equal(1, list.size)
    end

    define_test "List snapshot for a non-existing snapshot" do
      list = command(:list_snapshots, "xyz")
      assert_equal(0, list.size)
    end

    define_test "Restore snapshot without any args" do
      assert_raise(ArgumentError) do
        command(:restore_snapshot)
      end
    end

    define_test 'Restore snapshot should work' do
      drop_test_snapshot
      restore_table = 'test_restore_snapshot_table'
      command(:create, restore_table, 'f1', 'f2')
      assert_match(/f1/, admin.describe(restore_table))
      assert_match(/f2/, admin.describe(restore_table))
      command(:snapshot, restore_table, @create_test_snapshot)
      command(:alter, restore_table, ::HBaseConstants::METHOD => 'delete', ::HBaseConstants::NAME => 'f1')
      assert_no_match(/f1/, admin.describe(restore_table))
      assert_match(/f2/, admin.describe(restore_table))
      drop_test_table(restore_table)
      command(:restore_snapshot, @create_test_snapshot)
      assert_match(/f1/, admin.describe(restore_table))
      assert_match(/f2/, admin.describe(restore_table))
      drop_test_table(restore_table)
    end

    define_test 'Restore snapshot should fail' do
      drop_test_snapshot
      restore_table = 'test_restore_snapshot_table'
      command(:create, restore_table, 'f1', 'f2')
      assert_match(/f1/, admin.describe(restore_table))
      assert_match(/f2/, admin.describe(restore_table))
      command(:snapshot, restore_table, @create_test_snapshot)
      assert_raise(RuntimeError) do
        command(:restore_snapshot, @create_test_snapshot)
      end
      drop_test_table(restore_table)
    end

    define_test "Clone snapshot without any args" do
      assert_raise(ArgumentError) do
        command(:restore_snapshot)
      end
    end

    define_test "Clone snapshot without table name args" do
      assert_raise(ArgumentError) do
        command(:clone_snapshot, @create_test_snapshot)
      end
    end

    define_test "Clone snapshot should work" do
      drop_test_snapshot()
      clone_table = "test_clone_snapshot_table"
      assert_match(eval("/" + "x" + "/"), admin.describe(@test_name))
      assert_match(eval("/" + "y" + "/"), admin.describe(@test_name))
      command(:snapshot, @test_name, @create_test_snapshot)
      command(:clone_snapshot, @create_test_snapshot, clone_table)
      assert_match(eval("/" + "x" + "/"), admin.describe(clone_table))
      assert_match(eval("/" + "y" + "/"), admin.describe(clone_table))
      drop_test_table(clone_table)
    end

    define_test "Delete snapshot without any args" do
      assert_raise(ArgumentError) do
        admin.delete_snapshot()
      end
    end

    define_test "Delete snapshot should work" do
      drop_test_snapshot()
      command(:snapshot, @test_name, @create_test_snapshot)
      list = command(:list_snapshots)
      assert_equal(1, list.size)
      admin.delete_snapshot(@create_test_snapshot)
      list = command(:list_snapshots)
      assert_equal(0, list.size)
    end

    define_test "Delete all snapshots without any args" do
      assert_raise(ArgumentError) do
        admin.delete_all_snapshot()
      end
    end

    define_test "Delete all snapshots should work" do
      drop_test_snapshot()
      command(:snapshot, @test_name, "delete_all_snapshot1")
      command(:snapshot, @test_name, "delete_all_snapshot2")
      command(:snapshot, @test_name, "snapshot_delete_all_1")
      command(:snapshot, @test_name, "snapshot_delete_all_2")
      list = command(:list_snapshots)
      assert_equal(4, list.size)
      admin.delete_all_snapshot("d.*")
      list = command(:list_snapshots)
      assert_equal(2, list.size)
      admin.delete_all_snapshot(".*")
      list = command(:list_snapshots)
      assert_equal(0, list.size)
    end

    define_test "Delete table snapshots without any args" do
      assert_raise(ArgumentError) do
        admin.delete_table_snapshots()
      end
    end

    define_test "Delete table snapshots should work" do
      drop_test_snapshot()
      command(:snapshot, @test_name, "delete_table_snapshot1")
      command(:snapshot, @test_name, "delete_table_snapshot2")
      command(:snapshot, @test_name, "snapshot_delete_table1")
      new_table = "test_delete_table_snapshots_table"
      command(:create, new_table, 'f1')
      command(:snapshot, new_table, "delete_table_snapshot3")
      list = command(:list_snapshots)
      assert_equal(4, list.size)
      admin.delete_table_snapshots(@test_name, "d.*")
      list = command(:list_snapshots)
      assert_equal(2, list.size)
      admin.delete_table_snapshots(@test_name)
      list = command(:list_snapshots)
      assert_equal(1, list.size)
      admin.delete_table_snapshots(".*", "d.*")
      list = command(:list_snapshots)
      assert_equal(0, list.size)
      drop_test_table(new_table)
    end

    define_test "List table snapshots without any args" do
      assert_raise(ArgumentError) do
        command(:list_table_snapshots)
      end
    end

    define_test "List table snapshots should work" do
      drop_test_snapshot()
      command(:snapshot, @test_name, "delete_table_snapshot1")
      command(:snapshot, @test_name, "delete_table_snapshot2")
      command(:snapshot, @test_name, "snapshot_delete_table1")
      new_table = "test_list_table_snapshots_table"
      command(:create, new_table, 'f1')
      command(:snapshot, new_table, "delete_table_snapshot3")
      list = command(:list_table_snapshots, ".*")
      assert_equal(4, list.size)
      list = command(:list_table_snapshots, @test_name, "d.*")
      assert_equal(2, list.size)
      list = command(:list_table_snapshots, @test_name)
      assert_equal(3, list.size)
      admin.delete_table_snapshots(".*")
      list = command(:list_table_snapshots, ".*", ".*")
      assert_equal(0, list.size)
      drop_test_table(new_table)
    end
  end

class CommissioningTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      # Create test table if it does not exist
      @test_name = 'hbase_shell_commissioning_test'
      drop_test_table(@test_name)
      create_test_table(@test_name)
    end

    def teardown
      shutdown
    end

    define_test 'list decommissioned regionservers' do
      server_name = admin.getServerNames([], true)[0].getServerName()
      command(:decommission_regionservers, server_name)
      begin
        output = capture_stdout { command(:list_decommissioned_regionservers) }
        puts "#{output}"
        assert output.include? 'DECOMMISSIONED REGION SERVERS'
        assert output.include? "#{server_name}"
        assert output.include? '1 row(s)'
      ensure
        command(:recommission_regionserver, server_name)
        output = capture_stdout { command(:list_decommissioned_regionservers) }
        puts "#{output}"
        assert output.include? 'DECOMMISSIONED REGION SERVERS'
        assert (output.include? "#{server_name}") ? false : true
        assert output.include? '0 row(s)'
      end
    end

    define_test 'decommission regionservers without offload' do
      server_name = admin.getServerNames([], true)[0].getServerName()
      command(:decommission_regionservers, server_name, false)
      begin
        output = capture_stdout { command(:list_decommissioned_regionservers) }
        assert (output.include? "#{server_name}")
      ensure
        command(:recommission_regionserver, server_name)
        output = capture_stdout { command(:list_decommissioned_regionservers) }
        assert (output.include? "#{server_name}") ? false : true
      end
    end

    define_test 'decommission regionservers with server names as list' do
      server_name = admin.getServerNames([], true)[0].getServerName()
      command(:decommission_regionservers, [server_name])
      begin
        output = capture_stdout { command(:list_decommissioned_regionservers) }
        assert (output.include? "#{server_name}")
      ensure
        command(:recommission_regionserver, server_name)
        output = capture_stdout { command(:list_decommissioned_regionservers) }
        assert (output.include? "#{server_name}") ? false : true
      end
    end

    define_test 'decommission regionservers with server host name only' do
      server_name = admin.getServerNames([], true)[0]
      host_name = server_name.getHostname
      server_name_str = server_name.getServerName
      command(:decommission_regionservers, host_name)
      begin
        output = capture_stdout { command(:list_decommissioned_regionservers) }
        assert output.include? "#{server_name_str}"
      ensure
        command(:recommission_regionserver, host_name)
        output = capture_stdout { command(:list_decommissioned_regionservers) }
        assert (output.include? "#{server_name_str}") ? false : true
      end
    end

    define_test 'decommission regionservers with server host name and port' do
      server_name = admin.getServerNames([], true)[0]
      host_name_and_port = server_name.getHostname + ',' +server_name.getPort.to_s
      server_name_str = server_name.getServerName
      command(:decommission_regionservers, host_name_and_port)
      begin
        output = capture_stdout { command(:list_decommissioned_regionservers) }
        assert output.include? "#{server_name_str}"
      ensure
        command(:recommission_regionserver, host_name_and_port)
        output = capture_stdout { command(:list_decommissioned_regionservers) }
        assert (output.include? "#{server_name_str}") ? false : true
      end
    end

    define_test 'decommission regionservers with non-existant server name' do
      server_name = admin.getServerNames([], true)[0].getServerName()
      assert_raise(ArgumentError) do
        command(:decommission_regionservers, 'dummy')
      end
    end

    define_test 'recommission regionserver with non-existant server name' do
      server_name = admin.getServerNames([], true)[0].getServerName()
      assert_raise(ArgumentError) do
        command(:recommission_regionserver, 'dummy')
      end
    end

    define_test 'decommission regionservers with invalid argument' do
      assert_raise(ArgumentError) do
        command(:decommission_regionservers, 1)
      end

      assert_raise(ArgumentError) do
        command(:decommission_regionservers, {1=>1})
      end

      assert_raise(ArgumentError) do
       command(:decommission_regionservers, 'dummy', 1)
      end

      assert_raise(ArgumentError) do
        command(:decommission_regionservers, 'dummy', {1=>1})
      end
    end

    define_test 'recommission regionserver with invalid argument' do
      assert_raise(ArgumentError) do
        command(:recommission_regionserver, 1)
      end

      assert_raise(ArgumentError) do
        command(:recommission_regionserver, {1=>1})
      end

      assert_raise(ArgumentError) do
        command(:recommission_regionserver, 'dummy', 1)
      end

      assert_raise(ArgumentError) do
        command(:recommission_regionserver, 'dummy', {1=>1})
      end
    end
  end
  # rubocop:enable ClassLength
end
