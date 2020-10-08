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

module Hbase
  class ListLocksTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase

      @master = $TEST_CLUSTER.getHBaseClusterInterface.getMaster
      @scheduler = @master.getMasterProcedureExecutor.getEnvironment.getProcedureScheduler

      @list_locks = Shell::Commands::ListLocks.new(@shell)
    end

    def teardown
      shutdown
    end

    def set_field(object, field_name, value)
      field = object.getClass.getDeclaredField(field_name)
      field.setAccessible(true)
      field.set(object, value)
    end

    def create_lock(type, op_type, proc_id)
      lock = org.apache.hadoop.hbase.master.locking.LockProcedure.new
      set_field(lock, 'type', type)
      set_field(lock, 'opType', op_type)
      set_field(lock, 'description', 'description')
      lock.procId = proc_id
      lock.submittedTime = 0
      lock.lastUpdate = 0

      lock
    end

    def create_exclusive_lock(proc_id)
      create_lock(org.apache.hadoop.hbase.procedure2.LockType::EXCLUSIVE,
        org.apache.hadoop.hbase.master.procedure.TableProcedureInterface::TableOperationType::EDIT,
        proc_id)
    end

    def create_shared_lock(proc_id)
      create_lock(org.apache.hadoop.hbase.procedure2.LockType::SHARED,
        org.apache.hadoop.hbase.master.procedure.TableProcedureInterface::TableOperationType::READ,
        proc_id)
    end

    define_test 'list peer locks' do
      lock = create_exclusive_lock(0)
      peer_id = '1'

      @scheduler.waitPeerExclusiveLock(lock, peer_id)
      output = capture_stdout { @list_locks.command }
      @scheduler.wakePeerExclusiveLock(lock, peer_id)

      assert_equal(
        "PEER(1)\n" \
        "Lock type: EXCLUSIVE, procedure: {" \
          "\"className\"=>\"org.apache.hadoop.hbase.master.locking.LockProcedure\", " \
          "\"procId\"=>\"0\", \"submittedTime\"=>\"0\", \"state\"=>\"RUNNABLE\", " \
          "\"lastUpdate\"=>\"0\", " \
          "\"stateMessage\"=>[{\"lockType\"=>\"EXCLUSIVE\", \"description\"=>\"description\"}]" \
        "}\n\n",
        output)
    end

    define_test 'list server locks' do
      lock = create_exclusive_lock(0)

      server_name = org.apache.hadoop.hbase.ServerName.valueOf('server1,1234,0')

      @scheduler.waitServerExclusiveLock(lock, server_name)
      output = capture_stdout { @list_locks.command }
      @scheduler.wakeServerExclusiveLock(lock, server_name)

      assert_equal(
        "SERVER(server1,1234,0)\n" \
        "Lock type: EXCLUSIVE, procedure: {" \
          "\"className\"=>\"org.apache.hadoop.hbase.master.locking.LockProcedure\", " \
          "\"procId\"=>\"0\", \"submittedTime\"=>\"0\", \"state\"=>\"RUNNABLE\", " \
          "\"lastUpdate\"=>\"0\", " \
          "\"stateMessage\"=>[{\"lockType\"=>\"EXCLUSIVE\", \"description\"=>\"description\"}]" \
        "}\n\n",
        output)
    end

    define_test 'list namespace locks' do
      lock = create_exclusive_lock(1)

      @scheduler.waitNamespaceExclusiveLock(lock, 'ns1')
      output = capture_stdout { @list_locks.command }
      @scheduler.wakeNamespaceExclusiveLock(lock, 'ns1')

      assert_equal(
        "NAMESPACE(ns1)\n" \
        "Lock type: EXCLUSIVE, procedure: {" \
          "\"className\"=>\"org.apache.hadoop.hbase.master.locking.LockProcedure\", " \
          "\"procId\"=>\"1\", \"submittedTime\"=>\"0\", \"state\"=>\"RUNNABLE\", " \
          "\"lastUpdate\"=>\"0\", " \
          "\"stateMessage\"=>[{\"lockType\"=>\"EXCLUSIVE\", \"description\"=>\"description\"}]" \
        "}\n\n" \
        "TABLE(hbase:namespace)\n" \
        "Lock type: SHARED, count: 1\n\n",
        output)
    end

    define_test 'list table locks' do
      lock = create_exclusive_lock(2)

      table_name = org.apache.hadoop.hbase.TableName.valueOf('ns2', 'table2')

      @scheduler.waitTableExclusiveLock(lock, table_name)
      output = capture_stdout { @list_locks.command }
      @scheduler.wakeTableExclusiveLock(lock, table_name)

      assert_equal(
        "NAMESPACE(ns2)\n" \
        "Lock type: SHARED, count: 1\n\n" \
        "TABLE(ns2:table2)\n" \
        "Lock type: EXCLUSIVE, procedure: {" \
          "\"className\"=>\"org.apache.hadoop.hbase.master.locking.LockProcedure\", " \
          "\"procId\"=>\"2\", \"submittedTime\"=>\"0\", \"state\"=>\"RUNNABLE\", " \
          "\"lastUpdate\"=>\"0\", " \
          "\"stateMessage\"=>[{\"lockType\"=>\"EXCLUSIVE\", \"description\"=>\"description\"}]" \
        "}\n\n",
        output)
    end

    define_test 'list region locks' do
      lock = create_exclusive_lock(3)

      table_name = org.apache.hadoop.hbase.TableName.valueOf('ns3', 'table3')
      region_info = org.apache.hadoop.hbase.HRegionInfo.new(table_name)

      @scheduler.waitRegion(lock, region_info)
      output = capture_stdout { @list_locks.command }
      @scheduler.wakeRegion(lock, region_info)

      assert_equal(
        "NAMESPACE(ns3)\n" \
        "Lock type: SHARED, count: 1\n\n" \
        "TABLE(ns3:table3)\n" \
        "Lock type: SHARED, count: 1\n\n" \
        "REGION(" << region_info.getEncodedName << ")\n" \
        "Lock type: EXCLUSIVE, procedure: {" \
          "\"className\"=>\"org.apache.hadoop.hbase.master.locking.LockProcedure\", " \
          "\"procId\"=>\"3\", \"submittedTime\"=>\"0\", \"state\"=>\"RUNNABLE\", " \
          "\"lastUpdate\"=>\"0\", " \
          "\"stateMessage\"=>[{\"lockType\"=>\"EXCLUSIVE\", \"description\"=>\"description\"}]" \
        "}\n\n",
        output)
    end

    define_test 'list waiting locks' do
      table_name = org.apache.hadoop.hbase.TableName.valueOf('ns4', 'table4')

      lock1 = create_exclusive_lock(1)
      set_field(lock1, 'tableName', table_name)

      lock2 = create_shared_lock(2)
      set_field(lock2, 'tableName', table_name)

      @scheduler.waitTableExclusiveLock(lock1, table_name)
      @scheduler.waitTableSharedLock(lock2, table_name)
      output = capture_stdout { @list_locks.command }
      @scheduler.wakeTableExclusiveLock(lock1, table_name)
      @scheduler.wakeTableSharedLock(lock2, table_name)

      assert_equal(
        "NAMESPACE(ns4)\n" \
        "Lock type: SHARED, count: 1\n\n" \
        "TABLE(ns4:table4)\n" \
        "Lock type: EXCLUSIVE, procedure: {" \
          "\"className\"=>\"org.apache.hadoop.hbase.master.locking.LockProcedure\", " \
          "\"procId\"=>\"1\", \"submittedTime\"=>\"0\", \"state\"=>\"RUNNABLE\", " \
          "\"lastUpdate\"=>\"0\", \"stateMessage\"=>[{" \
            "\"lockType\"=>\"EXCLUSIVE\", " \
            "\"tableName\"=>{\"namespace\"=>\"bnM0\", \"qualifier\"=>\"dGFibGU0\"" \
          "}, \"description\"=>\"description\"}]}\n" \
        "Waiting procedures\n" \
        "{\"className\"=>\"org.apache.hadoop.hbase.master.locking.LockProcedure\", " \
          "\"procId\"=>\"2\", \"submittedTime\"=>\"0\", \"state\"=>\"RUNNABLE\", " \
          "\"lastUpdate\"=>\"0\", \"stateMessage\"=>[{" \
            "\"lockType\"=>\"SHARED\", " \
            "\"tableName\"=>{\"namespace\"=>\"bnM0\", \"qualifier\"=>\"dGFibGU0\"}, " \
            "\"description\"=>\"description\"}]}\n" \
        "1 row(s)\n\n",
        output)
    end
  end
end
