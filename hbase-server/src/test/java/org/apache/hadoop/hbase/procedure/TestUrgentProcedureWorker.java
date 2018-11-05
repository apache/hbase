/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUTKey WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.procedure;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category({MasterTests.class, SmallTests.class})
public class TestUrgentProcedureWorker {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestUrgentProcedureWorker.class);

  private static final Logger LOG = LoggerFactory
      .getLogger(TestUrgentProcedureWorker.class);
  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;
  private static final CountDownLatch metaFinished = new CountDownLatch(1);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME = TableName.valueOf("TestUrgentProcedureWorker");

  private static WALProcedureStore procStore;

  private static ProcedureExecutor<TestEnv> procExec;

  private static final class TestEnv {
    private final MasterProcedureScheduler scheduler;

    public TestEnv(MasterProcedureScheduler scheduler) {
      this.scheduler = scheduler;
    }

    public MasterProcedureScheduler getScheduler() {
      return scheduler;
    }
  }

  public static class WaitingMetaProcedure extends ProcedureTestingUtility.NoopProcedure<TestEnv>
      implements TableProcedureInterface {


    @Override
    protected Procedure<TestEnv>[] execute(TestEnv env)
        throws ProcedureYieldException, ProcedureSuspendedException,
        InterruptedException {
      metaFinished.await();
      return null;
    }

    @Override
    protected Procedure.LockState acquireLock(TestEnv env) {
      if (env.getScheduler().waitTableExclusiveLock(this, getTableName())) {
        return LockState.LOCK_EVENT_WAIT;
      }
      return LockState.LOCK_ACQUIRED;
    }

    @Override
    protected void releaseLock(TestEnv env) {
      env.getScheduler().wakeTableExclusiveLock(this, getTableName());
    }

    @Override
    protected boolean holdLock(TestEnv env) {
      return true;
    }

    @Override
    public TableName getTableName() {
      return TABLE_NAME;
    }

    @Override
    public TableOperationType getTableOperationType() {
      return TableOperationType.EDIT;
    }
  }

  public static class MetaProcedure extends ProcedureTestingUtility.NoopProcedure<TestEnv>
      implements TableProcedureInterface {


    @Override
    protected Procedure<TestEnv>[] execute(TestEnv env)
        throws ProcedureYieldException, ProcedureSuspendedException,
        InterruptedException {
      metaFinished.countDown();
      return null;
    }

    @Override
    protected Procedure.LockState acquireLock(TestEnv env) {
      if (env.getScheduler().waitTableExclusiveLock(this, getTableName())) {
        return LockState.LOCK_EVENT_WAIT;
      }
      return LockState.LOCK_ACQUIRED;
    }

    @Override
    protected void releaseLock(TestEnv env) {
      env.getScheduler().wakeTableExclusiveLock(this, getTableName());
    }

    @Override
    protected boolean holdLock(TestEnv env) {
      return true;
    }

    @Override
    public TableName getTableName() {
      return TableName.META_TABLE_NAME;
    }

    @Override
    public TableOperationType getTableOperationType() {
      return TableOperationType.EDIT;
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    UTIL.cleanupTestDir();
  }

  @BeforeClass
  public static void setUp() throws IOException {
    UTIL.getConfiguration().setInt("hbase.procedure.worker.stuck.threshold.msec", 6000000);
    procStore = ProcedureTestingUtility.createWalStore(UTIL.getConfiguration(),
        UTIL.getDataTestDir("TestUrgentProcedureWorker"));
    procStore.start(1);
    MasterProcedureScheduler scheduler = new MasterProcedureScheduler(pid -> null);
    procExec = new ProcedureExecutor<>(UTIL.getConfiguration(), new TestEnv(scheduler), procStore,
        scheduler);
    procExec.init(1, false);
    procExec.startWorkers();
  }

  @Test
  public void test() throws Exception {
    WaitingMetaProcedure waitingMetaProcedure = new WaitingMetaProcedure();
    long waitProc = procExec.submitProcedure(waitingMetaProcedure);
    MetaProcedure metaProcedure = new MetaProcedure();
    long metaProc = procExec.submitProcedure(metaProcedure);
    UTIL.waitFor(5000, () -> procExec.isFinished(waitProc));

  }



}
