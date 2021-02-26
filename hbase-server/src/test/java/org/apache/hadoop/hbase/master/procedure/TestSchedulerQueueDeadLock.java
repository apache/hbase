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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MasterTests.class, SmallTests.class })
public class TestSchedulerQueueDeadLock {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSchedulerQueueDeadLock.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME = TableName.valueOf("deadlock");

  private static final class TestEnv {
    private final MasterProcedureScheduler scheduler;

    public TestEnv(MasterProcedureScheduler scheduler) {
      this.scheduler = scheduler;
    }

    public MasterProcedureScheduler getScheduler() {
      return scheduler;
    }
  }

  public static class TableSharedProcedure extends NoopProcedure<TestEnv>
      implements TableProcedureInterface {

    private final Semaphore latch = new Semaphore(0);

    @Override
    protected Procedure<TestEnv>[] execute(TestEnv env)
        throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      latch.acquire();
      return null;
    }

    @Override
    protected LockState acquireLock(TestEnv env) {
      if (env.getScheduler().waitTableSharedLock(this, getTableName())) {
        return LockState.LOCK_EVENT_WAIT;
      }
      return LockState.LOCK_ACQUIRED;
    }

    @Override
    protected void releaseLock(TestEnv env) {
      env.getScheduler().wakeTableSharedLock(this, getTableName());
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
      return TableOperationType.READ;
    }
  }

  public static class TableExclusiveProcedure extends NoopProcedure<TestEnv>
      implements TableProcedureInterface {

    private final Semaphore latch = new Semaphore(0);

    @Override
    protected Procedure<TestEnv>[] execute(TestEnv env)
        throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      latch.acquire();
      return null;
    }

    @Override
    protected LockState acquireLock(TestEnv env) {
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

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    UTIL.cleanupTestDir();
  }

  private WALProcedureStore procStore;

  private ProcedureExecutor<TestEnv> procExec;

  @Rule
  public final TestName name = new TestName();

  @Before
  public void setUp() throws IOException {
    UTIL.getConfiguration().setInt("hbase.procedure.worker.stuck.threshold.msec", 6000000);
    procStore = ProcedureTestingUtility.createWalStore(UTIL.getConfiguration(),
      UTIL.getDataTestDir(name.getMethodName()));
    procStore.start(1);
    MasterProcedureScheduler scheduler = new MasterProcedureScheduler(pid -> null);
    procExec = new ProcedureExecutor<>(UTIL.getConfiguration(), new TestEnv(scheduler), procStore,
      scheduler);
    procExec.init(1, false);
  }

  @After
  public void tearDown() {
    procExec.stop();
    procStore.stop(false);
  }

  public static final class TableSharedProcedureWithId extends TableSharedProcedure {

    @Override
    protected void setProcId(long procId) {
      // this is a hack to make this procedure be loaded after the procedure below as we will sort
      // the procedures by id when loading.
      super.setProcId(2L);
    }
  }

  public static final class TableExclusiveProcedureWithId extends TableExclusiveProcedure {

    @Override
    protected void setProcId(long procId) {
      // this is a hack to make this procedure be loaded before the procedure above as we will
      // sort the procedures by id when loading.
      super.setProcId(1L);
    }
  }

  @Test
  public void testTableProcedureDeadLockAfterRestarting() throws Exception {
    // let the shared procedure run first, but let it have a greater procId so when loading it will
    // be loaded at last.
    long procId1 = procExec.submitProcedure(new TableSharedProcedureWithId());
    long procId2 = procExec.submitProcedure(new TableExclusiveProcedureWithId());
    procExec.startWorkers();
    UTIL.waitFor(10000,
      () -> ((TableSharedProcedure) procExec.getProcedure(procId1)).latch.hasQueuedThreads());

    ProcedureTestingUtility.restart(procExec);

    ((TableSharedProcedure) procExec.getProcedure(procId1)).latch.release();
    ((TableExclusiveProcedure) procExec.getProcedure(procId2)).latch.release();

    UTIL.waitFor(10000, () -> procExec.isFinished(procId1));
    UTIL.waitFor(10000, () -> procExec.isFinished(procId2));
  }

  public static final class TableShardParentProcedure extends NoopProcedure<TestEnv>
      implements TableProcedureInterface {

    private boolean scheduled;

    @Override
    protected Procedure<TestEnv>[] execute(TestEnv env)
        throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      if (!scheduled) {
        scheduled = true;
        return new Procedure[] { new TableSharedProcedure() };
      }
      return null;
    }

    @Override
    protected LockState acquireLock(TestEnv env) {
      if (env.getScheduler().waitTableSharedLock(this, getTableName())) {
        return LockState.LOCK_EVENT_WAIT;
      }
      return LockState.LOCK_ACQUIRED;
    }

    @Override
    protected void releaseLock(TestEnv env) {
      env.getScheduler().wakeTableSharedLock(this, getTableName());
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
      return TableOperationType.READ;
    }
  }

  @Test
  public void testTableProcedureSubProcedureDeadLock() throws Exception {
    // the shared procedure will also schedule a shared procedure, but after the exclusive procedure
    long procId1 = procExec.submitProcedure(new TableShardParentProcedure());
    long procId2 = procExec.submitProcedure(new TableExclusiveProcedure());
    procExec.startWorkers();
    UTIL.waitFor(10000,
      () -> procExec.getProcedures().stream().anyMatch(p -> p instanceof TableSharedProcedure));
    procExec.getProcedures().stream().filter(p -> p instanceof TableSharedProcedure)
      .map(p -> (TableSharedProcedure) p).forEach(p -> p.latch.release());
    ((TableExclusiveProcedure) procExec.getProcedure(procId2)).latch.release();

    UTIL.waitFor(10000, () -> procExec.isFinished(procId1));
    UTIL.waitFor(10000, () -> procExec.isFinished(procId2));
  }
}
