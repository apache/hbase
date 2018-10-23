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
package org.apache.hadoop.hbase.procedure2;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category({MasterTests.class, SmallTests.class})
public class TestProcedureCleanup {
  @ClassRule public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule
      .forClass(TestProcedureCleanup.class);


  private static final Logger LOG = LoggerFactory.getLogger(TestProcedureCleanup.class);
  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;

  private static TestProcEnv procEnv;
  private static WALProcedureStore procStore;

  private static ProcedureExecutor<TestProcEnv> procExecutor;

  private static HBaseCommonTestingUtility htu;

  private static FileSystem fs;
  private static Path testDir;
  private static Path logDir;

  private static class TestProcEnv {

  }

  private void createProcExecutor(String dir) throws Exception {
    logDir = new Path(testDir, dir);
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), logDir);
    procExecutor = new ProcedureExecutor<>(htu.getConfiguration(), procEnv,
        procStore);
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    ProcedureTestingUtility
        .initAndStartWorkers(procExecutor, PROCEDURE_EXECUTOR_SLOTS, true, true);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    htu = new HBaseCommonTestingUtility();

    // NOTE: The executor will be created by each test
    procEnv = new TestProcEnv();
    testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(htu.getConfiguration());
    assertTrue(testDir.depth() > 1);


  }

  @Test
  public void testProcedureShouldNotCleanOnLoad() throws Exception {
    createProcExecutor("testProcedureShouldNotCleanOnLoad");
    final RootProcedure proc = new RootProcedure();
    long rootProc = procExecutor.submitProcedure(proc);
    LOG.info("Begin to execute " + rootProc);
    // wait until the child procedure arrival
    while(procExecutor.getProcedures().size() < 2) {
      Thread.sleep(100);
    }
    SuspendProcedure suspendProcedure = (SuspendProcedure) procExecutor
        .getProcedures().get(1);
    // wait until the suspendProcedure executed
    suspendProcedure.latch.countDown();
    Thread.sleep(100);
    // roll the procedure log
    LOG.info("Begin to roll log ");
    procStore.rollWriterForTesting();
    LOG.info("finish to roll log ");
    Thread.sleep(500);
    LOG.info("begin to restart1 ");
    ProcedureTestingUtility.restart(procExecutor, true);
    LOG.info("finish to restart1 ");
    Assert.assertTrue(procExecutor.getProcedure(rootProc) != null);
    Thread.sleep(500);
    LOG.info("begin to restart2 ");
    ProcedureTestingUtility.restart(procExecutor, true);
    LOG.info("finish to restart2 ");
    Assert.assertTrue(procExecutor.getProcedure(rootProc) != null);
  }

  @Test
  public void testProcedureUpdatedShouldClean() throws Exception {
    createProcExecutor("testProcedureUpdatedShouldClean");
    SuspendProcedure suspendProcedure = new SuspendProcedure();
    long suspendProc = procExecutor.submitProcedure(suspendProcedure);
    LOG.info("Begin to execute " + suspendProc);
    suspendProcedure.latch.countDown();
    Thread.sleep(500);
    LOG.info("begin to restart1 ");
    ProcedureTestingUtility.restart(procExecutor, true);
    LOG.info("finish to restart1 ");
    while(procExecutor.getProcedure(suspendProc) == null) {
      Thread.sleep(100);
    }
    // Wait until the suspendProc executed after restart
    suspendProcedure = (SuspendProcedure) procExecutor.getProcedure(suspendProc);
    suspendProcedure.latch.countDown();
    Thread.sleep(500);
    // Should be 1 log since the suspendProcedure is updated in the new log
    Assert.assertTrue(procStore.getActiveLogs().size() == 1);
    // restart procExecutor
    LOG.info("begin to restart2");
    // Restart the executor but do not start the workers.
    // Otherwise, the suspendProcedure will soon be executed and the oldest log
    // will be cleaned, leaving only the newest log.
    ProcedureTestingUtility.restart(procExecutor, true, false);
    LOG.info("finish to restart2");
    // There should be two active logs
    Assert.assertTrue(procStore.getActiveLogs().size() == 2);
    procExecutor.startWorkers();

  }

  @Test
  public void testProcedureDeletedShouldClean() throws Exception {
    createProcExecutor("testProcedureDeletedShouldClean");
    WaitProcedure waitProcedure = new WaitProcedure();
    long waitProce = procExecutor.submitProcedure(waitProcedure);
    LOG.info("Begin to execute " + waitProce);
    Thread.sleep(500);
    LOG.info("begin to restart1 ");
    ProcedureTestingUtility.restart(procExecutor, true);
    LOG.info("finish to restart1 ");
    while(procExecutor.getProcedure(waitProce) == null) {
      Thread.sleep(100);
    }
    // Wait until the suspendProc executed after restart
    waitProcedure = (WaitProcedure) procExecutor.getProcedure(waitProce);
    waitProcedure.latch.countDown();
    Thread.sleep(500);
    // Should be 1 log since the suspendProcedure is updated in the new log
    Assert.assertTrue(procStore.getActiveLogs().size() == 1);
    // restart procExecutor
    LOG.info("begin to restart2");
    // Restart the executor but do not start the workers.
    // Otherwise, the suspendProcedure will soon be executed and the oldest log
    // will be cleaned, leaving only the newest log.
    ProcedureTestingUtility.restart(procExecutor, true, false);
    LOG.info("finish to restart2");
    // There should be two active logs
    Assert.assertTrue(procStore.getActiveLogs().size() == 2);
    procExecutor.startWorkers();
  }

  public static class WaitProcedure
      extends ProcedureTestingUtility.NoopProcedure<TestProcEnv> {
    public WaitProcedure() {
      super();
    }

    private CountDownLatch latch = new CountDownLatch(1);

    @Override
    protected Procedure[] execute(final TestProcEnv env)
        throws ProcedureSuspendedException {
      // Always wait here
      LOG.info("wait here");
      try {
        latch.await();
      } catch (Throwable t) {

      }
      LOG.info("finished");
      return null;
    }
  }

  public static class SuspendProcedure extends ProcedureTestingUtility.NoopProcedure<TestProcEnv> {
    public SuspendProcedure() {
      super();
    }

    private CountDownLatch latch = new CountDownLatch(1);

    @Override
    protected Procedure[] execute(final TestProcEnv env)
        throws ProcedureSuspendedException {
      // Always suspend the procedure
      LOG.info("suspend here");
      latch.countDown();
      throw new ProcedureSuspendedException();
    }
  }

  public static class RootProcedure extends ProcedureTestingUtility.NoopProcedure<TestProcEnv> {
    private boolean childSpwaned = false;

    public RootProcedure() {
      super();
    }

    @Override
    protected Procedure[] execute(final TestProcEnv env)
        throws ProcedureSuspendedException {
      if (!childSpwaned) {
        childSpwaned = true;
        return new Procedure[] {new SuspendProcedure()};
      } else {
        return null;
      }
    }
  }


}
