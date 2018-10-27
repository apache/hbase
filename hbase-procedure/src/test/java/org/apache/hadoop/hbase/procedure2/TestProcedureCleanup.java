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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.ByteStreams;

@Category({ MasterTests.class, SmallTests.class })
public class TestProcedureCleanup {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestProcedureCleanup.class);


  private static final Logger LOG = LoggerFactory.getLogger(TestProcedureCleanup.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 2;

  private static WALProcedureStore procStore;

  private static ProcedureExecutor<Void> procExecutor;

  private static HBaseCommonTestingUtility htu;

  private static FileSystem fs;
  private static Path testDir;
  private static Path logDir;

  @Rule
  public final TestName name = new TestName();

  private void createProcExecutor() throws Exception {
    logDir = new Path(testDir, name.getMethodName());
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), logDir);
    procExecutor = new ProcedureExecutor<>(htu.getConfiguration(), null, procStore);
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    ProcedureTestingUtility.initAndStartWorkers(procExecutor, PROCEDURE_EXECUTOR_SLOTS, true, true);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    htu = new HBaseCommonTestingUtility();
    htu.getConfiguration().setBoolean(WALProcedureStore.EXEC_WAL_CLEANUP_ON_LOAD_CONF_KEY, true);
    // NOTE: The executor will be created by each test
    testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(htu.getConfiguration());
    assertTrue(testDir.depth() > 1);
  }

  @Test
  public void testProcedureShouldNotCleanOnLoad() throws Exception {
    createProcExecutor();
    final RootProcedure proc = new RootProcedure();
    long rootProc = procExecutor.submitProcedure(proc);
    LOG.info("Begin to execute " + rootProc);
    // wait until the child procedure arrival
    htu.waitFor(10000, () -> procExecutor.getProcedures().size() >= 2);
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
    assertTrue(procExecutor.getProcedure(rootProc) != null);
    Thread.sleep(500);
    LOG.info("begin to restart2 ");
    ProcedureTestingUtility.restart(procExecutor, true);
    LOG.info("finish to restart2 ");
    assertTrue(procExecutor.getProcedure(rootProc) != null);
  }

  @Test
  public void testProcedureUpdatedShouldClean() throws Exception {
    createProcExecutor();
    SuspendProcedure suspendProcedure = new SuspendProcedure();
    long suspendProc = procExecutor.submitProcedure(suspendProcedure);
    LOG.info("Begin to execute " + suspendProc);
    suspendProcedure.latch.countDown();
    Thread.sleep(500);
    LOG.info("begin to restart1 ");
    ProcedureTestingUtility.restart(procExecutor, true);
    LOG.info("finish to restart1 ");
    htu.waitFor(10000, () -> procExecutor.getProcedure(suspendProc) != null);
    // Wait until the suspendProc executed after restart
    suspendProcedure = (SuspendProcedure) procExecutor.getProcedure(suspendProc);
    suspendProcedure.latch.countDown();
    Thread.sleep(500);
    // Should be 1 log since the suspendProcedure is updated in the new log
    assertTrue(procStore.getActiveLogs().size() == 1);
    // restart procExecutor
    LOG.info("begin to restart2");
    // Restart the executor but do not start the workers.
    // Otherwise, the suspendProcedure will soon be executed and the oldest log
    // will be cleaned, leaving only the newest log.
    ProcedureTestingUtility.restart(procExecutor, true, false);
    LOG.info("finish to restart2");
    // There should be two active logs
    assertTrue(procStore.getActiveLogs().size() == 2);
    procExecutor.startWorkers();

  }

  @Test
  public void testProcedureDeletedShouldClean() throws Exception {
    createProcExecutor();
    WaitProcedure waitProcedure = new WaitProcedure();
    long waitProce = procExecutor.submitProcedure(waitProcedure);
    LOG.info("Begin to execute " + waitProce);
    Thread.sleep(500);
    LOG.info("begin to restart1 ");
    ProcedureTestingUtility.restart(procExecutor, true);
    LOG.info("finish to restart1 ");
    htu.waitFor(10000, () -> procExecutor.getProcedure(waitProce) != null);
    // Wait until the suspendProc executed after restart
    waitProcedure = (WaitProcedure) procExecutor.getProcedure(waitProce);
    waitProcedure.latch.countDown();
    Thread.sleep(500);
    // Should be 1 log since the suspendProcedure is updated in the new log
    assertTrue(procStore.getActiveLogs().size() == 1);
    // restart procExecutor
    LOG.info("begin to restart2");
    // Restart the executor but do not start the workers.
    // Otherwise, the suspendProcedure will soon be executed and the oldest log
    // will be cleaned, leaving only the newest log.
    ProcedureTestingUtility.restart(procExecutor, true, false);
    LOG.info("finish to restart2");
    // There should be two active logs
    assertTrue(procStore.getActiveLogs().size() == 2);
    procExecutor.startWorkers();
  }

  private void corrupt(FileStatus file) throws IOException {
    LOG.info("Corrupt " + file);
    Path tmpFile = file.getPath().suffix(".tmp");
    // remove the last byte to make the trailer corrupted
    try (FSDataInputStream in = fs.open(file.getPath());
      FSDataOutputStream out = fs.create(tmpFile)) {
      ByteStreams.copy(ByteStreams.limit(in, file.getLen() - 1), out);
    }
    fs.delete(file.getPath(), false);
    fs.rename(tmpFile, file.getPath());
  }


  public static final class ExchangeProcedure extends ProcedureTestingUtility.NoopProcedure<Void> {

    private final Exchanger<Boolean> exchanger = new Exchanger<>();

    @SuppressWarnings("unchecked")
    @Override
    protected Procedure<Void>[] execute(Void env)
        throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      if (exchanger.exchange(Boolean.TRUE)) {
        return new Procedure[] { this };
      } else {
        return null;
      }
    }
  }

  @Test
  public void testResetDeleteWhenBuildingHoldingCleanupTracker() throws Exception {
    createProcExecutor();
    ExchangeProcedure proc1 = new ExchangeProcedure();
    ExchangeProcedure proc2 = new ExchangeProcedure();
    procExecutor.submitProcedure(proc1);
    long procId2 = procExecutor.submitProcedure(proc2);
    Thread.sleep(500);
    procStore.rollWriterForTesting();
    proc1.exchanger.exchange(Boolean.TRUE);
    Thread.sleep(500);

    FileStatus[] walFiles = fs.listStatus(logDir);
    Arrays.sort(walFiles, (f1, f2) -> f1.getPath().getName().compareTo(f2.getPath().getName()));
    // corrupt the first proc wal file, so we will have a partial tracker for it after restarting
    corrupt(walFiles[0]);
    ProcedureTestingUtility.restart(procExecutor, false, true);
    // also update proc2, which means that all the procedures in the first proc wal have been
    // updated and it should be deleted.
    proc2 = (ExchangeProcedure) procExecutor.getProcedure(procId2);
    proc2.exchanger.exchange(Boolean.TRUE);
    htu.waitFor(10000, () -> !fs.exists(walFiles[0].getPath()));
  }

  public static class WaitProcedure extends ProcedureTestingUtility.NoopProcedure<Void> {
    public WaitProcedure() {
      super();
    }

    private CountDownLatch latch = new CountDownLatch(1);

    @Override
    protected Procedure<Void>[] execute(Void env) throws ProcedureSuspendedException {
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

  public static class SuspendProcedure extends ProcedureTestingUtility.NoopProcedure<Void> {
    public SuspendProcedure() {
      super();
    }

    private CountDownLatch latch = new CountDownLatch(1);

    @Override
    protected Procedure<Void>[] execute(Void env) throws ProcedureSuspendedException {
      // Always suspend the procedure
      LOG.info("suspend here");
      latch.countDown();
      throw new ProcedureSuspendedException();
    }
  }

  public static class RootProcedure extends ProcedureTestingUtility.NoopProcedure<Void> {
    private boolean childSpwaned = false;

    public RootProcedure() {
      super();
    }

    @Override
    protected Procedure<Void>[] execute(Void env) throws ProcedureSuspendedException {
      if (!childSpwaned) {
        childSpwaned = true;
        return new Procedure[] { new SuspendProcedure() };
      } else {
        return null;
      }
    }
  }
}
