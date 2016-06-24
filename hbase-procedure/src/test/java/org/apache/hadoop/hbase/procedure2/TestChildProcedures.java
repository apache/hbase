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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, SmallTests.class})
public class TestChildProcedures {
  private static final Log LOG = LogFactory.getLog(TestChildProcedures.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;

  private static TestProcEnv procEnv;
  private static ProcedureExecutor<TestProcEnv> procExecutor;
  private static ProcedureStore procStore;
  private static int procSleepInterval;

  private HBaseCommonTestingUtility htu;
  private FileSystem fs;
  private Path testDir;
  private Path logDir;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(htu.getConfiguration());
    assertTrue(testDir.depth() > 1);

    logDir = new Path(testDir, "proc-logs");
    procEnv = new TestProcEnv();
    procStore = ProcedureTestingUtility.createStore(htu.getConfiguration(), fs, logDir);
    procExecutor = new ProcedureExecutor(htu.getConfiguration(), procEnv, procStore);
    procExecutor.testing = new ProcedureExecutor.Testing();
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    procExecutor.start(PROCEDURE_EXECUTOR_SLOTS, true);
    procSleepInterval = 0;
  }

  @After
  public void tearDown() throws IOException {
    procExecutor.stop();
    procStore.stop(false);
    fs.delete(logDir, true);
  }

  @Test
  public void testChildLoad() throws Exception {
    procEnv.toggleKillBeforeStoreUpdate = false;

    TestRootProcedure proc = new TestRootProcedure();
    long procId = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    ProcedureTestingUtility.restart(procExecutor);
    assertTrue("expected completed proc", procExecutor.isFinished(procId));
    ProcedureTestingUtility.assertProcNotFailed(procExecutor, procId);
  }

  @Test
  public void testChildLoadWithSteppedRestart() throws Exception {
    procEnv.toggleKillBeforeStoreUpdate = true;

    TestRootProcedure proc = new TestRootProcedure();
    long procId = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    int restartCount = 0;
    while (!procExecutor.isFinished(procId)) {
      ProcedureTestingUtility.restart(procExecutor);
      restartCount++;
    }
    assertEquals(7, restartCount);
    assertTrue("expected completed proc", procExecutor.isFinished(procId));
    ProcedureTestingUtility.assertProcNotFailed(procExecutor, procId);
  }

  @Test
  public void testChildRollbackLoad() throws Exception {
    procEnv.toggleKillBeforeStoreUpdate = false;
    procEnv.triggerRollbackOnChild = true;

    TestRootProcedure proc = new TestRootProcedure();
    long procId = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    ProcedureTestingUtility.restart(procExecutor);

    assertProcFailed(procId);
  }

  @Test
  public void testChildRollbackLoadWithSteppedRestart() throws Exception {
    procEnv.toggleKillBeforeStoreUpdate = true;
    procEnv.triggerRollbackOnChild = true;

    TestRootProcedure proc = new TestRootProcedure();
    long procId = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    int restartCount = 0;
    while (!procExecutor.isFinished(procId)) {
      ProcedureTestingUtility.restart(procExecutor);
      restartCount++;
    }
    assertEquals(6, restartCount);
    assertProcFailed(procId);
  }

  private void assertProcFailed(long procId) {
    assertTrue("expected completed proc", procExecutor.isFinished(procId));
    ProcedureInfo result = procExecutor.getResult(procId);
    assertEquals(true, result.isFailed());
    LOG.info(result.getExceptionFullMessage());
  }

  public static class TestRootProcedure extends SequentialProcedure<TestProcEnv> {
    public TestRootProcedure() {}

    public Procedure[] execute(TestProcEnv env) {
      if (env.toggleKillBeforeStoreUpdate) {
        ProcedureTestingUtility.toggleKillBeforeStoreUpdate(procExecutor);
      }
      return new Procedure[] { new TestChildProcedure(), new TestChildProcedure() };
    }

    public void rollback(TestProcEnv env) {
    }

    @Override
    public boolean abort(TestProcEnv env) {
      return false;
    }
  }

  public static class TestChildProcedure extends SequentialProcedure<TestProcEnv> {
    public TestChildProcedure() {}

    public Procedure[] execute(TestProcEnv env) {
      if (env.toggleKillBeforeStoreUpdate) {
        ProcedureTestingUtility.toggleKillBeforeStoreUpdate(procExecutor);
      }
      if (env.triggerRollbackOnChild) {
        setFailure("test", new Exception("test"));
      }
      return null;
    }

    public void rollback(TestProcEnv env) {
    }

    @Override
    public boolean abort(TestProcEnv env) {
      return false;
    }
  }

  private static class TestProcEnv {
    public boolean toggleKillBeforeStoreUpdate = false;
    public boolean triggerRollbackOnChild = false;
  }
}
