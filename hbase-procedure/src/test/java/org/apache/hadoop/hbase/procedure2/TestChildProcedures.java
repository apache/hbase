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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, SmallTests.class})
public class TestChildProcedures {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestChildProcedures.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestChildProcedures.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;

  private static TestProcEnv procEnv;
  private static ProcedureExecutor<TestProcEnv> procExecutor;
  private static ProcedureStore procStore;

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
    procStore = ProcedureTestingUtility.createStore(htu.getConfiguration(), logDir);
    procExecutor = new ProcedureExecutor<>(htu.getConfiguration(), procEnv, procStore);
    procExecutor.testing = new ProcedureExecutor.Testing();
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    ProcedureTestingUtility.initAndStartWorkers(procExecutor, PROCEDURE_EXECUTOR_SLOTS, true);
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
    ProcedureTestingUtility.waitProcedure(procExecutor, proc);

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
      ProcedureTestingUtility.waitProcedure(procExecutor, proc);
      restartCount++;
    }
    assertEquals(3, restartCount);
    assertTrue("expected completed proc", procExecutor.isFinished(procId));
    ProcedureTestingUtility.assertProcNotFailed(procExecutor, procId);
  }


  /**
   * Test the state setting that happens after store to WAL; in particular the bit where we
   * set the parent runnable again after its children have all completed successfully.
   * See HBASE-20978.
   */
  @Test
  public void testChildLoadWithRestartAfterChildSuccess() throws Exception {
    procEnv.toggleKillAfterStoreUpdate = true;

    TestRootProcedure proc = new TestRootProcedure();
    long procId = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    int restartCount = 0;
    while (!procExecutor.isFinished(procId)) {
      ProcedureTestingUtility.restart(procExecutor);
      ProcedureTestingUtility.waitProcedure(procExecutor, proc);
      restartCount++;
    }
    assertEquals(4, restartCount);
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
    ProcedureTestingUtility.waitProcedure(procExecutor, proc);

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
      ProcedureTestingUtility.waitProcedure(procExecutor, proc);
      restartCount++;
    }
    assertEquals(2, restartCount);
    assertProcFailed(procId);
  }

  private void assertProcFailed(long procId) {
    assertTrue("expected completed proc", procExecutor.isFinished(procId));
    Procedure<?> result = procExecutor.getResult(procId);
    assertEquals(true, result.isFailed());
    LOG.info(result.getException().getMessage());
  }

  public static class TestRootProcedure extends SequentialProcedure<TestProcEnv> {
    public TestRootProcedure() {}

    @Override
    public Procedure[] execute(TestProcEnv env) {
      if (env.toggleKillBeforeStoreUpdate) {
        ProcedureTestingUtility.toggleKillBeforeStoreUpdate(procExecutor);
      }
      if (env.toggleKillAfterStoreUpdate) {
        ProcedureTestingUtility.toggleKillAfterStoreUpdate(procExecutor);
      }
      return new Procedure[] { new TestChildProcedure(), new TestChildProcedure() };
    }

    @Override
    public void rollback(TestProcEnv env) {
    }

    @Override
    public boolean abort(TestProcEnv env) {
      return false;
    }
  }

  public static class TestChildProcedure extends SequentialProcedure<TestProcEnv> {
    public TestChildProcedure() {}

    @Override
    public Procedure[] execute(TestProcEnv env) {
      if (env.toggleKillBeforeStoreUpdate) {
        ProcedureTestingUtility.toggleKillBeforeStoreUpdate(procExecutor);
      }
      if (env.triggerRollbackOnChild) {
        setFailure("test", new Exception("test"));
      }
      return null;
    }

    @Override
    public void rollback(TestProcEnv env) {
    }

    @Override
    public boolean abort(TestProcEnv env) {
      return false;
    }
  }

  private static class TestProcEnv {
    public boolean toggleKillBeforeStoreUpdate = false;
    public boolean toggleKillAfterStoreUpdate = false;
    public boolean triggerRollbackOnChild = false;
  }
}
