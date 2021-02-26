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
import static org.junit.Assert.assertNotEquals;
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
public class TestProcedureMetrics {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestProcedureMetrics.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestProcedureMetrics.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;

  private TestProcEnv procEnv;
  private static ProcedureExecutor<TestProcEnv> procExecutor;
  private ProcedureStore procStore;

  private HBaseCommonTestingUtility htu;
  private FileSystem fs;
  private Path testDir;
  private Path logDir;

  private static int beginCount = 0;
  private static int successCount = 0;
  private static int failedCount = 0;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(htu.getConfiguration());
    assertTrue(testDir.depth() > 1);

    logDir = new Path(testDir, "proc-logs");
    procEnv = new TestProcEnv();
    procStore = ProcedureTestingUtility.createStore(htu.getConfiguration(), logDir);
    procExecutor = new ProcedureExecutor<TestProcEnv>(htu.getConfiguration(), procEnv, procStore);
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
  public void testMetricForSimpleProcedure() throws Exception {
    // procedure that executes successfully
    ProcedureMetrics proc = new ProcedureMetrics(true);
    long id = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    assertNotEquals("ProcId zero!", 0, id);
    beginCount++;
    successCount++;
    ProcedureTestingUtility.waitProcedure(procExecutor, proc);
    assertEquals("beginCount doesn't match!", beginCount, proc.beginCount);
    assertEquals("successCount doesn't match!", successCount, proc.successCount);
    assertEquals("failedCont doesn't match!", failedCount, proc.failedCount);
  }

  @Test
  public void testMetricsForFailedProcedure() throws Exception {
    // procedure that fails
    ProcedureMetrics proc = new ProcedureMetrics(false);
    long id = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    assertNotEquals("ProcId zero!", 0, id);
    beginCount++;
    failedCount++;
    ProcedureTestingUtility.waitProcedure(procExecutor, proc);
    assertEquals("beginCount doesn't match!", beginCount, proc.beginCount);
    assertEquals("successCount doesn't match!", successCount, proc.successCount);
    assertEquals("failedCont doesn't match!", failedCount, proc.failedCount);
  }

  @Test
  public void testMetricForYieldProcedure() throws Exception {
    // procedure that yields
    ProcedureMetrics proc = new ProcedureMetrics(true, true);
    long id = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    assertNotEquals("ProcId zero!", 0, id);
    beginCount++;
    successCount++;
    ProcedureTestingUtility.waitProcedure(procExecutor, proc);
    assertEquals("beginCount doesn't match!", beginCount, proc.beginCount);
    assertEquals("successCount doesn't match!", successCount, proc.successCount);
    assertEquals("failedCont doesn't match!", failedCount, proc.failedCount);
  }

  @Test
  public void testMetricForFailedYiledProcedure() {
    // procedure that yields and fails
    ProcedureMetrics proc = new ProcedureMetrics(false, true);
    long id = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    assertNotEquals("ProcId zero!", 0, id);
    beginCount++;
    failedCount++;
    ProcedureTestingUtility.waitProcedure(procExecutor, proc);
    assertEquals("beginCount doesn't match!", beginCount, proc.beginCount);
    assertEquals("successCount doesn't match!", successCount, proc.successCount);
    assertEquals("failedCont doesn't match!", failedCount, proc.failedCount);
  }

  @Test
  public void testMetricForProcedureWithChildren() throws Exception {
    // Procedure that yileds with one of the sub-procedures that fail
    int subProcCount = 10;
    int failChildIndex = 2;
    int yiledChildIndex = -1;
    ProcedureMetrics[] subprocs = new ProcedureMetrics[subProcCount];
    for (int i = 0; i < subProcCount; ++i) {
      subprocs[i] = new ProcedureMetrics(failChildIndex != i, yiledChildIndex == i, 3);
    }

    ProcedureMetrics proc = new ProcedureMetrics(true, true, 3, subprocs);
    long id = ProcedureTestingUtility.submitAndWait(procExecutor, proc);
    assertNotEquals("ProcId zero!", 0, id);
    beginCount += subProcCount + 1;
    successCount += subProcCount - (failChildIndex + 1);
    if (failChildIndex >= 0) {
      failedCount += subProcCount + 1;
    } else {
      successCount++;
    }
    ProcedureTestingUtility.waitProcedure(procExecutor, proc);
    assertEquals("beginCount doesn't match!", beginCount, proc.beginCount);
    assertEquals("successCount doesn't match!", successCount, proc.successCount);
    assertEquals("failedCont doesn't match!", failedCount, proc.failedCount);
  }

  private static class TestProcEnv {
    public boolean toggleKillBeforeStoreUpdate = false;
    public boolean triggerRollbackOnChild = false;
  }

  public static class ProcedureMetrics extends SequentialProcedure<TestProcEnv> {
    public static long beginCount = 0;
    public static long successCount = 0;
    public static long failedCount = 0;

    private boolean success;
    private boolean yield;
    private int yieldCount;
    private int yieldNum;

    private ProcedureMetrics[] subprocs = null;

    public ProcedureMetrics() {
      this(true);
    }

    public ProcedureMetrics(boolean success) {
      this(success, true);
    }

    public ProcedureMetrics(boolean success, boolean yield) {
      this(success, yield, 1);
    }

    public ProcedureMetrics(boolean success, boolean yield, int yieldCount) {
      this(success, yield, yieldCount, null);
    }

    public ProcedureMetrics(boolean success, ProcedureMetrics[] subprocs) {
      this(success, false, 1, subprocs);
    }

    public ProcedureMetrics(boolean success, boolean yield, int yieldCount,
                            ProcedureMetrics[] subprocs) {
      this.success = success;
      this.yield = yield;
      this.yieldCount = yieldCount;
      this.subprocs = subprocs;
      yieldNum = 0;
    }

    @Override
    protected void updateMetricsOnSubmit(TestProcEnv env) {
      beginCount++;
    }

    @Override
    protected Procedure[] execute(TestProcEnv env) throws ProcedureYieldException,
        ProcedureSuspendedException, InterruptedException {
      if (this.yield) {
        if (yieldNum < yieldCount) {
          yieldNum++;
          throw new ProcedureYieldException();
        }
      }
      if (!this.success) {
        setFailure("Failed", new InterruptedException("Failed"));
        return null;
      }
      return subprocs;
    }

    @Override
    protected void rollback(TestProcEnv env) throws IOException, InterruptedException {
    }

    @Override
    protected boolean abort(TestProcEnv env) {
      return false;
    }

    @Override
    protected void updateMetricsOnFinish(final TestProcEnv env, final long time, boolean success) {
      if (success) {
        successCount++;
      } else {
        failedCount++;
      }
    }
  }
}
