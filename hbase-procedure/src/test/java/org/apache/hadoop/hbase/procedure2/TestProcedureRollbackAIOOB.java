/*
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
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Testcase for HBASE-20973
 */
@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestProcedureRollbackAIOOB {

  private static final HBaseCommonTestingUtil UTIL = new HBaseCommonTestingUtil();

  public static final class ParentProcedure extends NoopProcedure<Void> {

    private final CountDownLatch latch = new CountDownLatch(1);

    private boolean scheduled;

    @Override
    protected Procedure<Void>[] execute(Void env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      latch.await();
      if (scheduled) {
        return null;
      }
      scheduled = true;
      return new Procedure[] { new SubProcedure() };
    }
  }

  public static final class SubProcedure extends NoopProcedure<Void> {

    @Override
    protected Procedure[] execute(Void env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      setFailure("Inject error", new RuntimeException("Inject error"));
      return null;
    }
  }

  private WALProcedureStore procStore;

  private ProcedureExecutor<Void> procExec;

  @BeforeEach
  public void setUp(TestInfo testInfo) throws IOException {
    procStore = ProcedureTestingUtility.createWalStore(UTIL.getConfiguration(),
      UTIL.getDataTestDir(testInfo.getTestMethod().get().getName()));
    procStore.start(2);
    procExec = new ProcedureExecutor<>(UTIL.getConfiguration(), null, procStore);
    ProcedureTestingUtility.initAndStartWorkers(procExec, 2, true);
  }

  @AfterEach
  public void tearDown() {
    procExec.stop();
    procStore.stop(false);
  }

  @AfterAll
  public static void tearDownAfterClass() throws IOException {
    UTIL.cleanupTestDir();
  }

  @Test
  public void testArrayIndexOutOfBounds() {
    ParentProcedure proc = new ParentProcedure();
    long procId = procExec.submitProcedure(proc);
    long noopProcId = -1L;
    // make sure that the sub procedure will have a new BitSetNode
    for (int i = 0; i < Long.SIZE - 2; i++) {
      noopProcId = procExec.submitProcedure(new NoopProcedure<>());
    }
    final long lastNoopProcId = noopProcId;
    UTIL.waitFor(30000, () -> procExec.isFinished(lastNoopProcId));
    proc.latch.countDown();
    UTIL.waitFor(10000, () -> procExec.isFinished(procId));
  }
}
