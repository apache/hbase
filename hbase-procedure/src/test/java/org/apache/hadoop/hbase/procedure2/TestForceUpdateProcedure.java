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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Exchanger;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

@Category({ MasterTests.class, SmallTests.class })
public class TestForceUpdateProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestForceUpdateProcedure.class);

  private static HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility();

  private static WALProcedureStore STORE;

  private static ProcedureExecutor<Void> EXEC;

  private static Exchanger<Boolean> EXCHANGER = new Exchanger<>();

  private static int WAL_COUNT = 5;

  @Rule
  public final TestName name = new TestName();

  private void createStoreAndExecutor() throws IOException {
    UTIL.getConfiguration().setInt(CompletedProcedureCleaner.CLEANER_INTERVAL_CONF_KEY, 1000);
    Path logDir = UTIL.getDataTestDir(name.getMethodName());
    STORE = ProcedureTestingUtility.createWalStore(UTIL.getConfiguration(), logDir);
    STORE.start(1);
    EXEC = new ProcedureExecutor<Void>(UTIL.getConfiguration(), null, STORE);
    ProcedureTestingUtility.initAndStartWorkers(EXEC, 1, true);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    UTIL.getConfiguration().setInt(WALProcedureStore.WAL_COUNT_WARN_THRESHOLD_CONF_KEY, WAL_COUNT);
  }

  private void stopStoreAndExecutor() {
    EXEC.stop();
    STORE.stop(false);
    EXEC = null;
    STORE = null;
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    UTIL.cleanupTestDir();
  }

  @Before
  public void setUp() throws IOException {
    createStoreAndExecutor();
  }

  @After
  public void tearDown() {
    stopStoreAndExecutor();
  }

  public static final class WaitingProcedure extends NoopProcedure<Void> {

    @Override
    protected Procedure<Void>[] execute(Void env)
        throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      EXCHANGER.exchange(Boolean.TRUE);
      setState(ProcedureState.WAITING_TIMEOUT);
      setTimeout(Integer.MAX_VALUE);
      throw new ProcedureSuspendedException();
    }
  }

  public static final class ParentProcedure extends NoopProcedure<Void> {

    @SuppressWarnings("unchecked")
    @Override
    protected Procedure<Void>[] execute(Void env)
        throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      return new Procedure[] { new NoopProcedure<>(), new WaitingProcedure() };
    }
  }

  public static final class ExchangeProcedure extends NoopProcedure<Void> {

    @SuppressWarnings("unchecked")
    @Override
    protected Procedure<Void>[] execute(Void env)
        throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      if (EXCHANGER.exchange(Boolean.TRUE)) {
        return new Procedure[] { this };
      } else {
        return null;
      }
    }
  }

  public static final class NoopNoAckProcedure extends NoopProcedure<Void> {

    @Override
    protected boolean shouldWaitClientAck(Void env) {
      return false;
    }
  }

  @Test
  public void testProcedureStuck() throws IOException, InterruptedException {
    EXEC.submitProcedure(new ParentProcedure());
    EXCHANGER.exchange(Boolean.TRUE);
    UTIL.waitFor(10000, () -> EXEC.getActiveExecutorCount() == 0);
    // The above operations are used to make sure that we have persist the states of the two
    // procedures.
    long procId = EXEC.submitProcedure(new ExchangeProcedure());
    assertEquals(1, STORE.getActiveLogs().size());
    for (int i = 0; i < WAL_COUNT - 1; i++) {
      assertTrue(STORE.rollWriterForTesting());
      // The WaitinProcedure never gets updated so we can not delete the oldest wal file, so the
      // number of wal files will increase
      assertEquals(2 + i, STORE.getActiveLogs().size());
      EXCHANGER.exchange(Boolean.TRUE);
      Thread.sleep(1000);
    }
    STORE.rollWriterForTesting();
    // Finish the ExchangeProcedure
    EXCHANGER.exchange(Boolean.FALSE);
    // Make sure that we can delete several wal files because we force update the state of
    // WaitingProcedure. Notice that the last closed wal files can not be deleted, as when rolling
    // the newest wal file does not have anything in it, and in the closed file we still have the
    // state for the ExchangeProcedure so it can not be deleted
    UTIL.waitFor(10000, () -> STORE.getActiveLogs().size() <= 2);
    UTIL.waitFor(10000, () -> EXEC.isFinished(procId));
    // Make sure that after the force update we could still load the procedures
    stopStoreAndExecutor();
    createStoreAndExecutor();
    Map<Class<?>, Procedure<Void>> procMap = new HashMap<>();
    EXEC.getActiveProceduresNoCopy().forEach(p -> procMap.put(p.getClass(), p));
    assertEquals(3, procMap.size());
    ParentProcedure parentProc = (ParentProcedure) procMap.get(ParentProcedure.class);
    assertEquals(ProcedureState.WAITING, parentProc.getState());
    WaitingProcedure waitingProc = (WaitingProcedure) procMap.get(WaitingProcedure.class);
    assertEquals(ProcedureState.WAITING_TIMEOUT, waitingProc.getState());
    NoopProcedure<Void> noopProc = (NoopProcedure<Void>) procMap.get(NoopProcedure.class);
    assertEquals(ProcedureState.SUCCESS, noopProc.getState());
  }

  @Test
  public void testCompletedProcedure() throws InterruptedException, IOException {
    long procId = EXEC.submitProcedure(new ExchangeProcedure());
    EXCHANGER.exchange(Boolean.FALSE);
    UTIL.waitFor(10000, () -> EXEC.isFinished(procId));
    for (int i = 0; i < WAL_COUNT - 1; i++) {
      assertTrue(STORE.rollWriterForTesting());
      // The exchange procedure is completed but still not deleted yet so we can not delete the
      // oldest wal file
      long pid = EXEC.submitProcedure(new NoopNoAckProcedure());
      assertEquals(2 + i, STORE.getActiveLogs().size());
      UTIL.waitFor(10000, () -> EXEC.isFinished(pid));
    }
    // Only the exchange procedure can not be deleted
    UTIL.waitFor(10000, () -> EXEC.getCompletedSize() == 1);
    STORE.rollWriterForTesting();
    UTIL.waitFor(10000, () -> STORE.getActiveLogs().size() <= 1);
  }
}
