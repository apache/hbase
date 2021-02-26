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

import static org.junit.Assert.assertFalse;
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

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

@Category({ MasterTests.class, SmallTests.class })
public class TestProcedureSkipPersistence {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestProcedureSkipPersistence.class);
  private ProcedureExecutor<ProcEnv> procExecutor;
  private ProcedureStore procStore;

  private HBaseCommonTestingUtility htu;
  private FileSystem fs;
  private Path testDir;
  private Path logDir;

  private static volatile int STEP = 0;

  public class ProcEnv {

    public ProcedureExecutor<ProcEnv> getProcedureExecutor() {
      return procExecutor;
    }
  }

  public static class TestProcedure extends Procedure<ProcEnv> {

    // need to override this method, otherwise we will persist the release lock operation and the
    // test will fail.
    @Override
    protected boolean holdLock(ProcEnv env) {
      return true;
    }

    @Override
    protected Procedure<ProcEnv>[] execute(ProcEnv env)
        throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      if (STEP == 0) {
        STEP = 1;
        setTimeout(60 * 60 * 1000);
        setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
        skipPersistence();
        throw new ProcedureSuspendedException();
      } else if (STEP == 1) {
        STEP = 2;
        if (hasTimeout()) {
          setFailure("Should not persist the timeout value",
            new IOException("Should not persist the timeout value"));
          return null;
        }
        setTimeout(2 * 1000);
        setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
        // used to confirm that we reset the persist flag before execution
        throw new ProcedureSuspendedException();
      } else {
        if (!hasTimeout()) {
          setFailure("Should have persisted the timeout value",
            new IOException("Should have persisted the timeout value"));
        }
        return null;
      }
    }

    @Override
    protected synchronized boolean setTimeoutFailure(ProcEnv env) {
      setState(ProcedureProtos.ProcedureState.RUNNABLE);
      env.getProcedureExecutor().getProcedureScheduler().addFront(this);
      return false;
    }

    @Override
    protected void rollback(ProcEnv env) throws IOException, InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean abort(ProcEnv env) {
      return false;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }
  }

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(htu.getConfiguration());
    assertTrue(testDir.depth() > 1);

    logDir = new Path(testDir, "proc-logs");
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), logDir);
    procExecutor = new ProcedureExecutor<>(htu.getConfiguration(), new ProcEnv(), procStore);
    procStore.start(1);
    ProcedureTestingUtility.initAndStartWorkers(procExecutor, 1, true);
  }

  @After
  public void tearDown() throws IOException {
    procExecutor.stop();
    procStore.stop(false);
    fs.delete(logDir, true);
  }

  @Test
  public void test() throws Exception {
    TestProcedure proc = new TestProcedure();
    long procId = procExecutor.submitProcedure(proc);
    htu.waitFor(30000, () -> proc.isWaiting() && procExecutor.getActiveExecutorCount() == 0);
    ProcedureTestingUtility.restart(procExecutor);
    htu.waitFor(30000, () -> {
      Procedure<?> p = procExecutor.getProcedure(procId);
      return (p.isWaiting() || p.isFinished()) && procExecutor.getActiveExecutorCount() == 0;
    });
    assertFalse(procExecutor.isFinished(procId));
    ProcedureTestingUtility.restart(procExecutor);
    htu.waitFor(30000, () -> procExecutor.isFinished(procId));
    Procedure<ProcEnv> p = procExecutor.getResult(procId);
    assertTrue(p.isSuccess());
  }
}
