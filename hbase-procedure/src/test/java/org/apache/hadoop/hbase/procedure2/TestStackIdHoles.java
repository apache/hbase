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
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreBase;
import org.apache.hadoop.hbase.procedure2.store.ProcedureTree;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.AtomicUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Testcase for HBASE-28210, where we persist the procedure which has been inserted later to
 * {@link RootProcedureState} first and then crash, and then cause holes in stack ids when loading,
 * and finally fail the start up of master.
 */
@Category({ MasterTests.class, SmallTests.class })
public class TestStackIdHoles {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStackIdHoles.class);

  private final class DummyProcedureStore extends ProcedureStoreBase {

    private int numThreads;

    private final LinkedHashMap<Long, ProcedureProtos.Procedure> procMap =
      new LinkedHashMap<Long, ProcedureProtos.Procedure>();

    private final AtomicLong maxProcId = new AtomicLong(0);

    private final AtomicBoolean updated = new AtomicBoolean(false);

    @Override
    public void start(int numThreads) throws IOException {
      this.numThreads = numThreads;
      setRunning(true);
    }

    @Override
    public void stop(boolean abort) {
    }

    @Override
    public int getNumThreads() {
      return numThreads;
    }

    @Override
    public int setRunningProcedureCount(int count) {
      return count;
    }

    @Override
    public void recoverLease() throws IOException {
    }

    @Override
    public void load(ProcedureLoader loader) throws IOException {
      loader.setMaxProcId(maxProcId.get());
      ProcedureTree tree = ProcedureTree.build(procMap.values());
      loader.load(tree.getValidProcs());
      loader.handleCorrupted(tree.getCorruptedProcs());
    }

    @Override
    public void insert(Procedure<?> proc, Procedure<?>[] subprocs) {
      long max = proc.getProcId();
      synchronized (procMap) {
        try {
          procMap.put(proc.getProcId(), ProcedureUtil.convertToProtoProcedure(proc));
          if (subprocs != null) {
            for (Procedure<?> p : subprocs) {
              procMap.put(p.getProcId(), ProcedureUtil.convertToProtoProcedure(p));
              max = Math.max(max, p.getProcId());
            }
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      AtomicUtils.updateMax(maxProcId, max);
    }

    @Override
    public void insert(Procedure<?>[] procs) {
      long max = -1;
      synchronized (procMap) {
        try {
          for (Procedure<?> p : procs) {
            procMap.put(p.getProcId(), ProcedureUtil.convertToProtoProcedure(p));
            max = Math.max(max, p.getProcId());
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      AtomicUtils.updateMax(maxProcId, max);
    }

    @Override
    public void update(Procedure<?> proc) {
      // inject a sleep to simulate the scenario in HBASE-28210
      if (proc.hasParent() && proc.getStackIndexes() != null) {
        int lastStackId = proc.getStackIndexes()[proc.getStackIndexes().length - 1];
        try {
          // sleep more times if the stack id is smaller
          Thread.sleep(100L * (10 - lastStackId));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        // simulate the failure when updating the second sub procedure
        if (!updated.compareAndSet(false, true)) {
          procExec.stop();
          throw new RuntimeException("inject error");
        }
      }
      synchronized (procMap) {
        try {
          procMap.put(proc.getProcId(), ProcedureUtil.convertToProtoProcedure(proc));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    @Override
    public void delete(long procId) {
      synchronized (procMap) {
        procMap.remove(procId);
      }
    }

    @Override
    public void delete(Procedure<?> parentProc, long[] subProcIds) {
      synchronized (procMap) {
        try {
          procMap.put(parentProc.getProcId(), ProcedureUtil.convertToProtoProcedure(parentProc));
          for (long procId : subProcIds) {
            procMap.remove(procId);
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    @Override
    public void delete(long[] procIds, int offset, int count) {
      synchronized (procMap) {
        for (int i = 0; i < count; i++) {
          long procId = procIds[offset + i];
          procMap.remove(procId);
        }
      }
    }
  }

  private final HBaseCommonTestingUtility HBTU = new HBaseCommonTestingUtility();

  private DummyProcedureStore procStore;

  private ProcedureExecutor<Void> procExec;

  @Before
  public void setUp() throws IOException {
    procStore = new DummyProcedureStore();
    procStore.start(4);
    procExec = new ProcedureExecutor<Void>(HBTU.getConfiguration(), null, procStore);
    procExec.init(4, true);
    procExec.startWorkers();
  }

  @After
  public void tearDown() {
    procExec.stop();
  }

  public static class DummyProcedure extends NoopProcedure<Void> {

    @Override
    protected Procedure<Void>[] execute(Void env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      return new Procedure[] { new NoopProcedure<Void>(), new NoopProcedure<Void>() };
    }
  }

  @Test
  public void testLoad() throws IOException {
    procExec.submitProcedure(new DummyProcedure());
    // wait for the error
    HBTU.waitFor(30000, () -> !procExec.isRunning());
    procExec = new ProcedureExecutor<Void>(HBTU.getConfiguration(), null, procStore);
    // make sure there is no error while loading
    procExec.init(4, true);
  }
}
