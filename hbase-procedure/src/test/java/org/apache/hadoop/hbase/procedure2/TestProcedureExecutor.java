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

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;
import org.apache.hadoop.hbase.procedure2.store.NoopProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, SmallTests.class})
public class TestProcedureExecutor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestProcedureExecutor.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestProcedureExecutor.class);

  private TestProcEnv procEnv;
  private NoopProcedureStore procStore;
  private ProcedureExecutor<TestProcEnv> procExecutor;

  private HBaseCommonTestingUtility htu;

  @Before
  public void setUp() throws Exception {
    htu = new HBaseCommonTestingUtility();

    // NOTE: The executor will be created by each test
    procEnv = new TestProcEnv();
    procStore = new NoopProcedureStore();
    procStore.start(1);
  }

  @After
  public void tearDown() throws Exception {
    procExecutor.stop();
    procStore.stop(false);
    procExecutor.join();
  }

  private void createNewExecutor(final Configuration conf, final int numThreads) throws Exception {
    procExecutor = new ProcedureExecutor<>(conf, procEnv, procStore);
    ProcedureTestingUtility.initAndStartWorkers(procExecutor, numThreads, true);
  }

  @Test
  public void testWorkerStuck() throws Exception {
    // replace the executor
    final Configuration conf = new Configuration(htu.getConfiguration());
    conf.setFloat("hbase.procedure.worker.add.stuck.percentage", 0.5f);
    conf.setInt("hbase.procedure.worker.monitor.interval.msec", 500);
    conf.setInt("hbase.procedure.worker.stuck.threshold.msec", 750);

    final int NUM_THREADS = 2;
    createNewExecutor(conf, NUM_THREADS);

    Semaphore latch1 = new Semaphore(2);
    latch1.acquire(2);
    BusyWaitProcedure busyProc1 = new BusyWaitProcedure(latch1);

    Semaphore latch2 = new Semaphore(2);
    latch2.acquire(2);
    BusyWaitProcedure busyProc2 = new BusyWaitProcedure(latch2);

    long busyProcId1 = procExecutor.submitProcedure(busyProc1);
    long busyProcId2 = procExecutor.submitProcedure(busyProc2);
    long otherProcId = procExecutor.submitProcedure(new NoopProcedure());

    // wait until a new worker is being created
    int threads1 = waitThreadCount(NUM_THREADS + 1);
    LOG.info("new threads got created: " + (threads1 - NUM_THREADS));
    assertEquals(NUM_THREADS + 1, threads1);

    ProcedureTestingUtility.waitProcedure(procExecutor, otherProcId);
    assertEquals(true, procExecutor.isFinished(otherProcId));
    ProcedureTestingUtility.assertProcNotFailed(procExecutor, otherProcId);

    assertEquals(true, procExecutor.isRunning());
    assertEquals(false, procExecutor.isFinished(busyProcId1));
    assertEquals(false, procExecutor.isFinished(busyProcId2));

    // terminate the busy procedures
    latch1.release();
    latch2.release();

    LOG.info("set keep alive and wait threads being removed");
    procExecutor.setKeepAliveTime(500L, TimeUnit.MILLISECONDS);
    int threads2 = waitThreadCount(NUM_THREADS);
    LOG.info("threads got removed: " + (threads1 - threads2));
    assertEquals(NUM_THREADS, threads2);

    // terminate the busy procedures
    latch1.release();
    latch2.release();

    // wait for all procs to complete
    ProcedureTestingUtility.waitProcedure(procExecutor, busyProcId1);
    ProcedureTestingUtility.waitProcedure(procExecutor, busyProcId2);
    ProcedureTestingUtility.assertProcNotFailed(procExecutor, busyProcId1);
    ProcedureTestingUtility.assertProcNotFailed(procExecutor, busyProcId2);
  }

  @Test
  public void testSubmitBatch() throws Exception {
    Procedure[] procs = new Procedure[5];
    for (int i = 0; i < procs.length; ++i) {
      procs[i] = new NoopProcedure<TestProcEnv>();
    }

    // submit procedures
    createNewExecutor(htu.getConfiguration(), 3);
    procExecutor.submitProcedures(procs);

    // wait for procs to be completed
    for (int i = 0; i < procs.length; ++i) {
      final long procId = procs[i].getProcId();
      ProcedureTestingUtility.waitProcedure(procExecutor, procId);
      ProcedureTestingUtility.assertProcNotFailed(procExecutor, procId);
    }
  }

  private int waitThreadCount(final int expectedThreads) {
    while (procExecutor.isRunning()) {
      if (procExecutor.getWorkerThreadCount() == expectedThreads) {
        break;
      }
      LOG.debug("waiting for thread count=" + expectedThreads +
        " current=" + procExecutor.getWorkerThreadCount());
      Threads.sleepWithoutInterrupt(250);
    }
    return procExecutor.getWorkerThreadCount();
  }

  public static class BusyWaitProcedure extends NoopProcedure<TestProcEnv> {
    private final Semaphore latch;

    public BusyWaitProcedure(final Semaphore latch) {
      this.latch = latch;
    }

    @Override
    protected Procedure[] execute(final TestProcEnv env) {
      try {
        LOG.info("worker started " + this);
        if (!latch.tryAcquire(1, 30, TimeUnit.SECONDS)) {
          throw new Exception("waited too long");
        }

        LOG.info("worker step 2 " + this);
        if (!latch.tryAcquire(1, 30, TimeUnit.SECONDS)) {
          throw new Exception("waited too long");
        }
      } catch (Exception e) {
        LOG.error("got unexpected exception", e);
        setFailure("BusyWaitProcedure", e);
      }
      return null;
    }
  }

  private static class TestProcEnv { }
}
