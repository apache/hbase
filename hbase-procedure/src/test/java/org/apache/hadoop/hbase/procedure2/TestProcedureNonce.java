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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, SmallTests.class})
public class TestProcedureNonce {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestProcedureNonce.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestProcedureNonce.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 2;

  private static TestProcEnv procEnv;
  private static ProcedureExecutor<TestProcEnv> procExecutor;
  private static ProcedureStore procStore;

  private HBaseCommonTestingUtility htu;
  private FileSystem fs;
  private Path logDir;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    Path testDir = htu.getDataTestDir();
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
  public void testCompletedProcWithSameNonce() throws Exception {
    final long nonceGroup = 123;
    final long nonce = 2222;

    // register the nonce
    final NonceKey nonceKey = procExecutor.createNonceKey(nonceGroup, nonce);
    assertFalse(procExecutor.registerNonce(nonceKey) >= 0);

    // Submit a proc and wait for its completion
    Procedure proc = new TestSingleStepProcedure();
    long procId = procExecutor.submitProcedure(proc, nonceKey);
    ProcedureTestingUtility.waitProcedure(procExecutor, procId);

    // Restart
    ProcedureTestingUtility.restart(procExecutor);
    ProcedureTestingUtility.waitProcedure(procExecutor, procId);

    // try to register a procedure with the same nonce
    // we should get back the old procId
    assertEquals(procId, procExecutor.registerNonce(nonceKey));

    Procedure<?> result = procExecutor.getResult(procId);
    ProcedureTestingUtility.assertProcNotFailed(result);
  }

  @Test
  public void testRunningProcWithSameNonce() throws Exception {
    final long nonceGroup = 456;
    final long nonce = 33333;

    // register the nonce
    final NonceKey nonceKey = procExecutor.createNonceKey(nonceGroup, nonce);
    assertFalse(procExecutor.registerNonce(nonceKey) >= 0);

    // Submit a proc and use a latch to prevent the step execution until we submitted proc2
    CountDownLatch latch = new CountDownLatch(1);
    TestSingleStepProcedure proc = new TestSingleStepProcedure();
    procEnv.setWaitLatch(latch);
    long procId = procExecutor.submitProcedure(proc, nonceKey);
    while (proc.step != 1) {
      Threads.sleep(25);
    }

    // try to register a procedure with the same nonce
    // we should get back the old procId
    assertEquals(procId, procExecutor.registerNonce(nonceKey));

    // complete the procedure
    latch.countDown();

    // Restart, the procedure is not completed yet
    ProcedureTestingUtility.restart(procExecutor);
    ProcedureTestingUtility.waitProcedure(procExecutor, procId);

    // try to register a procedure with the same nonce
    // we should get back the old procId
    assertEquals(procId, procExecutor.registerNonce(nonceKey));

    Procedure<?> result = procExecutor.getResult(procId);
    ProcedureTestingUtility.assertProcNotFailed(result);
  }

  @Test
  public void testSetFailureResultForNonce() throws IOException {
    final long nonceGroup = 234;
    final long nonce = 55555;

    // check and register the request nonce
    final NonceKey nonceKey = procExecutor.createNonceKey(nonceGroup, nonce);
    assertFalse(procExecutor.registerNonce(nonceKey) >= 0);

    procExecutor.setFailureResultForNonce(nonceKey, "testProc", User.getCurrent(),
      new IOException("test failure"));

    final long procId = procExecutor.registerNonce(nonceKey);
    Procedure<?> result = procExecutor.getResult(procId);
    ProcedureTestingUtility.assertProcFailed(result);
  }

  @Test
  public void testConcurrentNonceRegistration() throws IOException {
    testConcurrentNonceRegistration(true, 567, 44444);
  }

  @Test
  public void testConcurrentNonceRegistrationWithRollback() throws IOException {
    testConcurrentNonceRegistration(false, 890, 55555);
  }

  private void testConcurrentNonceRegistration(final boolean submitProcedure,
      final long nonceGroup, final long nonce) throws IOException {
    // register the nonce
    final NonceKey nonceKey = procExecutor.createNonceKey(nonceGroup, nonce);

    final AtomicReference<Throwable> t1Exception = new AtomicReference();
    final AtomicReference<Throwable> t2Exception = new AtomicReference();

    final CountDownLatch t1NonceRegisteredLatch = new CountDownLatch(1);
    final CountDownLatch t2BeforeNonceRegisteredLatch = new CountDownLatch(1);
    final Thread[] threads = new Thread[2];
    threads[0] = new Thread() {
      @Override
      public void run() {
        try {
          // release the nonce and wake t2
          assertFalse("unexpected already registered nonce",
            procExecutor.registerNonce(nonceKey) >= 0);
          t1NonceRegisteredLatch.countDown();

          // hold the submission until t2 is registering the nonce
          t2BeforeNonceRegisteredLatch.await();
          Threads.sleep(1000);

          if (submitProcedure) {
            CountDownLatch latch = new CountDownLatch(1);
            TestSingleStepProcedure proc = new TestSingleStepProcedure();
            procEnv.setWaitLatch(latch);

            procExecutor.submitProcedure(proc, nonceKey);
            Threads.sleep(100);

            // complete the procedure
            latch.countDown();
          } else {
            procExecutor.unregisterNonceIfProcedureWasNotSubmitted(nonceKey);
          }
        } catch (Throwable e) {
          t1Exception.set(e);
        } finally {
          t1NonceRegisteredLatch.countDown();
          t2BeforeNonceRegisteredLatch.countDown();
        }
      }
    };

    threads[1] = new Thread() {
      @Override
      public void run() {
        try {
          // wait until t1 has registered the nonce
          t1NonceRegisteredLatch.await();

          // register the nonce
          t2BeforeNonceRegisteredLatch.countDown();
          assertFalse("unexpected non registered nonce",
            procExecutor.registerNonce(nonceKey) < 0);
        } catch (Throwable e) {
          t2Exception.set(e);
        } finally {
          t1NonceRegisteredLatch.countDown();
          t2BeforeNonceRegisteredLatch.countDown();
        }
      }
    };

    for (int i = 0; i < threads.length; ++i) {
      threads[i].start();
    }

    for (int i = 0; i < threads.length; ++i) {
      Threads.shutdown(threads[i]);
    }

    ProcedureTestingUtility.waitNoProcedureRunning(procExecutor);
    assertEquals(null, t1Exception.get());
    assertEquals(null, t2Exception.get());
  }

  public static class TestSingleStepProcedure extends SequentialProcedure<TestProcEnv> {
    private int step = 0;

    public TestSingleStepProcedure() { }

    @Override
    protected Procedure[] execute(TestProcEnv env) throws InterruptedException {
      step++;
      env.waitOnLatch();
      LOG.debug("execute procedure " + this + " step=" + step);
      step++;
      setResult(Bytes.toBytes(step));
      return null;
    }

    @Override
    protected void rollback(TestProcEnv env) { }

    @Override
    protected boolean abort(TestProcEnv env) {
      return true;
    }
  }

  private static class TestProcEnv {
    private CountDownLatch latch = null;

    /**
     * set/unset a latch. every procedure execute() step will wait on the latch if any.
     */
    public void setWaitLatch(CountDownLatch latch) {
      this.latch = latch;
    }

    public void waitOnLatch() throws InterruptedException {
      if (latch != null) {
        latch.await();
      }
    }
  }
}
