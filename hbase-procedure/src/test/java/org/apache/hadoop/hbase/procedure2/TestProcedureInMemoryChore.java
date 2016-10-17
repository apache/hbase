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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.NoopProcedureStore;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, SmallTests.class})
public class TestProcedureInMemoryChore {
  private static final Log LOG = LogFactory.getLog(TestProcedureInMemoryChore.class);

  private static final int PROCEDURE_EXECUTOR_SLOTS = 1;

  private TestProcEnv procEnv;
  private NoopProcedureStore procStore;
  private ProcedureExecutor<TestProcEnv> procExecutor;

  private HBaseCommonTestingUtility htu;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();

    procEnv = new TestProcEnv();
    procStore = new NoopProcedureStore();
    procExecutor = new ProcedureExecutor(htu.getConfiguration(), procEnv, procStore);
    procExecutor.testing = new ProcedureExecutor.Testing();
    procStore.start(PROCEDURE_EXECUTOR_SLOTS);
    procExecutor.start(PROCEDURE_EXECUTOR_SLOTS, true);
  }

  @After
  public void tearDown() throws IOException {
    procExecutor.stop();
    procStore.stop(false);
  }

  @Test
  public void testChoreAddAndRemove() throws Exception {
    final int timeoutMSec = 50;
    final int nCountDown = 5;

    // submit the chore and wait for execution
    CountDownLatch latch = new CountDownLatch(nCountDown);
    TestLatchChore chore = new TestLatchChore(timeoutMSec, latch);
    procExecutor.addChore(chore);
    assertTrue(chore.isWaiting());
    latch.await();

    // remove the chore and verify it is no longer executed
    assertTrue(chore.isWaiting());
    procExecutor.removeChore(chore);
    latch = new CountDownLatch(nCountDown);
    chore.setLatch(latch);
    latch.await(timeoutMSec * nCountDown, TimeUnit.MILLISECONDS);
    LOG.info("chore latch count=" + latch.getCount());
    assertFalse(chore.isWaiting());
    assertTrue("latchCount=" + latch.getCount(), latch.getCount() > 0);
  }

  public static class TestLatchChore extends ProcedureInMemoryChore<TestProcEnv> {
    private CountDownLatch latch;

    public TestLatchChore(final int timeoutMSec, final CountDownLatch latch) {
      super(timeoutMSec);
      setLatch(latch);
    }

    public void setLatch(final CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    protected void periodicExecute(final TestProcEnv env) {
      LOG.info("periodic execute " + this);
      latch.countDown();
    }
  }

  private static class TestProcEnv {
  }
}
