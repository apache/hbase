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
package org.apache.hadoop.hbase.procedure2.store.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.LoadCounter;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.TestProcedure;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestStressWALProcedureStore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStressWALProcedureStore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestWALProcedureStore.class);

  private static final int PROCEDURE_STORE_SLOTS = 8;

  private WALProcedureStore procStore;

  private HBaseCommonTestingUtil htu;
  private FileSystem fs;
  private Path testDir;
  private Path logDir;

  private void setupConfiguration(Configuration conf) {
    conf.setBoolean(WALProcedureStore.USE_HSYNC_CONF_KEY, false);
    conf.setInt(WALProcedureStore.PERIODIC_ROLL_CONF_KEY, 5000);
    conf.setInt(WALProcedureStore.ROLL_THRESHOLD_CONF_KEY, 128 * 1024);
  }

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtil();
    setupConfiguration(htu.getConfiguration());

    testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(htu.getConfiguration());
    assertTrue(testDir.depth() > 1);

    logDir = new Path(testDir, "proc-logs");
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), logDir);
    procStore.start(PROCEDURE_STORE_SLOTS);
    procStore.recoverLease();

    LoadCounter loader = new LoadCounter();
    procStore.load(loader);
    assertEquals(0, loader.getMaxProcId());
    assertEquals(0, loader.getLoadedCount());
    assertEquals(0, loader.getCorruptedCount());
  }

  @After
  public void tearDown() throws IOException {
    procStore.stop(false);
    fs.delete(logDir, true);
  }

  @Test
  public void testInsertUpdateDelete() throws Exception {
    final long LAST_PROC_ID = 19999;
    final Thread[] thread = new Thread[PROCEDURE_STORE_SLOTS];
    final Random rand = ThreadLocalRandom.current();
    final AtomicLong procCounter = new AtomicLong(rand.nextInt(100));
    for (int i = 0; i < thread.length; ++i) {
      thread[i] = new Thread() {
        @Override
        public void run() {
          TestProcedure proc;
          do {
            // After HBASE-15579 there may be gap in the procId sequence, trying to simulate that.
            long procId = procCounter.addAndGet(1 + rand.nextInt(3));
            proc = new TestProcedure(procId);
            // Insert
            procStore.insert(proc, null);
            // Update
            for (int i = 0, nupdates = rand.nextInt(10); i <= nupdates; ++i) {
              try {
                Thread.sleep(0, rand.nextInt(15));
              } catch (InterruptedException e) {}
              procStore.update(proc);
            }
            // Delete
            procStore.delete(proc.getProcId());
          } while (proc.getProcId() < LAST_PROC_ID);
        }
      };
      thread[i].start();
    }

    for (int i = 0; i < thread.length; ++i) {
      thread[i].join();
    }

    procStore.getStoreTracker().dump();
    assertTrue(procCounter.get() >= LAST_PROC_ID);
    assertTrue(procStore.getStoreTracker().isEmpty());
    assertEquals(1, procStore.getActiveLogs().size());
  }

  @Ignore @Test // REENABLE after merge of
  // https://github.com/google/protobuf/issues/2228#issuecomment-252058282
  public void testEntrySizeLimit() throws Exception {
    final int NITEMS = 20;
    for (int i = 1; i <= NITEMS; ++i) {
      final byte[] data = new byte[256 << i];
      LOG.info(String.format("Writing %s", StringUtils.humanSize(data.length)));
      TestProcedure proc = new TestProcedure(i, 0, data);
      procStore.insert(proc, null);
    }

    // check that we are able to read the big proc-blobs
    ProcedureTestingUtility.storeRestartAndAssert(procStore, NITEMS, NITEMS, 0, 0);
  }
}
