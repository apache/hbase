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

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.LoadCounter;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.TestProcedure;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({MasterTests.class, LargeTests.class})
public class TestStressWALProcedureStore {
  private static final Log LOG = LogFactory.getLog(TestWALProcedureStore.class);

  private static final int PROCEDURE_STORE_SLOTS = 8;

  private WALProcedureStore procStore;

  private HBaseCommonTestingUtility htu;
  private FileSystem fs;
  private Path testDir;
  private Path logDir;

  private void setupConfiguration(Configuration conf) {
    conf.setBoolean("hbase.procedure.store.wal.use.hsync", false);
    conf.setInt("hbase.procedure.store.wal.periodic.roll.msec", 5000);
    conf.setInt("hbase.procedure.store.wal.roll.threshold", 128 * 1024);
  }

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    setupConfiguration(htu.getConfiguration());

    testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(htu.getConfiguration());
    assertTrue(testDir.depth() > 1);

    logDir = new Path(testDir, "proc-logs");
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), fs, logDir);
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
    final AtomicLong procCounter = new AtomicLong((long)Math.round(Math.random() * 100));
    for (int i = 0; i < thread.length; ++i) {
      thread[i] = new Thread() {
        @Override
        public void run() {
          Random rand = new Random();
          TestProcedure proc;
          do {
            proc = new TestProcedure(procCounter.addAndGet(1));
            // Insert
            procStore.insert(proc, null);
            // Update
            for (int i = 0, nupdates = rand.nextInt(10); i <= nupdates; ++i) {
              try { Thread.sleep(0, rand.nextInt(15)); } catch (InterruptedException e) {}
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
}
