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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.TestProcedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, LargeTests.class})
public class TestWALProcedureStoreOnHDFS {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWALProcedureStoreOnHDFS.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestWALProcedureStoreOnHDFS.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private WALProcedureStore store;

  private ProcedureStore.ProcedureStoreListener stopProcedureListener = new ProcedureStore.ProcedureStoreListener() {
    @Override
    public void postSync() {}

    @Override
    public void abortProcess() {
      LOG.error(HBaseMarkers.FATAL, "Abort the Procedure Store");
      store.stop(true);
    }
  };

  @Before
  public void initConfig() {
    Configuration conf = UTIL.getConfiguration();

    conf.setInt("dfs.replication", 3);
    conf.setInt("dfs.namenode.replication.min", 3);

    // increase the value for slow test-env
    conf.setInt(WALProcedureStore.WAIT_BEFORE_ROLL_CONF_KEY, 1000);
    conf.setInt(WALProcedureStore.ROLL_RETRIES_CONF_KEY, 10);
    conf.setInt(WALProcedureStore.MAX_SYNC_FAILURE_ROLL_CONF_KEY, 10);
  }

  // No @Before because some tests need to do additional config first
  private void setupDFS() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    MiniDFSCluster dfs = UTIL.startMiniDFSCluster(3);
    CommonFSUtils.setWALRootDir(conf, new Path(conf.get("fs.defaultFS"), "/tmp/wal"));

    Path logDir = new Path(new Path(dfs.getFileSystem().getUri()), "/test-logs");
    store = ProcedureTestingUtility.createWalStore(conf, logDir);
    store.registerListener(stopProcedureListener);
    store.start(8);
    store.recoverLease();
  }

  // No @After
  @SuppressWarnings("JUnit4TearDownNotRun")
  public void tearDown() throws Exception {
    store.stop(false);
    UTIL.getDFSCluster().getFileSystem().delete(store.getWALDir(), true);

    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Test(expected=RuntimeException.class)
  public void testWalAbortOnLowReplication() throws Exception {
    setupDFS();

    assertEquals(3, UTIL.getDFSCluster().getDataNodes().size());

    LOG.info("Stop DataNode");
    UTIL.getDFSCluster().stopDataNode(0);
    assertEquals(2, UTIL.getDFSCluster().getDataNodes().size());

    store.insert(new TestProcedure(1, -1), null);
    for (long i = 2; store.isRunning(); ++i) {
      assertEquals(2, UTIL.getDFSCluster().getDataNodes().size());
      store.insert(new TestProcedure(i, -1), null);
      Thread.sleep(100);
    }
    assertFalse(store.isRunning());
  }

  @Test
  public void testWalAbortOnLowReplicationWithQueuedWriters() throws Exception {
    setupDFS();

    assertEquals(3, UTIL.getDFSCluster().getDataNodes().size());
    store.registerListener(new ProcedureStore.ProcedureStoreListener() {
      @Override
      public void postSync() { Threads.sleepWithoutInterrupt(2000); }

      @Override
      public void abortProcess() {}
    });

    final AtomicInteger reCount = new AtomicInteger(0);
    Thread[] thread = new Thread[store.getNumThreads() * 2 + 1];
    for (int i = 0; i < thread.length; ++i) {
      final long procId = i + 1L;
      thread[i] = new Thread(() -> {
        try {
          LOG.debug("[S] INSERT " + procId);
          store.insert(new TestProcedure(procId, -1), null);
          LOG.debug("[E] INSERT " + procId);
        } catch (RuntimeException e) {
          reCount.incrementAndGet();
          LOG.debug("[F] INSERT " + procId + ": " + e.getMessage());
        }
      });
      thread[i].start();
    }

    Thread.sleep(1000);
    LOG.info("Stop DataNode");
    UTIL.getDFSCluster().stopDataNode(0);
    assertEquals(2, UTIL.getDFSCluster().getDataNodes().size());

    for (int i = 0; i < thread.length; ++i) {
      thread[i].join();
    }

    assertFalse(store.isRunning());
    assertTrue(reCount.toString(), reCount.get() >= store.getNumThreads() &&
                                   reCount.get() < thread.length);
  }

  @Test
  public void testWalRollOnLowReplication() throws Exception {
    UTIL.getConfiguration().setInt("dfs.namenode.replication.min", 1);
    setupDFS();

    int dnCount = 0;
    store.insert(new TestProcedure(1, -1), null);
    UTIL.getDFSCluster().restartDataNode(dnCount);
    for (long i = 2; i < 100; ++i) {
      store.insert(new TestProcedure(i, -1), null);
      waitForNumReplicas(3);
      Thread.sleep(100);
      if ((i % 30) == 0) {
        LOG.info("Restart Data Node");
        UTIL.getDFSCluster().restartDataNode(++dnCount % 3);
      }
    }
    assertTrue(store.isRunning());
  }

  public void waitForNumReplicas(int numReplicas) throws Exception {
    while (UTIL.getDFSCluster().getDataNodes().size() < numReplicas) {
      Thread.sleep(100);
    }

    for (int i = 0; i < numReplicas; ++i) {
      for (DataNode dn: UTIL.getDFSCluster().getDataNodes()) {
        while (!dn.isDatanodeFullyStarted()) {
          Thread.sleep(100);
        }
      }
    }
  }
}
