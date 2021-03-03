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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

@Category(MediumTests.class)
public class TestReplicator extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicator.class);

  static final Logger LOG = LoggerFactory.getLogger(TestReplicator.class);
  static final int NUM_ROWS = 10;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set RPC size limit to 10kb (will be applied to both source and sink clusters)
    CONF1.setInt(RpcServer.MAX_REQUEST_SIZE, 1024 * 10);
    TestReplicationBase.setUpBeforeClass();
  }

  @Test
  public void testReplicatorBatching() throws Exception {
    // Clear the tables
    truncateTable(UTIL1, tableName);
    truncateTable(UTIL2, tableName);

    // Replace the peer set up for us by the base class with a wrapper for this test
    admin.addPeer("testReplicatorBatching",
      new ReplicationPeerConfig().setClusterKey(UTIL2.getClusterKey())
          .setReplicationEndpointImpl(ReplicationEndpointForTest.class.getName()),
      null);

    ReplicationEndpointForTest.setBatchCount(0);
    ReplicationEndpointForTest.setEntriesCount(0);
    try {
      ReplicationEndpointForTest.pause();
      try {
        // Queue up a bunch of cells of size 8K. Because of RPC size limits, they will all
        // have to be replicated separately.
        final byte[] valueBytes = new byte[8 * 1024];
        for (int i = 0; i < NUM_ROWS; i++) {
          htable1.put(new Put(Bytes.toBytes("row" + Integer.toString(i))).addColumn(famName, null,
            valueBytes));
        }
      } finally {
        ReplicationEndpointForTest.resume();
      }

      // Wait for replication to complete.
      Waiter.waitFor(CONF1, 60000, new Waiter.ExplainingPredicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          LOG.info("Count=" + ReplicationEndpointForTest.getBatchCount());
          return ReplicationEndpointForTest.getBatchCount() >= NUM_ROWS;
        }

        @Override
        public String explainFailure() throws Exception {
          return "We waited too long for expected replication of " + NUM_ROWS + " entries";
        }
      });

      assertEquals("We sent an incorrect number of batches", NUM_ROWS,
        ReplicationEndpointForTest.getBatchCount());
      assertEquals("We did not replicate enough rows", NUM_ROWS, UTIL2.countRows(htable2));
    } finally {
      admin.removePeer("testReplicatorBatching");
    }
  }

  @Test
  public void testReplicatorWithErrors() throws Exception {
    // Clear the tables
    truncateTable(UTIL1, tableName);
    truncateTable(UTIL2, tableName);

    // Replace the peer set up for us by the base class with a wrapper for this test
    admin.addPeer("testReplicatorWithErrors",
      new ReplicationPeerConfig().setClusterKey(UTIL2.getClusterKey())
          .setReplicationEndpointImpl(FailureInjectingReplicationEndpointForTest.class.getName()),
      null);

    FailureInjectingReplicationEndpointForTest.setBatchCount(0);
    FailureInjectingReplicationEndpointForTest.setEntriesCount(0);
    try {
      FailureInjectingReplicationEndpointForTest.pause();
      try {
        // Queue up a bunch of cells of size 8K. Because of RPC size limits, they will all
        // have to be replicated separately.
        final byte[] valueBytes = new byte[8 * 1024];
        for (int i = 0; i < NUM_ROWS; i++) {
          htable1.put(new Put(Bytes.toBytes("row" + Integer.toString(i))).addColumn(famName, null,
            valueBytes));
        }
      } finally {
        FailureInjectingReplicationEndpointForTest.resume();
      }

      // Wait for replication to complete.
      // We can expect 10 batches
      Waiter.waitFor(CONF1, 60000, new Waiter.ExplainingPredicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return FailureInjectingReplicationEndpointForTest.getEntriesCount() >= NUM_ROWS;
        }

        @Override
        public String explainFailure() throws Exception {
          return "We waited too long for expected replication of " + NUM_ROWS + " entries";
        }
      });

      assertEquals("We did not replicate enough rows", NUM_ROWS, UTIL2.countRows(htable2));
    } finally {
      admin.removePeer("testReplicatorWithErrors");
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TestReplicationBase.tearDownAfterClass();
  }

  private void truncateTable(HBaseTestingUtility util, TableName tablename) throws IOException {
    HBaseAdmin admin = util.getHBaseAdmin();
    admin.disableTable(tableName);
    admin.truncateTable(tablename, false);
  }

  public static class ReplicationEndpointForTest extends HBaseInterClusterReplicationEndpoint {

    protected static AtomicInteger batchCount = new AtomicInteger(0);
    protected static int entriesCount;
    private static final Object latch = new Object();
    private static AtomicBoolean useLatch = new AtomicBoolean(false);

    public static void resume() {
      useLatch.set(false);
      synchronized (latch) {
        latch.notifyAll();
      }
    }

    public static void pause() {
      useLatch.set(true);
    }

    public static void await() throws InterruptedException {
      if (useLatch.get()) {
        LOG.info("Waiting on latch");
        synchronized (latch) {
          latch.wait();
        }
        LOG.info("Waited on latch, now proceeding");
      }
    }

    public static int getBatchCount() {
      return batchCount.get();
    }

    public static void setBatchCount(int i) {
      LOG.info("SetBatchCount=" + i + ", old=" + getBatchCount());
      batchCount.set(i);
    }

    public static int getEntriesCount() {
      return entriesCount;
    }

    public static void setEntriesCount(int i) {
      LOG.info("SetEntriesCount=" + i);
      entriesCount = i;
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      try {
        await();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted waiting for latch", e);
      }
      return super.replicate(replicateContext);
    }

    @Override
    protected Callable<Integer> createReplicator(List<Entry> entries, int ordinal, int timeout) {
      return () -> {
        int batchIndex = replicateEntries(entries, ordinal, timeout);
        entriesCount += entries.size();
        int count = batchCount.incrementAndGet();
        LOG.info(
          "Completed replicating batch " + System.identityHashCode(entries) + " count=" + count);
        return batchIndex;
      };
    }
  }

  public static class FailureInjectingReplicationEndpointForTest
      extends ReplicationEndpointForTest {
    private final AtomicBoolean failNext = new AtomicBoolean(false);

    @Override
    protected Callable<Integer> createReplicator(List<Entry> entries, int ordinal, int timeout) {
      return () -> {
        if (failNext.compareAndSet(false, true)) {
          int batchIndex = replicateEntries(entries, ordinal, timeout);
          entriesCount += entries.size();
          int count = batchCount.incrementAndGet();
          LOG.info(
            "Completed replicating batch " + System.identityHashCode(entries) + " count=" + count);
          return batchIndex;
        } else if (failNext.compareAndSet(true, false)) {
          throw new ServiceException("Injected failure");
        }
        return ordinal;
      };
    }
  }
}
