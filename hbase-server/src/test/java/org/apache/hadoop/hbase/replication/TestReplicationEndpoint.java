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

package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests ReplicationSource and ReplicationEndpoint interactions
 */
@Category(MediumTests.class)
public class TestReplicationEndpoint extends TestReplicationBase {
  static final Log LOG = LogFactory.getLog(TestReplicationEndpoint.class);

  static int numRegionServers;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestReplicationBase.setUpBeforeClass();
    utility2.shutdownMiniCluster(); // we don't need the second cluster
    admin.removePeer("2");
    numRegionServers = utility1.getHBaseCluster().getRegionServerThreads().size();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TestReplicationBase.tearDownAfterClass();
    // check stop is called
    Assert.assertTrue(ReplicationEndpointForTest.stoppedCount.get() > 0);
  }

  @Before
  public void setup() throws FailedLogCloseException, IOException {
    ReplicationEndpointForTest.contructedCount.set(0);
    ReplicationEndpointForTest.startedCount.set(0);
    ReplicationEndpointForTest.replicateCount.set(0);
    ReplicationEndpointReturningFalse.replicated.set(false);
    ReplicationEndpointForTest.lastEntries = null;
    for (RegionServerThread rs : utility1.getMiniHBaseCluster().getRegionServerThreads()) {
      utility1.getHBaseAdmin().rollHLogWriter(rs.getRegionServer().getServerName().toString());
    }
  }

  @Test (timeout=120000)
  public void testCustomReplicationEndpoint() throws Exception {
    // test installing a custom replication endpoint other than the default one.
    admin.addPeer("testCustomReplicationEndpoint",
      new ReplicationPeerConfig().setClusterKey(ZKUtil.getZooKeeperClusterKey(conf1))
        .setReplicationEndpointImpl(ReplicationEndpointForTest.class.getName()), null);

    // check whether the class has been constructed and started
    Waiter.waitFor(conf1, 60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return ReplicationEndpointForTest.contructedCount.get() >= numRegionServers;
      }
    });

    Waiter.waitFor(conf1, 60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return ReplicationEndpointForTest.startedCount.get() >= numRegionServers;
      }
    });

    Assert.assertEquals(0, ReplicationEndpointForTest.replicateCount.get());

    // now replicate some data.
    doPut(Bytes.toBytes("row42"));

    Waiter.waitFor(conf1, 60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return ReplicationEndpointForTest.replicateCount.get() >= 1;
      }
    });

    doAssert(Bytes.toBytes("row42"));

    admin.removePeer("testCustomReplicationEndpoint");
  }

  @Test (timeout=120000)
  public void testReplicationEndpointReturnsFalseOnReplicate() throws Exception {
    Assert.assertEquals(0, ReplicationEndpointForTest.replicateCount.get());
    Assert.assertTrue(!ReplicationEndpointReturningFalse.replicated.get());
    int peerCount = admin.getPeersCount();
    final String id = "testReplicationEndpointReturnsFalseOnReplicate";
    admin.addPeer(id,
      new ReplicationPeerConfig().setClusterKey(ZKUtil.getZooKeeperClusterKey(conf1))
        .setReplicationEndpointImpl(ReplicationEndpointReturningFalse.class.getName()), null);
    // This test is flakey and then there is so much stuff flying around in here its, hard to
    // debug.  Peer needs to be up for the edit to make it across. This wait on
    // peer count seems to be a hack that has us not progress till peer is up.
    if (admin.getPeersCount() <= peerCount) {
      LOG.info("Waiting on peercount to go up from " + peerCount);
      Threads.sleep(100);
    }
    // now replicate some data
    doPut(row);

    Waiter.waitFor(conf1, 60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        // Looks like replication endpoint returns false unless we put more than 10 edits. We
        // only send over one edit.
        int count = ReplicationEndpointForTest.replicateCount.get();
        LOG.info("count=" + count);
        return ReplicationEndpointReturningFalse.replicated.get();
      }
    });
    if (ReplicationEndpointReturningFalse.ex.get() != null) {
      throw ReplicationEndpointReturningFalse.ex.get();
    }

    admin.removePeer("testReplicationEndpointReturnsFalseOnReplicate");
  }

  @Test (timeout=120000)
  public void testWALEntryFilterFromReplicationEndpoint() throws Exception {
    admin.addPeer("testWALEntryFilterFromReplicationEndpoint",
      new ReplicationPeerConfig().setClusterKey(ZKUtil.getZooKeeperClusterKey(conf1))
        .setReplicationEndpointImpl(ReplicationEndpointWithWALEntryFilter.class.getName()), null);
    // now replicate some data.
    try (Connection connection = ConnectionFactory.createConnection(conf1)) {
      doPut(connection, Bytes.toBytes("row1"));
      doPut(connection, row);
      doPut(connection, Bytes.toBytes("row2"));
    }

    Waiter.waitFor(conf1, 60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return ReplicationEndpointForTest.replicateCount.get() >= 1;
      }
    });

    Assert.assertNull(ReplicationEndpointWithWALEntryFilter.ex.get());
    admin.removePeer("testWALEntryFilterFromReplicationEndpoint");
  }


  private void doPut(byte[] row) throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(conf1)) {
      doPut(connection, row);
    }
  }

  private void doPut(final Connection connection, final byte [] row) throws IOException {
    try (Table t = connection.getTable(tableName)) {
      Put put = new Put(row);
      put.add(famName, row, row);
      t.put(put);
    }
  }

  private static void doAssert(byte[] row) throws Exception {
    if (ReplicationEndpointForTest.lastEntries == null) {
      return; // first call
    }
    Assert.assertEquals(1, ReplicationEndpointForTest.lastEntries.size());
    List<Cell> cells = ReplicationEndpointForTest.lastEntries.get(0).getEdit().getCells();
    Assert.assertEquals(1, cells.size());
    Assert.assertTrue(Bytes.equals(cells.get(0).getRowArray(), cells.get(0).getRowOffset(),
      cells.get(0).getRowLength(), row, 0, row.length));
  }

  public static class ReplicationEndpointForTest extends BaseReplicationEndpoint {
    static UUID uuid = UUID.randomUUID();
    static AtomicInteger contructedCount = new AtomicInteger();
    static AtomicInteger startedCount = new AtomicInteger();
    static AtomicInteger stoppedCount = new AtomicInteger();
    static AtomicInteger replicateCount = new AtomicInteger();
    static volatile List<HLog.Entry> lastEntries = null;

    public ReplicationEndpointForTest() {
      contructedCount.incrementAndGet();
    }

    @Override
    public UUID getPeerUUID() {
      return uuid;
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      replicateCount.incrementAndGet();
      lastEntries = replicateContext.entries;
      return true;
    }

    @Override
    protected void doStart() {
      startedCount.incrementAndGet();
      notifyStarted();
    }

    @Override
    protected void doStop() {
      stoppedCount.incrementAndGet();
      notifyStopped();
    }
  }

  public static class ReplicationEndpointReturningFalse extends ReplicationEndpointForTest {
    static int COUNT = 10;
    static AtomicReference<Exception> ex = new AtomicReference<Exception>(null);
    static AtomicBoolean replicated = new AtomicBoolean(false);
    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      try {
        // check row
        doAssert(row);
      } catch (Exception e) {
        ex.set(e);
      }

      super.replicate(replicateContext);
      LOG.info("Replicated " + row + ", count=" + replicateCount.get());

      replicated.set(replicateCount.get() > COUNT); // first 10 times, we return false
      return replicated.get();
    }
  }

  // return a WALEntry filter which only accepts "row", but not other rows
  public static class ReplicationEndpointWithWALEntryFilter extends ReplicationEndpointForTest {
    static AtomicReference<Exception> ex = new AtomicReference<Exception>(null);

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      try {
        super.replicate(replicateContext);
        doAssert(row);
      } catch (Exception e) {
        ex.set(e);
      }
      return true;
    }

    @Override
    public WALEntryFilter getWALEntryfilter() {
      return new ChainWALEntryFilter(super.getWALEntryfilter(), new WALEntryFilter() {
        @Override
        public Entry filter(Entry entry) {
          ArrayList<Cell> cells = entry.getEdit().getCells();
          int size = cells.size();
          for (int i = size-1; i >= 0; i--) {
            Cell cell = cells.get(i);
            if (!Bytes.equals(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
              row, 0, row.length)) {
              cells.remove(i);
            }
          }
          return entry;
        }
      });
    }
  }
}
