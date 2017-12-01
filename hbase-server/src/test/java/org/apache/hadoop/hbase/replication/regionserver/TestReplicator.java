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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService.BlockingInterface;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.*;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.wal.WAL.Entry;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

@Category(MediumTests.class)
public class TestReplicator extends TestReplicationBase {

  static final Log LOG = LogFactory.getLog(TestReplicator.class);
  static final int NUM_ROWS = 10;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set RPC size limit to 10kb (will be applied to both source and sink clusters)
    conf1.setInt(RpcServer.MAX_REQUEST_SIZE, 1024 * 10);
    TestReplicationBase.setUpBeforeClass();
    admin.removePeer("2"); // Remove the peer set up for us by base class
  }

  @Test
  public void testReplicatorBatching() throws Exception {
    // Clear the tables
    truncateTable(utility1, tableName);
    truncateTable(utility2, tableName);

    // Replace the peer set up for us by the base class with a wrapper for this test
    admin.addPeer("testReplicatorBatching",
      new ReplicationPeerConfig().setClusterKey(utility2.getClusterKey())
        .setReplicationEndpointImpl(ReplicationEndpointForTest.class.getName()), null);

    ReplicationEndpointForTest.setBatchCount(0);
    ReplicationEndpointForTest.setEntriesCount(0);
    try {
      ReplicationEndpointForTest.pause();
      try {
        // Queue up a bunch of cells of size 8K. Because of RPC size limits, they will all
        // have to be replicated separately.
        final byte[] valueBytes = new byte[8 *1024];
        for (int i = 0; i < NUM_ROWS; i++) {
          htable1.put(new Put(("row"+Integer.toString(i)).getBytes())
            .addColumn(famName, null, valueBytes)
          );
        }
      } finally {
        ReplicationEndpointForTest.resume();
      }

      // Wait for replication to complete.
      // We can expect 10 batches, 1 row each
      Waiter.waitFor(conf1, 60000, new Waiter.ExplainingPredicate<Exception>() {
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
      assertEquals("We did not replicate enough rows", NUM_ROWS,
        utility2.countRows(htable2));
    } finally {
      admin.removePeer("testReplicatorBatching");
    }
  }

  @Test
  public void testReplicatorWithErrors() throws Exception {
    // Clear the tables
    truncateTable(utility1, tableName);
    truncateTable(utility2, tableName);

    // Replace the peer set up for us by the base class with a wrapper for this test
    admin.addPeer("testReplicatorWithErrors",
      new ReplicationPeerConfig().setClusterKey(utility2.getClusterKey())
          .setReplicationEndpointImpl(FailureInjectingReplicationEndpointForTest.class.getName()),
        null);

    FailureInjectingReplicationEndpointForTest.setBatchCount(0);
    FailureInjectingReplicationEndpointForTest.setEntriesCount(0);
    try {
      FailureInjectingReplicationEndpointForTest.pause();
      try {
        // Queue up a bunch of cells of size 8K. Because of RPC size limits, they will all
        // have to be replicated separately.
        final byte[] valueBytes = new byte[8 *1024];
        for (int i = 0; i < NUM_ROWS; i++) {
          htable1.put(new Put(("row"+Integer.toString(i)).getBytes())
            .addColumn(famName, null, valueBytes)
          );
        }
      } finally {
        FailureInjectingReplicationEndpointForTest.resume();
      }

      // Wait for replication to complete.
      // We can expect 10 batches
      Waiter.waitFor(conf1, 60000, new Waiter.ExplainingPredicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return FailureInjectingReplicationEndpointForTest.getEntriesCount() >= NUM_ROWS;
        }

        @Override
        public String explainFailure() throws Exception {
          return "We waited too long for expected replication of " + NUM_ROWS + " entries";
        }
      });

      assertEquals("We did not replicate enough rows", NUM_ROWS,
        utility2.countRows(htable2));
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

    private static AtomicInteger batchCount = new AtomicInteger(0);
    private static int entriesCount;
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
        synchronized(latch) {
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

    public class ReplicatorForTest extends Replicator {

      public ReplicatorForTest(List<Entry> entries, int ordinal) {
        super(entries, ordinal);
      }

      @Override
      protected void replicateEntries(BlockingInterface rrs, final List<Entry> entries,
          String replicationClusterId, Path baseNamespaceDir, Path hfileArchiveDir)
          throws IOException {
        try {
          long size = 0;
          for (Entry e: entries) {
            size += e.getKey().estimatedSerializedSizeOf();
            size += e.getEdit().estimatedSerializedSizeOf();
          }
          LOG.info("Replicating batch " + System.identityHashCode(entries) + " of " +
              entries.size() + " entries with total size " + size + " bytes to " +
              replicationClusterId);
          super.replicateEntries(rrs, entries, replicationClusterId, baseNamespaceDir,
            hfileArchiveDir);
          entriesCount += entries.size();
          int count = batchCount.incrementAndGet();
          LOG.info("Completed replicating batch " + System.identityHashCode(entries) +
              " count=" + count);
        } catch (IOException e) {
          LOG.info("Failed to replicate batch " + System.identityHashCode(entries), e);
          throw e;
        }
      }
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
    protected Replicator createReplicator(List<Entry> entries, int ordinal) {
      return new ReplicatorForTest(entries, ordinal);
    }
  }

  public static class FailureInjectingReplicationEndpointForTest
      extends ReplicationEndpointForTest {

    static class FailureInjectingBlockingInterface implements BlockingInterface {

      private final BlockingInterface delegate;
      private volatile boolean failNext;

      public FailureInjectingBlockingInterface(BlockingInterface delegate) {
        this.delegate = delegate;
      }

      @Override
      public ReplicateWALEntryResponse replicateWALEntry(RpcController controller,
          ReplicateWALEntryRequest request) throws ServiceException {
        if (!failNext) {
          failNext = true;
          return delegate.replicateWALEntry(controller, request);
        } else {
          failNext = false;
          throw new ServiceException("Injected failure");
        }
      }

      @Override
      public GetRegionInfoResponse getRegionInfo(RpcController controller,
          GetRegionInfoRequest request) throws ServiceException {
        return delegate.getRegionInfo(controller, request);
      }

      @Override
      public GetStoreFileResponse getStoreFile(RpcController controller,
          GetStoreFileRequest request) throws ServiceException {
        return delegate.getStoreFile(controller, request);
      }

      @Override
      public GetOnlineRegionResponse getOnlineRegion(RpcController controller,
          GetOnlineRegionRequest request) throws ServiceException {
        return delegate.getOnlineRegion(controller, request);
      }

      @Override
      public OpenRegionResponse openRegion(RpcController controller, OpenRegionRequest request)
          throws ServiceException {
        return delegate.openRegion(controller, request);
      }

      @Override
      public WarmupRegionResponse warmupRegion(RpcController controller,
          WarmupRegionRequest request) throws ServiceException {
        return delegate.warmupRegion(controller, request);
      }

      @Override
      public CloseRegionResponse closeRegion(RpcController controller, CloseRegionRequest request)
          throws ServiceException {
        return delegate.closeRegion(controller, request);
      }

      @Override
      public FlushRegionResponse flushRegion(RpcController controller, FlushRegionRequest request)
          throws ServiceException {
        return delegate.flushRegion(controller, request);
      }

      @Override
      public SplitRegionResponse splitRegion(RpcController controller, SplitRegionRequest request)
          throws ServiceException {
        return delegate.splitRegion(controller, request);
      }

      @Override
      public CompactRegionResponse compactRegion(RpcController controller,
          CompactRegionRequest request) throws ServiceException {
        return delegate.compactRegion(controller, request);
      }

      @Override
      public MergeRegionsResponse mergeRegions(RpcController controller,
          MergeRegionsRequest request) throws ServiceException {
        return delegate.mergeRegions(controller, request);
      }

      @Override
      public ReplicateWALEntryResponse replay(RpcController controller,
          ReplicateWALEntryRequest request) throws ServiceException {
        return delegate.replay(controller, request);
      }

      @Override
      public RollWALWriterResponse rollWALWriter(RpcController controller,
          RollWALWriterRequest request) throws ServiceException {
        return delegate.rollWALWriter(controller, request);
      }

      @Override
      public GetServerInfoResponse getServerInfo(RpcController controller,
          GetServerInfoRequest request) throws ServiceException {
        return delegate.getServerInfo(controller, request);
      }

      @Override
      public StopServerResponse stopServer(RpcController controller, StopServerRequest request)
          throws ServiceException {
        return delegate.stopServer(controller, request);
      }

      @Override
      public UpdateFavoredNodesResponse updateFavoredNodes(RpcController controller,
          UpdateFavoredNodesRequest request) throws ServiceException {
        return delegate.updateFavoredNodes(controller, request);
      }

      @Override
      public UpdateConfigurationResponse updateConfiguration(RpcController controller,
          UpdateConfigurationRequest request) throws ServiceException {
        return delegate.updateConfiguration(controller, request);
      }
    }

    public class FailureInjectingReplicatorForTest extends ReplicatorForTest {

      public FailureInjectingReplicatorForTest(List<Entry> entries, int ordinal) {
        super(entries, ordinal);
      }

      @Override
      protected void replicateEntries(BlockingInterface rrs, List<Entry> entries,
          String replicationClusterId, Path baseNamespaceDir, Path hfileArchiveDir)
          throws IOException {
        super.replicateEntries(new FailureInjectingBlockingInterface(rrs), entries,
          replicationClusterId, baseNamespaceDir, hfileArchiveDir);
      }
    }

    @Override
    protected Replicator createReplicator(List<Entry> entries, int ordinal) {
      return new FailureInjectingReplicatorForTest(entries, ordinal);
    }
  }

}
