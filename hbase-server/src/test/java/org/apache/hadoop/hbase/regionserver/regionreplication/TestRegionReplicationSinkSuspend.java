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
package org.apache.hadoop.hbase.regionserver.regionreplication;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.regionreplication.RegionReplicationSink.SinkEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionReplicationSinkSuspend {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionReplicationSinkSuspend.class);

  private static final byte[] FAMILY = Bytes.toBytes("family_test");

  private static final byte[] QUAL = Bytes.toBytes("qualifier_test");

  private static final HBaseTestingUtil HTU = new HBaseTestingUtil();
  private static final int NB_SERVERS = 2;

  private static TableName tableName = TableName.valueOf("testRegionReplicationSinkSuspend");
  private static volatile boolean startTest = false;
  private static volatile boolean initialFlush = false;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = HTU.getConfiguration();
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    conf.setClass(HConstants.REGION_IMPL, HRegionForTest.class, HRegion.class);
    conf.setInt(RegionReplicationSink.RETRIES_NUMBER, 15);
    conf.setLong(RegionReplicationSink.RPC_TIMEOUT_MS, 10 * 60 * 1000);
    conf.setLong(RegionReplicationSink.OPERATION_TIMEOUT_MS, 20 * 60 * 1000);
    conf.setLong(RegionReplicationSink.META_EDIT_RPC_TIMEOUT_MS, 10 * 60 * 1000);
    conf.setLong(RegionReplicationSink.META_EDIT_OPERATION_TIMEOUT_MS, 20 * 60 * 1000);
    conf.setBoolean(RegionReplicaUtil.REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH_CONF_KEY, false);
    HTU.startMiniCluster(StartTestingClusterOption.builder().rsClass(RSForTest.class)
        .numRegionServers(NB_SERVERS).build());

  }

  @AfterClass
  public static void tearDown() throws Exception {
    HTU.shutdownMiniCluster();
  }

  /**
   * This test is for HBASE-26768
   */
  @Test
  public void test() throws Exception {
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
        .setRegionReplication(NB_SERVERS).setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setRegionMemStoreReplication(true).build();
    HTU.getAdmin().createTable(tableDescriptor);
    final HRegionForTest[] regions = new HRegionForTest[NB_SERVERS];
    for (int i = 0; i < NB_SERVERS; i++) {
      HRegionServer rs = HTU.getMiniHBaseCluster().getRegionServer(i);
      List<HRegion> onlineRegions = rs.getRegions(tableName);
      for (HRegion region : onlineRegions) {
        int replicaId = region.getRegionInfo().getReplicaId();
        assertTrue(regions[replicaId] == null);
        regions[region.getRegionInfo().getReplicaId()] = (HRegionForTest) region;
      }
    }
    for (Region region : regions) {
      assertNotNull(region);
    }

    final AtomicInteger sinkCheckFailedReplicaAndSendCounter = new AtomicInteger(0);
    RegionReplicationSink regionReplicationSink =
        regions[0].getRegionReplicationSink().get();
    assertTrue(regionReplicationSink != null);
    RegionReplicationSink spiedRegionReplicationSink = Mockito.spy(regionReplicationSink);
    AtomicReference<Throwable> catchedThrowableRef = new AtomicReference<Throwable>(null);
    Mockito.doAnswer((invocationOnMock) -> {
      try {
        if (!startTest) {
          invocationOnMock.callRealMethod();
          return null;
        }
        int count = sinkCheckFailedReplicaAndSendCounter.incrementAndGet();
        if (count == 1) {
          RegionReplicationSink currentSink = (RegionReplicationSink) invocationOnMock.getMock();
          assertTrue(currentSink.getFailedReplicas().size() == 0);
          @SuppressWarnings("unchecked")
          Set<Integer> inputFailedReplicas = (Set<Integer>) invocationOnMock.getArgument(0);
          assertTrue(inputFailedReplicas.size() == 1);
          /**
           * Wait {@link RegionReplicationSink#add} for {@link FlushAction#START_FLUSH} starting,
           * Only before entering {@link RegionReplicationSink#checkFailedReplicaAndSend},
           * {@link RegionReplicationSink#add} for {@link FlushAction#START_FLUSH} could starting.
           */
          regions[0].cyclicBarrier.await();
          /**
           * Wait {@link RegionReplicationSink#add} for {@link FlushAction#START_FLUSH} ending.
           * After {@link RegionReplicationSink#add} for {@link FlushAction#START_FLUSH} ending,
           * {@link RegionReplicationSink#checkFailedReplicaAndSend} could enter its method to
           * execute.
           */
          regions[0].cyclicBarrier.await();
          assertTrue(currentSink.getFailedReplicas().isEmpty());

          invocationOnMock.callRealMethod();
          assertTrue(currentSink.getFailedReplicas().isEmpty());
          assertTrue(currentSink.getFlushRequester().getPendingFlushRequest() == null);
          /**
           * After {@link RegionReplicationSink#checkFailedReplicaAndSend} ending, MemStore Flushing
           * could commit flush
           */
          regions[0].cyclicBarrier.await();
          return null;
        }
        invocationOnMock.callRealMethod();
        return null;
      } catch (Throwable throwable) {
        catchedThrowableRef.set(throwable);
      }
      return null;
    }).when(spiedRegionReplicationSink).checkFailedReplicaAndSend(
      Mockito.anySet(),
      Mockito.anyLong(),
      Mockito.anyLong());

    Mockito.doAnswer((invocationOnMock) -> {
      if (!startTest) {
        invocationOnMock.callRealMethod();
        return null;
      }
      if (regions[0].prepareFlush
          && Thread.currentThread().getName().equals(HRegionForTest.USER_THREAD_NAME)) {
        /**
         * Only before entering {@link RegionReplicationSink#checkFailedReplicaAndSend},
         * {@link RegionReplicationSink#add} for {@link FlushAction#START_FLUSH} could starting.
         */
        regions[0].cyclicBarrier.await();
        invocationOnMock.callRealMethod();
        /**
         * After {@link RegionReplicationSink#add} for {@link FlushAction#START_FLUSH} ending,
         * {@link RegionReplicationSink#checkFailedReplicaAndSend} could enter its method to
         * execute.
         */
        regions[0].cyclicBarrier.await();
        /**
         * Wait {@link RegionReplicationSink#checkFailedReplicaAndSend} ending. After
         * {@link RegionReplicationSink#checkFailedReplicaAndSend} ending, MemStore Flushing could
         * commit flush.
         */
        regions[0].cyclicBarrier.await();
        return null;
      }
      invocationOnMock.callRealMethod();
      return null;
    }).when(spiedRegionReplicationSink).add(Mockito.any(), Mockito.any(), Mockito.any());

    final AtomicInteger sinkSendCounter = new AtomicInteger(0);
    Mockito.doAnswer((invocationOnMock) -> {
      if (startTest) {
        sinkSendCounter.incrementAndGet();
      }
      invocationOnMock.callRealMethod();
      return null;
    }).when(spiedRegionReplicationSink).send();

    regions[0].setRegionReplicationSink(spiedRegionReplicationSink);

    String oldThreadName = Thread.currentThread().getName();
    Thread.currentThread().setName(HRegionForTest.USER_THREAD_NAME);
    try {
      initialFlush = true;
      spiedRegionReplicationSink.getFlushRequester().requestFlush(-1);
      regions[0].flushMemStoreCyclicBarrierBeforeStartTest.await();
      HTU.waitFor(120000, () -> !spiedRegionReplicationSink.isSending());
      assertTrue(spiedRegionReplicationSink.getFlushRequester().getPendingFlushRequest() == null);
      long initialRequestNanos =
          spiedRegionReplicationSink.getFlushRequester().getLastRequestNanos();
      assertTrue(initialRequestNanos > 0);
      initialFlush = false;
      startTest = true;
      /**
       * Write First cell,replicating to secondary replica is error.
       * {@link RegionReplicationSink#checkFailedReplicaAndSend} for this cell would wait for
       * {@link RegionReplicationSink#add} for {@link FlushAction#START_FLUSH} ending.
       */
      regions[0].put(new Put(Bytes.toBytes(1)).addColumn(FAMILY, QUAL, Bytes.toBytes(1)));

      /**
       * For {@link FlushAction#START_FLUSH}, after {@link RegionReplicationSink#add} is
       * ending,{@link RegionReplicationSink#checkFailedReplicaAndSend} could execute,
       * {@link FlushAction#START_FLUSH} is in {@link RegionReplicationSink#entries},and is sent by
       * {@link RegionReplicationSink#checkFailedReplicaAndSend},After
       * {@link RegionReplicationSink#checkFailedReplicaAndSend} ending, MemStore Flushing could
       * commit flush.<br/>
       * For {@link FlushAction#COMMIT_FLUSH},it is directly sent.
       */
      regions[0].flushcache(true, true, FlushLifeCycleTracker.DUMMY);

      HTU.waitFor(120000, () -> !spiedRegionReplicationSink.isSending());
      /**
       * No failed replicas.
       */
      assertTrue(spiedRegionReplicationSink.getFailedReplicas().isEmpty());
      /**
       * No flush requester.
       */
      assertTrue(spiedRegionReplicationSink.getFlushRequester().getPendingFlushRequest() == null);
      assertTrue(spiedRegionReplicationSink.getFlushRequester()
          .getLastRequestNanos() == initialRequestNanos);
      Queue<SinkEntry> sinkEntrys = spiedRegionReplicationSink.getEntries();
      assertTrue(sinkEntrys.size() == 0);
      assertTrue(!spiedRegionReplicationSink.isSending());
      /**
       * send key1,{@link FlushAction#START_FLUSH} and {@link FlushAction#COMMIT_FLUSH}
       */
      assertTrue(sinkSendCounter.get() == 3);

      regions[0].put(new Put(Bytes.toBytes(2)).addColumn(FAMILY, QUAL, Bytes.toBytes(2)));

      HTU.waitFor(1200000, () -> !spiedRegionReplicationSink.isSending());
      assertTrue(spiedRegionReplicationSink.getFlushRequester().getPendingFlushRequest() == null);
      assertTrue(spiedRegionReplicationSink.getFlushRequester()
          .getLastRequestNanos() == initialRequestNanos);
      assertTrue(sinkSendCounter.get() == 4);
      assertTrue(sinkEntrys.size() == 0);
      assertTrue(catchedThrowableRef.get() == null);
    } finally {
      startTest = false;
      Thread.currentThread().setName(oldThreadName);
    }
  }


  public static final class HRegionForTest extends HRegion {
    static final String USER_THREAD_NAME = "TestReplicationHang";
    final CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
    volatile boolean prepareFlush = false;
    final CyclicBarrier flushMemStoreCyclicBarrierBeforeStartTest = new CyclicBarrier(2);

    public HRegionForTest(HRegionFileSystem fs, WAL wal, Configuration confParam,
        TableDescriptor htd, RegionServerServices rsServices) {
      super(fs, wal, confParam, htd, rsServices);
    }

    @SuppressWarnings("deprecation")
    public HRegionForTest(Path tableDir, WAL wal, FileSystem fs, Configuration confParam,
        RegionInfo regionInfo, TableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
    }

    public void setRegionReplicationSink(RegionReplicationSink regionReplicationSink) {
      this.regionReplicationSink = Optional.of(regionReplicationSink);
    }

    @Override
    protected FlushResultImpl internalFlushcache(WAL wal, long myseqid,
        Collection<HStore> storesToFlush, MonitoredTask status, boolean writeFlushWalMarker,
        FlushLifeCycleTracker tracker) throws IOException {

      FlushResultImpl result = super.internalFlushcache(wal, myseqid, storesToFlush, status,
        writeFlushWalMarker, tracker);
      if (!startTest && this.getRegionInfo().getReplicaId() == 0
          && this.getRegionInfo().getTable().equals(tableName) && initialFlush) {
        try {
          this.flushMemStoreCyclicBarrierBeforeStartTest.await();
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }
      return result;
    }

    @Override
    protected PrepareFlushResult internalPrepareFlushCache(WAL wal, long myseqid,
        Collection<HStore> storesToFlush, MonitoredTask status, boolean writeFlushWalMarker,
        FlushLifeCycleTracker tracker) throws IOException {
      if (!startTest) {
        return super.internalPrepareFlushCache(wal, myseqid, storesToFlush, status,
          writeFlushWalMarker, tracker);
      }

      if (this.getRegionInfo().getReplicaId() == 0
          && Thread.currentThread().getName().equals(USER_THREAD_NAME)) {
        this.prepareFlush = true;
      }
      try {
        return super.internalPrepareFlushCache(wal, myseqid, storesToFlush, status,
          writeFlushWalMarker, tracker);
      }
      finally {
        if (this.getRegionInfo().getReplicaId() == 0
            && Thread.currentThread().getName().equals(USER_THREAD_NAME)) {
          this.prepareFlush = false;
        }
      }

    }
  }

  public static final class ErrorReplayRSRpcServices extends RSRpcServices {
    private static final AtomicInteger callCounter = new AtomicInteger(0);

    public ErrorReplayRSRpcServices(HRegionServer rs) throws IOException {
      super(rs);
    }

    @Override
    public ReplicateWALEntryResponse replicateToReplica(RpcController rpcController,
        ReplicateWALEntryRequest replicateWALEntryRequest) throws ServiceException {

      if (!startTest) {
        return super.replicateToReplica(rpcController, replicateWALEntryRequest);
      }

      List<WALEntry> entries = replicateWALEntryRequest.getEntryList();
      if (CollectionUtils.isEmpty(entries)) {
        return ReplicateWALEntryResponse.getDefaultInstance();
      }
      ByteString regionName = entries.get(0).getKey().getEncodedRegionName();

      HRegion region;
      try {
        region = server.getRegionByEncodedName(regionName.toStringUtf8());
      } catch (NotServingRegionException e) {
        throw new ServiceException(e);
      }

      if (!region.getRegionInfo().getTable().equals(tableName)
          || region.getRegionInfo().getReplicaId() != 1) {
        return super.replicateToReplica(rpcController, replicateWALEntryRequest);
      }

      int count = callCounter.incrementAndGet();
      if (count > 1) {
        return super.replicateToReplica(rpcController, replicateWALEntryRequest);
      }
      throw new ServiceException(new DoNotRetryIOException("Inject error!"));
    }
  }

  public static final class RSForTest
      extends SingleProcessHBaseCluster.MiniHBaseClusterRegionServer {

    public RSForTest(Configuration conf) throws IOException, InterruptedException {
      super(conf);
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
      return new ErrorReplayRSRpcServices(this);
    }
  }
}
