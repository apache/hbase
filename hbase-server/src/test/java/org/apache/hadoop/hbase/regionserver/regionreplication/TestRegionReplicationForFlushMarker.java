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
package org.apache.hadoop.hbase.regionserver.regionreplication;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.hadoop.hbase.regionserver.MemStoreFlusher;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor.FlushAction;

@Category({ RegionServerTests.class, LargeTests.class })
public class TestRegionReplicationForFlushMarker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionReplicationForFlushMarker.class);

  private static final byte[] FAMILY = Bytes.toBytes("family_test");

  private static final byte[] QUAL = Bytes.toBytes("qualifier_test");

  private static final HBaseTestingUtil HTU = new HBaseTestingUtil();
  private static final int NB_SERVERS = 2;

  private static TableName tableName = TableName.valueOf("TestRegionReplicationForFlushMarker");
  private static volatile boolean startTest = false;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = HTU.getConfiguration();
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    conf.setClass(HConstants.REGION_IMPL, HRegionForTest.class, HRegion.class);
    conf.setInt(RegionReplicationSink.RETRIES_NUMBER, 1);
    conf.setLong(RegionReplicationSink.RPC_TIMEOUT_MS, 10 * 60 * 1000);
    conf.setLong(RegionReplicationSink.OPERATION_TIMEOUT_MS, 20 * 60 * 1000);
    conf.setLong(RegionReplicationSink.META_EDIT_RPC_TIMEOUT_MS, 10 * 60 * 1000);
    conf.setLong(RegionReplicationSink.META_EDIT_OPERATION_TIMEOUT_MS, 20 * 60 * 1000);
    conf.setBoolean(RegionReplicaUtil.REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH_CONF_KEY, false);
    conf.setInt(RegionReplicationFlushRequester.MIN_INTERVAL_SECS, 3);
    HTU.startMiniCluster(StartTestingClusterOption.builder().rsClass(RSForTest.class)
      .numRegionServers(NB_SERVERS).build());

  }

  @AfterClass
  public static void tearDown() throws Exception {
    HTU.shutdownMiniCluster();
  }

  /**
   * This test is for HBASE-26960, before HBASE-26960, {@link MemStoreFlusher} does not write the
   * {@link FlushAction#CANNOT_FLUSH} marker to the WAL when the memstore is empty,so if the
   * {@link RegionReplicationSink} request a flush when the memstore is empty, it could not receive
   * the {@link FlushAction#CANNOT_FLUSH} and the replication may be hanged. After HBASE-26768,when
   * the {@link RegionReplicationSink} request a flush when the memstore is empty,even it does not
   * writes the {@link FlushAction#CANNOT_FLUSH} marker to the WAL,we also replicate the
   * {@link FlushAction#CANNOT_FLUSH} marker to the secondary region replica.
   */
  @Test
  public void testCannotFlushMarker() throws Exception {
    final HRegionForTest[] regions = this.createTable();
    RegionReplicationSink regionReplicationSink = regions[0].getRegionReplicationSink().get();
    assertTrue(regionReplicationSink != null);

    String oldThreadName = Thread.currentThread().getName();
    Thread.currentThread().setName(HRegionForTest.USER_THREAD_NAME);
    try {

      byte[] rowKey1 = Bytes.toBytes(1);
      startTest = true;
      /**
       * Write First cell,replicating to secondary replica is error,and then
       * {@link RegionReplicationSink} request flush,after {@link RegionReplicationSink} receiving
       * the {@link FlushAction#START_FLUSH},the {@link RegionReplicationSink#failedReplicas} is
       * cleared,but replicating {@link FlushAction#START_FLUSH} is failed again,so
       * {@link RegionReplicationSink} request flush once more, but now memstore is empty,so the
       * {@link MemStoreFlusher} just write a {@link FlushAction#CANNOT_FLUSH} marker to the WAL.
       */
      regions[0].put(new Put(rowKey1).addColumn(FAMILY, QUAL, Bytes.toBytes(1)));
      /**
       * Wait for the {@link FlushAction#CANNOT_FLUSH} is written and initiating replication
       */
      regions[0].cyclicBarrier.await();
      assertTrue(regions[0].prepareFlushCounter.get() == 2);
      /**
       * The {@link RegionReplicationSink#failedReplicas} is cleared by the
       * {@link FlushAction#CANNOT_FLUSH} marker.
       */
      assertTrue(regionReplicationSink.getFailedReplicas().isEmpty());
    } finally {
      startTest = false;
      Thread.currentThread().setName(oldThreadName);
    }
  }

  private HRegionForTest[] createTable() throws Exception {
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(NB_SERVERS)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
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
    return regions;
  }

  public static final class HRegionForTest extends HRegion {
    static final String USER_THREAD_NAME = "TestRegionReplicationForFlushMarker";
    final CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
    final AtomicInteger prepareFlushCounter = new AtomicInteger(0);

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
    protected void writeRegionOpenMarker(WAL wal, long openSeqId) throws IOException {
      // not write the region open marker to interrupt the test.
    }

    @Override
    protected PrepareFlushResult internalPrepareFlushCache(WAL wal, long myseqid,
      Collection<HStore> storesToFlush, MonitoredTask status, boolean writeFlushWalMarker,
      FlushLifeCycleTracker tracker) throws IOException {
      if (!startTest) {
        return super.internalPrepareFlushCache(wal, myseqid, storesToFlush, status,
          writeFlushWalMarker, tracker);
      }

      if (this.getRegionInfo().getReplicaId() != 0) {
        return super.internalPrepareFlushCache(wal, myseqid, storesToFlush, status,
          writeFlushWalMarker, tracker);
      }

      try {
        PrepareFlushResult result = super.internalPrepareFlushCache(wal, myseqid, storesToFlush,
          status, writeFlushWalMarker, tracker);
        this.prepareFlushCounter.incrementAndGet();
        /**
         * First flush is {@link FlushAction#START_FLUSH} marker and the second flush is
         * {@link FlushAction#CANNOT_FLUSH} marker because the memstore is empty.
         */
        if (
          this.prepareFlushCounter.get() == 2 && result.getResult() != null
            && result.getResult().getResult() == FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY
        ) {

          cyclicBarrier.await();
        }
        return result;
      } catch (BrokenBarrierException | InterruptedException e) {
        throw new RuntimeException(e);
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

      if (
        !region.getRegionInfo().getTable().equals(tableName)
          || region.getRegionInfo().getReplicaId() != 1
      ) {
        return super.replicateToReplica(rpcController, replicateWALEntryRequest);
      }

      /**
       * Simulate the first cell write and {@link FlushAction#START_FLUSH} marker replicating error.
       */
      int count = callCounter.incrementAndGet();
      if (count > 2) {
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
