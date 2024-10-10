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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.AsyncFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALSyncTimeoutIOException;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;

@Category({ RegionServerTests.class, LargeTests.class })
public class TestRegionReplicationForWriteException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionReplicationForWriteException.class);

  private static final byte[] FAMILY = Bytes.toBytes("family_test");

  private static final byte[] QUAL = Bytes.toBytes("qualifier_test");

  private static final HBaseTestingUtil HTU = new HBaseTestingUtil();
  private static final int NB_SERVERS = 2;

  private static TableName tableName = TableName.valueOf("TestRegionReplicationForWriteException");
  private static volatile boolean testWALTimout = false;
  private static volatile boolean testCP = false;
  private static final long timeoutMIlliseconds = 3000;
  private static final String USER_THREAD_NAME = tableName.getNameAsString();

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
    conf.setClass(WALFactory.WAL_PROVIDER, SlowAsyncFSWALProvider.class, WALProvider.class);
    conf.setLong(AbstractFSWAL.WAL_SYNC_TIMEOUT_MS, timeoutMIlliseconds);
    HTU.startMiniCluster(StartTestingClusterOption.builder().numRegionServers(NB_SERVERS).build());

  }

  @AfterClass
  public static void tearDown() throws Exception {
    HTU.shutdownMiniCluster();
  }

  /**
   * This test is for HBASE-27303.
   */
  @Test
  public void testWriteException() throws Exception {
    final HRegionForTest[] regions = this.createTable();
    RegionReplicationSink regionReplicationSink = regions[0].getRegionReplicationSink().get();
    assertTrue(regionReplicationSink != null);
    final AtomicInteger replicateCounter = new AtomicInteger(0);
    this.setUpSpiedRegionReplicationSink(regionReplicationSink, regions[0], replicateCounter);

    String oldThreadName = Thread.currentThread().getName();
    Thread.currentThread().setName(USER_THREAD_NAME);
    try {
      testCP = true;
      try {
        byte[] rowKey1 = Bytes.toBytes(1);
        byte[] value1 = Bytes.toBytes(3);
        /**
         * Write first put,{@link WAL#sync} is successful but {@link RegionObserver#postBatchMutate}
         * throws exception,the rowkey1 is applied to primary and secondary replicas successfully.
         */
        try {
          regions[0].put(new Put(rowKey1).addColumn(FAMILY, QUAL, value1));
          fail();
        } catch (DoNotRetryIOException e) {
          assertTrue(e.getMessage().equals(MyRegionObserver.ERROR_MESSAGE));
        }

        try (Table table = HTU.getConnection().getTable(tableName)) {
          assertTrue(checkReplica(table, FAMILY, QUAL, rowKey1, value1, 0));
          HTU.waitFor(30000, () -> checkReplica(table, FAMILY, QUAL, rowKey1, value1, 1));
        }
      } finally {
        testCP = false;
      }

      byte[] rowKey2 = Bytes.toBytes(2);
      byte[] value2 = Bytes.toBytes(6);
      replicateCounter.set(0);
      testWALTimout = true;
      try {
        /**
         * Write second put,the {@link WAL#sync} timeout and throws
         * {@link WALSyncTimeoutIOException},{@link HRegion#put} is failed and rowKey2 is not
         * applied to primary and secondary replicas.
         */
        try {
          regions[0].put(new Put(rowKey2).addColumn(FAMILY, QUAL, value2));
          fail();
        } catch (WALSyncTimeoutIOException e) {
          assertTrue(e != null);
        }

        assertTrue(regions[0].getRSServices().isAborted());
        assertTrue(replicateCounter.get() == 0);
        Thread.sleep(2000);
        try (Table table = HTU.getConnection().getTable(tableName)) {
          assertFalse(checkReplica(table, FAMILY, QUAL, rowKey2, value2, 1));
        }
      } finally {
        testWALTimout = false;
      }
    } finally {
      Thread.currentThread().setName(oldThreadName);
    }
  }

  private RegionReplicationSink setUpSpiedRegionReplicationSink(
    final RegionReplicationSink regionReplicationSink, final HRegionForTest primaryRegion,
    final AtomicInteger counter) {
    RegionReplicationSink spiedRegionReplicationSink = Mockito.spy(regionReplicationSink);

    Mockito.doAnswer((invocationOnMock) -> {
      if (!testWALTimout || !USER_THREAD_NAME.equals(Thread.currentThread().getName())) {
        invocationOnMock.callRealMethod();
        return null;
      }
      WALKeyImpl walKey = invocationOnMock.getArgument(0);
      if (!walKey.getTableName().equals(tableName)) {
        invocationOnMock.callRealMethod();
        return null;
      }
      counter.incrementAndGet();
      invocationOnMock.callRealMethod();
      return null;
    }).when(spiedRegionReplicationSink).add(Mockito.any(), Mockito.any(), Mockito.any());
    primaryRegion.setRegionReplicationSink(spiedRegionReplicationSink);
    return spiedRegionReplicationSink;
  }

  private static boolean checkReplica(Table table, byte[] fam, byte[] qual, byte[] rowKey,
    byte[] expectValue, int replicaId) throws IOException {
    Get get = new Get(rowKey).setConsistency(Consistency.TIMELINE).setReplicaId(replicaId);
    Result result = table.get(get);
    byte[] value = result.getValue(fam, qual);
    return value != null && value.length > 0 && Arrays.equals(expectValue, value);
  }

  private HRegionForTest[] createTable() throws Exception {
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setRegionReplication(NB_SERVERS).setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
      .setCoprocessor(MyRegionObserver.class.getName()).build();
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
    for (HRegionForTest region : regions) {
      assertNotNull(region);
    }
    return regions;
  }

  public static final class HRegionForTest extends HRegion {

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

    public RegionServerServices getRSServices() {
      return this.rsServices;
    }
  }

  public static class SlowAsyncFSWAL extends AsyncFSWAL {

    public SlowAsyncFSWAL(FileSystem fs, Abortable abortable, Path rootDir, String logDir,
      String archiveDir, Configuration conf, List<WALActionsListener> listeners,
      boolean failIfWALExists, String prefix, String suffix, EventLoopGroup eventLoopGroup,
      Class<? extends Channel> channelClass, StreamSlowMonitor monitor)
      throws FailedLogCloseException, IOException {
      super(fs, abortable, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix,
        suffix, null, null, eventLoopGroup, channelClass, monitor);
    }

    @Override
    protected void atHeadOfRingBufferEventHandlerAppend() {
      if (testWALTimout) {
        try {
          Thread.sleep(timeoutMIlliseconds + 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      super.atHeadOfRingBufferEventHandlerAppend();
    }

  }

  public static class SlowAsyncFSWALProvider extends AsyncFSWALProvider {

    @Override
    protected AsyncFSWAL createWAL() throws IOException {
      return new SlowAsyncFSWAL(CommonFSUtils.getWALFileSystem(conf), this.abortable,
        CommonFSUtils.getWALRootDir(conf), getWALDirectoryName(factory.getFactoryId()),
        getWALArchiveDirectoryName(conf, factory.getFactoryId()), conf, listeners, true, logPrefix,
        META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null, eventLoopGroup,
        channelClass, factory.getExcludeDatanodeManager().getStreamSlowMonitor(providerId));
    }

  }

  public static class MyRegionObserver implements RegionCoprocessor, RegionObserver {

    private static final String ERROR_MESSAGE = "Inject error!";

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void postBatchMutate(ObserverContext<? extends RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      if (!testCP || !RegionReplicaUtil.isDefaultReplica(c.getEnvironment().getRegionInfo())) {
        return;
      }
      throw new DoNotRetryIOException(ERROR_MESSAGE);
    }
  }

}
