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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.regionreplication.RegionReplicationSink;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;

@Category({ RegionServerTests.class, LargeTests.class })
public class TestWALSyncTimeoutException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALSyncTimeoutException.class);

  private static final byte[] FAMILY = Bytes.toBytes("family_test");

  private static final byte[] QUAL = Bytes.toBytes("qualifier_test");

  private static final HBaseTestingUtil HTU = new HBaseTestingUtil();

  private static TableName tableName = TableName.valueOf("TestWALSyncTimeoutException");
  private static volatile boolean testWALTimout = false;
  private static final long timeoutMIlliseconds = 3000;
  private static final String USER_THREAD_NAME = tableName.getNameAsString();

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = HTU.getConfiguration();
    conf.setClass(HConstants.REGION_IMPL, HRegionForTest.class, HRegion.class);
    conf.setInt(RegionReplicationSink.RETRIES_NUMBER, 1);
    conf.setLong(RegionReplicationSink.RPC_TIMEOUT_MS, 10 * 60 * 1000);
    conf.setLong(RegionReplicationSink.OPERATION_TIMEOUT_MS, 20 * 60 * 1000);
    conf.setLong(RegionReplicationSink.META_EDIT_RPC_TIMEOUT_MS, 10 * 60 * 1000);
    conf.setLong(RegionReplicationSink.META_EDIT_OPERATION_TIMEOUT_MS, 20 * 60 * 1000);
    conf.setClass(WALFactory.WAL_PROVIDER, SlowAsyncFSWALProvider.class, WALProvider.class);
    conf.setLong(AbstractFSWAL.WAL_SYNC_TIMEOUT_MS, timeoutMIlliseconds);
    HTU.startMiniCluster(StartTestingClusterOption.builder().numRegionServers(1).build());

  }

  @AfterClass
  public static void tearDown() throws Exception {
    HTU.shutdownMiniCluster();
  }

  /**
   * This test is for HBASE-27230. When {@link WAL#sync} timeout, it would throws
   * {@link WALSyncTimeoutIOException},and when {@link HRegion#doWALAppend} catches this exception
   * it aborts the RegionServer.
   */
  @Test
  public void testWALSyncWriteException() throws Exception {
    final HRegionForTest region = this.createTable();

    String oldThreadName = Thread.currentThread().getName();
    Thread.currentThread().setName(USER_THREAD_NAME);
    try {
      byte[] rowKey1 = Bytes.toBytes(1);
      byte[] value1 = Bytes.toBytes(3);
      Thread.sleep(2000);
      testWALTimout = true;

      /**
       * The {@link WAL#sync} would timeout and throws {@link WALSyncTimeoutIOException},when
       * {@link HRegion#doWALAppend} catches this exception it aborts the RegionServer.
       */
      try {
        region.put(new Put(rowKey1).addColumn(FAMILY, QUAL, value1));
        fail();
      } catch (WALSyncTimeoutIOException e) {
        assertTrue(e != null);
      }
      assertTrue(region.getRSServices().isAborted());
    } finally {
      Thread.currentThread().setName(oldThreadName);
      testWALTimout = false;
    }
  }

  private HRegionForTest createTable() throws Exception {
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    HTU.getAdmin().createTable(tableDescriptor);
    HRegionServer rs = HTU.getMiniHBaseCluster().getRegionServer(0);
    return (HRegionForTest) rs.getRegions(tableName).get(0);
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
}
