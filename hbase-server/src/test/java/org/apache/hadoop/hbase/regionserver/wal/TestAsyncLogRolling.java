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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowRegionServerTests;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;

@Category({ VerySlowRegionServerTests.class, LargeTests.class })
public class TestAsyncLogRolling extends AbstractTestLogRolling {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncLogRolling.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TestAsyncLogRolling.TEST_UTIL.getConfiguration();
    conf.setInt(FanOutOneBlockAsyncDFSOutputHelper.ASYNC_DFS_OUTPUT_CREATE_MAX_RETRIES, 100);
    conf.set(WALFactory.WAL_PROVIDER, "asyncfs");
    AbstractTestLogRolling.setUpBeforeClass();
  }

  public static class SlowSyncLogWriter extends AsyncProtobufLogWriter {

    public SlowSyncLogWriter(EventLoopGroup eventLoopGroup, Class<? extends Channel> channelClass) {
      super(eventLoopGroup, channelClass);
    }

    @Override
    public CompletableFuture<Long> sync(boolean forceSync) {
      try {
        Thread.sleep(syncLatencyMillis);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return super.sync(forceSync);
    }
  }

  @Override
  protected void setSlowLogWriter(Configuration conf) {
    conf.set(AsyncFSWALProvider.WRITER_IMPL, SlowSyncLogWriter.class.getName());
  }

  @Override
  protected void setDefaultLogWriter(Configuration conf) {
    conf.set(AsyncFSWALProvider.WRITER_IMPL, AsyncProtobufLogWriter.class.getName());
  }

  @Test
  public void testSlowSyncLogRolling() throws Exception {
    // Create the test table
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(getName()))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();
    admin.createTable(desc);
    try (Table table = TEST_UTIL.getConnection().getTable(desc.getTableName())) {
      server = TEST_UTIL.getRSForFirstRegionInTable(desc.getTableName());
      RegionInfo region = server.getRegions(desc.getTableName()).get(0).getRegionInfo();
      final AbstractFSWAL<?> log = getWALAndRegisterSlowSyncHook(region);

      // Set default log writer, no additional latency to any sync on the hlog.
      checkSlowSync(log, table, -1, 10, false);

      // Adds 5000 ms of latency to any sync on the hlog. This will trip the other threshold.
      // Write some data. Should only take one sync.
      checkSlowSync(log, table, 5000, 1, true);

      // Set default log writer, no additional latency to any sync on the hlog.
      checkSlowSync(log, table, -1, 10, false);
    }
  }

  @Test
  public void testLogRollOnDatanodeDeath() throws IOException, InterruptedException {
    dfsCluster.startDataNodes(TEST_UTIL.getConfiguration(), 3, true, null, null);
    tableName = getName();
    Table table = createTestTable(tableName);
    TEST_UTIL.waitUntilAllRegionsAssigned(table.getName());
    doPut(table, 1);
    server = TEST_UTIL.getRSForFirstRegionInTable(table.getName());
    RegionInfo hri = server.getRegions(table.getName()).get(0).getRegionInfo();
    AsyncFSWAL wal = (AsyncFSWAL) server.getWAL(hri);
    int numRolledLogFiles = AsyncFSWALProvider.getNumRolledLogFiles(wal);
    DatanodeInfo[] dnInfos = wal.getPipeline();
    DataNodeProperties dnProp = TEST_UTIL.getDFSCluster().stopDataNode(dnInfos[0].getName());
    TEST_UTIL.getDFSCluster().restartDataNode(dnProp);
    doPut(table, 2);
    assertEquals(numRolledLogFiles + 1, AsyncFSWALProvider.getNumRolledLogFiles(wal));
  }
}
