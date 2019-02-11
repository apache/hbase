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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicy;
import org.apache.hadoop.hbase.client.backoff.ExponentialClientBackoffPolicy;
import org.apache.hadoop.hbase.client.backoff.ServerStatistics;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that we can actually send and use region metrics to slowdown client writes
 */
@Category(MediumTests.class)
public class TestClientPushback {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestClientPushback.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestClientPushback.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName tableName = TableName.valueOf("client-pushback");
  private static final byte[] family = Bytes.toBytes("f");
  private static final byte[] qualifier = Bytes.toBytes("q");
  private static final long flushSizeBytes = 512;

  @BeforeClass
  public static void setupCluster() throws Exception{
    Configuration conf = UTIL.getConfiguration();
    // enable backpressure
    conf.setBoolean(HConstants.ENABLE_CLIENT_BACKPRESSURE, true);
    // use the exponential backoff policy
    conf.setClass(ClientBackoffPolicy.BACKOFF_POLICY_CLASS, ExponentialClientBackoffPolicy.class,
      ClientBackoffPolicy.class);
    // turn the memstore size way down so we don't need to write a lot to see changes in memstore
    // load
    conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, flushSizeBytes);
    // ensure we block the flushes when we are double that flushsize
    conf.setLong(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER, HConstants.DEFAULT_HREGION_MEMSTORE_BLOCK_MULTIPLIER);
    conf.setBoolean(CLIENT_SIDE_METRICS_ENABLED_KEY, true);
    UTIL.startMiniCluster(1);
    UTIL.createTable(tableName, family);
  }

  @AfterClass
  public static void teardownCluster() throws Exception{
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testClientTracksServerPushback() throws Exception{
    Configuration conf = UTIL.getConfiguration();

    ConnectionImplementation conn =
      (ConnectionImplementation) ConnectionFactory.createConnection(conf);
    BufferedMutatorImpl mutator = (BufferedMutatorImpl) conn.getBufferedMutator(tableName);

    HRegionServer rs = UTIL.getHBaseCluster().getRegionServer(0);
    Region region = rs.getRegions(tableName).get(0);

    LOG.debug("Writing some data to "+tableName);
    // write some data
    Put p = new Put(Bytes.toBytes("row"));
    p.addColumn(family, qualifier, Bytes.toBytes("value1"));
    mutator.mutate(p);
    mutator.flush();

    // get the current load on RS. Hopefully memstore isn't flushed since we wrote the the data
    int load = (int) ((region.getMemStoreHeapSize() * 100)
        / flushSizeBytes);
    LOG.debug("Done writing some data to "+tableName);

    // get the stats for the region hosting our table
    ClientBackoffPolicy backoffPolicy = conn.getBackoffPolicy();
    assertTrue("Backoff policy is not correctly configured",
      backoffPolicy instanceof ExponentialClientBackoffPolicy);

    ServerStatisticTracker stats = conn.getStatisticsTracker();
    assertNotNull( "No stats configured for the client!", stats);
    // get the names so we can query the stats
    ServerName server = rs.getServerName();
    byte[] regionName = region.getRegionInfo().getRegionName();

    // check to see we found some load on the memstore
    ServerStatistics serverStats = stats.getServerStatsForTesting(server);
    ServerStatistics.RegionStatistics regionStats = serverStats.getStatsForRegion(regionName);
    assertEquals("We did not find some load on the memstore", load,
      regionStats.getMemStoreLoadPercent());
    // check that the load reported produces a nonzero delay
    long backoffTime = backoffPolicy.getBackoffTime(server, regionName, serverStats);
    assertNotEquals("Reported load does not produce a backoff", 0, backoffTime);
    LOG.debug("Backoff calculated for " + region.getRegionInfo().getRegionNameAsString() + " @ " +
      server + " is " + backoffTime);

    // Reach into the connection and submit work directly to AsyncProcess so we can
    // monitor how long the submission was delayed via a callback
    List<Row> ops = new ArrayList<>(1);
    ops.add(p);
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicLong endTime = new AtomicLong();
    long startTime = EnvironmentEdgeManager.currentTime();
    Batch.Callback<Result> callback = (byte[] r, byte[] row, Result result) -> {
        endTime.set(EnvironmentEdgeManager.currentTime());
        latch.countDown();
    };
    AsyncProcessTask<Result> task = AsyncProcessTask.newBuilder(callback)
            .setPool(mutator.getPool())
            .setTableName(tableName)
            .setRowAccess(ops)
            .setSubmittedRows(AsyncProcessTask.SubmittedRows.AT_LEAST_ONE)
            .setOperationTimeout(conn.getConnectionConfiguration().getOperationTimeout())
            .setRpcTimeout(60 * 1000)
            .build();
    mutator.getAsyncProcess().submit(task);
    // Currently the ExponentialClientBackoffPolicy under these test conditions
    // produces a backoffTime of 151 milliseconds. This is long enough so the
    // wait and related checks below are reasonable. Revisit if the backoff
    // time reported by above debug logging has significantly deviated.
    String name = server.getServerName() + "," + Bytes.toStringBinary(regionName);
    MetricsConnection.RegionStats rsStats = conn.getConnectionMetrics().
            serverStats.get(server).get(regionName);
    assertEquals(name, rsStats.name);
    assertEquals(rsStats.heapOccupancyHist.getSnapshot().getMean(),
        (double)regionStats.getHeapOccupancyPercent(), 0.1 );
    assertEquals(rsStats.memstoreLoadHist.getSnapshot().getMean(),
        (double)regionStats.getMemStoreLoadPercent(), 0.1);

    MetricsConnection.RunnerStats runnerStats = conn.getConnectionMetrics().runnerStats;

    assertEquals(1, runnerStats.delayRunners.getCount());
    assertEquals(1, runnerStats.normalRunners.getCount());
    assertEquals("", runnerStats.delayIntevalHist.getSnapshot().getMean(),
      (double)backoffTime, 0.1);

    latch.await(backoffTime * 2, TimeUnit.MILLISECONDS);
    assertNotEquals("AsyncProcess did not submit the work time", 0, endTime.get());
    assertTrue("AsyncProcess did not delay long enough", endTime.get() - startTime >= backoffTime);
  }

  @Test
  public void testMutateRowStats() throws IOException {
    Configuration conf = UTIL.getConfiguration();
    ConnectionImplementation conn =
      (ConnectionImplementation) ConnectionFactory.createConnection(conf);
    Table table = conn.getTable(tableName);
    HRegionServer rs = UTIL.getHBaseCluster().getRegionServer(0);
    Region region = rs.getRegions(tableName).get(0);

    RowMutations mutations = new RowMutations(Bytes.toBytes("row"));
    Put p = new Put(Bytes.toBytes("row"));
    p.addColumn(family, qualifier, Bytes.toBytes("value2"));
    mutations.add(p);
    table.mutateRow(mutations);

    ServerStatisticTracker stats = conn.getStatisticsTracker();
    assertNotNull( "No stats configured for the client!", stats);
    // get the names so we can query the stats
    ServerName server = rs.getServerName();
    byte[] regionName = region.getRegionInfo().getRegionName();

    // check to see we found some load on the memstore
    ServerStatistics serverStats = stats.getServerStatsForTesting(server);
    ServerStatistics.RegionStatistics regionStats = serverStats.getStatsForRegion(regionName);

    assertNotNull(regionStats);
    assertTrue(regionStats.getMemStoreLoadPercent() > 0);
    }
}
