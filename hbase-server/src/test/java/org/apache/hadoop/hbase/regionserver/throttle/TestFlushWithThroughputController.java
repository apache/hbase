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
package org.apache.hadoop.hbase.regionserver.throttle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.StripeStoreEngine;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestFlushWithThroughputController {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFlushWithThroughputController.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestFlushWithThroughputController.class);
  private static final double EPSILON = 1.3E-6;

  private HBaseTestingUtil hbtu;
  @Rule public TestName testName = new TestName();
  private TableName tableName;
  private final byte[] family = Bytes.toBytes("f");
  private final byte[] qualifier = Bytes.toBytes("q");

  @Before
  public void setUp() {
    hbtu = new HBaseTestingUtil();
    tableName = TableName.valueOf("Table-" + testName.getMethodName());
    hbtu.getConfiguration().set(
        FlushThroughputControllerFactory.HBASE_FLUSH_THROUGHPUT_CONTROLLER_KEY,
        PressureAwareFlushThroughputController.class.getName());
  }

  @After
  public void tearDown() throws Exception {
    hbtu.shutdownMiniCluster();
  }

  private HStore getStoreWithName(TableName tableName) {
    SingleProcessHBaseCluster cluster = hbtu.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (Region region : hrs.getRegions(tableName)) {
        return ((HRegion) region).getStores().iterator().next();
      }
    }
    return null;
  }

  private void setMaxMinThroughputs(long max, long min) {
    Configuration conf = hbtu.getConfiguration();
    conf.setLong(
        PressureAwareFlushThroughputController.HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_LOWER_BOUND, min);
    conf.setLong(
        PressureAwareFlushThroughputController.HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_UPPER_BOUND, max);
  }

  /**
   * Writes Puts to the table and flushes few times.
   * @return {@link Pair} of (throughput, duration).
   */
  private Pair<Double, Long> generateAndFlushData(Table table) throws IOException {
    // Internally, throughput is controlled after every cell write, so keep value size less for
    // better control.
    final int NUM_FLUSHES = 3, NUM_PUTS = 50, VALUE_SIZE = 200 * 1024;
    long duration = 0;
    for (int i = 0; i < NUM_FLUSHES; i++) {
      // Write about 10M (10 times of throughput rate) per iteration.
      for (int j = 0; j < NUM_PUTS; j++) {
        byte[] value = new byte[VALUE_SIZE];
        Bytes.random(value);
        table.put(new Put(Bytes.toBytes(i * 10 + j)).addColumn(family, qualifier, value));
      }
      long startTime = System.nanoTime();
      hbtu.getAdmin().flush(tableName);
      duration += System.nanoTime() - startTime;
    }
    HStore store = getStoreWithName(tableName);
    assertEquals(NUM_FLUSHES, store.getStorefilesCount());
    double throughput = (double)store.getStorefilesSize()
        / TimeUnit.NANOSECONDS.toSeconds(duration);
    return new Pair<>(throughput, duration);
  }

  private long testFlushWithThroughputLimit() throws Exception {
    final long throughputLimit = 1024 * 1024;
    setMaxMinThroughputs(throughputLimit, throughputLimit);
    Configuration conf = hbtu.getConfiguration();
    conf.setLong(
      PressureAwareFlushThroughputController.HBASE_HSTORE_FLUSH_THROUGHPUT_CONTROL_CHECK_INTERVAL,
      throughputLimit);
    hbtu.startMiniCluster(1);
    Table table = hbtu.createTable(tableName, family);
    Pair<Double, Long> result = generateAndFlushData(table);
    hbtu.deleteTable(tableName);
    LOG.debug("Throughput is: " + (result.getFirst() / 1024 / 1024) + " MB/s");
    // confirm that the speed limit work properly(not too fast, and also not too slow)
    // 20% is the max acceptable error rate.
    assertTrue(result.getFirst()  < throughputLimit * 1.2);
    assertTrue(result.getFirst() > throughputLimit * 0.8);
    return result.getSecond();
  }

  @Test
  public void testFlushControl() throws Exception {
    testFlushWithThroughputLimit();
  }

  /**
   * Test the tuning task of {@link PressureAwareFlushThroughputController}
   */
  @Test
  public void testFlushThroughputTuning() throws Exception {
    Configuration conf = hbtu.getConfiguration();
    setMaxMinThroughputs(20L * 1024 * 1024, 10L * 1024 * 1024);
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, DefaultStoreEngine.class.getName());
    conf.setInt(PressureAwareFlushThroughputController.HBASE_HSTORE_FLUSH_THROUGHPUT_TUNE_PERIOD,
      3000);
    hbtu.startMiniCluster(1);
    Connection conn = ConnectionFactory.createConnection(conf);
    hbtu.getAdmin().createTable(TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).setCompactionEnabled(false)
      .build());
    hbtu.waitTableAvailable(tableName);
    HRegionServer regionServer = hbtu.getRSForFirstRegionInTable(tableName);
    double pressure = regionServer.getFlushPressure();
    LOG.debug("Flush pressure before flushing: " + pressure);
    PressureAwareFlushThroughputController throughputController =
        (PressureAwareFlushThroughputController) regionServer.getFlushThroughputController();
    for (HRegion region : regionServer.getRegions()) {
      region.flush(true);
    }
    // We used to assert that the flush pressure is zero but after HBASE-15787 or HBASE-18294 we
    // changed to use heapSize instead of dataSize to calculate the flush pressure, and since
    // heapSize will never be zero, so flush pressure will never be zero either. So we changed the
    // assertion here.
    assertTrue(regionServer.getFlushPressure() < pressure);
    Thread.sleep(5000);
    Table table = conn.getTable(tableName);
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        byte[] value = new byte[256 * 1024];
        Bytes.random(value);
        table.put(new Put(Bytes.toBytes(i * 10 + j)).addColumn(family, qualifier, value));
      }
    }
    Thread.sleep(5000);
    double expectedThroughPut = 10L * 1024 * 1024 * (1 + regionServer.getFlushPressure());
    assertEquals(expectedThroughPut, throughputController.getMaxThroughput(), EPSILON);

    conf.set(FlushThroughputControllerFactory.HBASE_FLUSH_THROUGHPUT_CONTROLLER_KEY,
      NoLimitThroughputController.class.getName());
    regionServer.onConfigurationChange(conf);
    assertTrue(throughputController.isStopped());
    assertTrue(regionServer.getFlushThroughputController() instanceof NoLimitThroughputController);
    conn.close();
  }

  /**
   * Test the logic for striped store.
   */
  @Test
  public void testFlushControlForStripedStore() throws Exception {
    hbtu.getConfiguration().set(StoreEngine.STORE_ENGINE_CLASS_KEY,
      StripeStoreEngine.class.getName());
    testFlushWithThroughputLimit();
  }
}
