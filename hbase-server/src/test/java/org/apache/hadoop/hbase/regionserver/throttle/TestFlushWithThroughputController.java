/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.throttle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.StripeStoreEngine;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(MediumTests.class)
public class TestFlushWithThroughputController {
  private static final Log LOG = LogFactory.getLog(TestFlushWithThroughputController.class);
  private static final double EPSILON = 1E-6;

  private HBaseTestingUtility hbtu;
  @Rule public TestName testName = new TestName();
  private TableName tableName;
  private final byte[] family = Bytes.toBytes("f");
  private final byte[] qualifier = Bytes.toBytes("q");

  @Before
  public void setUp() {
    hbtu = new HBaseTestingUtility();
    tableName = TableName.valueOf("Table-" + testName.getMethodName());
    hbtu.getConfiguration().set(
        FlushThroughputControllerFactory.HBASE_FLUSH_THROUGHPUT_CONTROLLER_KEY,
        PressureAwareFlushThroughputController.class.getName());
  }

  @After
  public void tearDown() throws Exception {
    hbtu.shutdownMiniCluster();
  }

  private Store getStoreWithName(TableName tableName) {
    MiniHBaseCluster cluster = hbtu.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (Region region : hrs.getOnlineRegions(tableName)) {
        return region.getStores().iterator().next();
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
    Random rand = new Random();
    long duration = 0;
    for (int i = 0; i < NUM_FLUSHES; i++) {
      // Write about 10M (10 times of throughput rate) per iteration.
      for (int j = 0; j < NUM_PUTS; j++) {
        byte[] value = new byte[VALUE_SIZE];
        rand.nextBytes(value);
        table.put(new Put(Bytes.toBytes(i * 10 + j)).addColumn(family, qualifier, value));
      }
      long startTime = System.nanoTime();
      hbtu.getHBaseAdmin().flush(tableName);
      duration += System.nanoTime() - startTime;
    }
    Store store = getStoreWithName(tableName);
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
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(family));
    htd.setCompactionEnabled(false);
    hbtu.getHBaseAdmin().createTable(htd);
    hbtu.waitTableAvailable(tableName);
    HRegionServer regionServer = hbtu.getRSForFirstRegionInTable(tableName);
    PressureAwareFlushThroughputController throughputController =
        (PressureAwareFlushThroughputController) regionServer.getFlushThroughputController();
    for (Region region : regionServer.getOnlineRegions()) {
      region.flush(true);
    }
    assertEquals(0.0, regionServer.getFlushPressure(), EPSILON);
    Thread.sleep(5000);
    assertEquals(10L * 1024 * 1024, throughputController.getMaxThroughput(), EPSILON);
    Table table = conn.getTable(tableName);
    Random rand = new Random();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        byte[] value = new byte[256 * 1024];
        rand.nextBytes(value);
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
