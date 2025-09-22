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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HConstants.HFILE_BLOCK_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.HConstants.ROW_CACHE_ACTIVATE_MIN_HFILES_KEY;
import static org.apache.hadoop.hbase.HConstants.ROW_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.ROW_CACHE_HIT_COUNT;
import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRowCacheConfigurationObserver {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowCacheConfigurationObserver.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final byte[] CF1 = Bytes.toBytes("cf1");
  private static final byte[] Q1 = Bytes.toBytes("q1");

  private static MetricsAssertHelper metricsHelper;
  private static MetricsRegionServer metricsRegionServer;
  private static MetricsRegionServerSource serverSource;

  private static Admin admin;

  private TableName tableName;
  private Table table;
  private final Map<String, Long> counterBase = new HashMap<>();

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    // Enable row cache but reduce the block cache size to fit in 80% of the heap
    conf.setFloat(ROW_CACHE_SIZE_KEY, 0.01f);
    conf.setFloat(HFILE_BLOCK_CACHE_SIZE_KEY, 0.38f);

    // Set a value different from the default
    conf.setInt(ROW_CACHE_ACTIVATE_MIN_HFILES_KEY, 0);
    SingleProcessHBaseCluster cluster = TEST_UTIL.startMiniCluster();
    cluster.waitForActiveAndReadyMaster();
    admin = TEST_UTIL.getAdmin();

    metricsHelper = CompatibilityFactory.getInstance(MetricsAssertHelper.class);
    metricsRegionServer = cluster.getRegionServer(0).getMetrics();
    serverSource = metricsRegionServer.getMetricsSource();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HRegionServer.TEST_SKIP_REPORTING_TRANSITION = false;
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void beforeTestMethod() throws Exception {
    ColumnFamilyDescriptor cf1 = ColumnFamilyDescriptorBuilder.newBuilder(CF1).build();

    tableName = TableName.valueOf(testName.getMethodName());
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName).setRowCacheEnabled(true)
      .setColumnFamily(cf1).build();
    admin.createTable(td);
    table = admin.getConnection().getTable(tableName);
  }

  @After
  public void afterTestMethod() throws Exception {
    counterBase.clear();

    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  private void setCounterBase(String metric, long value) {
    counterBase.put(metric, value);
  }

  private void assertCounterDiff(String metric, long diff) {
    Long base = counterBase.get(metric);
    if (base == null) {
      throw new IllegalStateException(
        "base counter of " + metric + " metric should have been set before by setCounterBase()");
    }
    long newValue = base + diff;
    metricsHelper.assertCounter(metric, newValue, serverSource);
    counterBase.put(metric, newValue);
  }

  private static void recomputeMetrics() {
    metricsRegionServer.getRegionServerWrapper().forceRecompute();
  }

  /**
   * If the number of HFiles is below the configured minimum, row caching has no effect.
   */
  @Test
  public void testRowCacheWithHFilesCount() throws IOException {
    // Change ROW_CACHE_ACTIVATE_MIN_HFILES_KEY online
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(ROW_CACHE_ACTIVATE_MIN_HFILES_KEY, 2);
    for (ServerName serverName : admin.getRegionServers()) {
      HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(serverName);
      regionServer.getConfigurationManager().notifyAllObservers(conf);
    }

    byte[] rowKey = "row".getBytes();
    Get get = new Get(rowKey);
    Result result;

    // Initialize metrics
    recomputeMetrics();
    setCounterBase("Get_num_ops", metricsHelper.getCounter("Get_num_ops", serverSource));
    setCounterBase(ROW_CACHE_HIT_COUNT,
      metricsHelper.getCounter(ROW_CACHE_HIT_COUNT, serverSource));

    Put put = new Put(rowKey);
    put.addColumn(CF1, Q1, "11".getBytes());
    table.put(put);

    // The row cache is not populated yet, as the store file count is 0
    result = table.get(get);
    recomputeMetrics();
    assertArrayEquals(rowKey, result.getRow());
    assertArrayEquals("11".getBytes(), result.getValue(CF1, Q1));
    assertCounterDiff("Get_num_ops", 1);
    assertCounterDiff(ROW_CACHE_HIT_COUNT, 0);

    // Flush, 1 store file exists
    TEST_UTIL.getAdmin().flush(tableName);

    // The row cache is not populated yet, as the store file count is 1
    result = table.get(get);
    recomputeMetrics();
    assertArrayEquals(rowKey, result.getRow());
    assertArrayEquals("11".getBytes(), result.getValue(CF1, Q1));
    assertCounterDiff("Get_num_ops", 1);
    assertCounterDiff(ROW_CACHE_HIT_COUNT, 0);

    // Flush, 2(ROW_CACHE_ACTIVATE_MIN_HFILES_DEFAULT) store files exist
    table.put(put);
    TEST_UTIL.getAdmin().flush(tableName);

    // The row cache is populated now, as the store file count is 2.
    // But the row cache is not hit yet, it will be hit only after this operation.
    result = table.get(get);
    recomputeMetrics();
    assertArrayEquals(rowKey, result.getRow());
    assertArrayEquals("11".getBytes(), result.getValue(CF1, Q1));
    assertCounterDiff("Get_num_ops", 1);
    assertCounterDiff(ROW_CACHE_HIT_COUNT, 0);

    // Get again, this time the row cache is hit
    result = table.get(get);
    recomputeMetrics();
    assertArrayEquals(rowKey, result.getRow());
    assertArrayEquals("11".getBytes(), result.getValue(CF1, Q1));
    assertCounterDiff("Get_num_ops", 1);
    assertCounterDiff(ROW_CACHE_HIT_COUNT, 1);
  }
}
