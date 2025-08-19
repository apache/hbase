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

import static org.apache.hadoop.hbase.HConstants.ROW_CACHE_ACTIVATE_MIN_HFILES_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.RowCacheKey;
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
public class TestRowCache {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowCache.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final byte[] CF1 = Bytes.toBytes("cf1");
  private static final byte[] CF2 = Bytes.toBytes("cf2");
  private static final byte[] Q1 = Bytes.toBytes("q1");
  private static final byte[] Q2 = Bytes.toBytes("q2");

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
    // To test simply, regardless of the number of HFiles
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
    // To test data block encoding
    ColumnFamilyDescriptor cf2 = ColumnFamilyDescriptorBuilder.newBuilder(CF2)
      .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF).build();

    tableName = TableName.valueOf(testName.getMethodName());
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName).setRowCacheEnabled(true)
      .setColumnFamily(cf1).setColumnFamily(cf2).build();
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

  @Test
  public void testGetWithRowCache() throws IOException, InterruptedException {
    byte[] rowKey = "row".getBytes();
    Get get = new Get(rowKey);
    Result result;

    HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegions().stream()
      .filter(r -> r.getRegionInfo().getTable().equals(tableName)).findFirst().orElseThrow();
    RowCacheKey rowCacheKey = new RowCacheKey(region, rowKey);

    // Put a row
    Put put = new Put(rowKey);
    put.addColumn(CF1, Q1, Bytes.toBytes(0L));
    put.addColumn(CF1, Q2, "12".getBytes());
    put.addColumn(CF2, Q1, "21".getBytes());
    put.addColumn(CF2, Q2, "22".getBytes());
    table.put(put);

    // Initialize metrics
    recomputeMetrics();
    setCounterBase("Get_num_ops", metricsHelper.getCounter("Get_num_ops", serverSource));
    setCounterBase("blockCacheRowHitCount",
      metricsHelper.getCounter("blockCacheRowHitCount", serverSource));

    // First get to populate the row cache
    result = table.get(get);
    recomputeMetrics();
    assertArrayEquals(rowKey, result.getRow());
    assertArrayEquals(Bytes.toBytes(0L), result.getValue(CF1, Q1));
    assertArrayEquals("12".getBytes(), result.getValue(CF1, Q2));
    assertArrayEquals("21".getBytes(), result.getValue(CF2, Q1));
    assertArrayEquals("22".getBytes(), result.getValue(CF2, Q2));
    assertCounterDiff("Get_num_ops", 1);
    // Ensure the get operation from HFile without row cache
    assertCounterDiff("blockCacheRowHitCount", 0);

    // Get from the row cache
    result = table.get(get);
    recomputeMetrics();
    assertArrayEquals(rowKey, result.getRow());
    assertArrayEquals(Bytes.toBytes(0L), result.getValue(CF1, Q1));
    assertArrayEquals("12".getBytes(), result.getValue(CF1, Q2));
    assertArrayEquals("21".getBytes(), result.getValue(CF2, Q1));
    assertArrayEquals("22".getBytes(), result.getValue(CF2, Q2));
    assertCounterDiff("Get_num_ops", 1);
    // Ensure the get operation from the row cache
    assertCounterDiff("blockCacheRowHitCount", 1);

    // Row cache is invalidated by the put operation
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));
    table.put(put);
    assertNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));

    // Get is executed without the row cache; however, the cache is re-populated as a result
    result = table.get(get);
    recomputeMetrics();
    assertArrayEquals(rowKey, result.getRow());
    assertCounterDiff("Get_num_ops", 1);
    // Ensure the get operation not from the row cache
    assertCounterDiff("blockCacheRowHitCount", 0);

    // Get again with the row cache
    result = table.get(get);
    recomputeMetrics();
    assertArrayEquals(rowKey, result.getRow());
    assertCounterDiff("Get_num_ops", 1);
    // Ensure the get operation from the row cache
    assertCounterDiff("blockCacheRowHitCount", 1);

    // Row cache is invalidated by the increment operation
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));
    table.incrementColumnValue(rowKey, CF1, Q1, 1);
    assertNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));

    // Get is executed without the row cache; however, the cache is re-populated as a result
    table.get(get);
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));

    // Row cache is invalidated by the append operation
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));
    Append append = new Append(rowKey);
    append.addColumn(CF1, Q1, Bytes.toBytes(0L));
    table.append(append);
    assertNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));

    // Get is executed without the row cache; however, the cache is re-populated as a result
    table.get(get);
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));

    // Row cache is invalidated by the delete operation
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));
    Delete delete = new Delete(rowKey);
    delete.addColumn(CF1, Q1);
    table.delete(delete);
    assertNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPutWithTTL() throws IOException {
    // Put with TTL is not allowed on tables with row cache enabled, because cached rows cannot
    // track TTL expiration
    Put put = new Put("row".getBytes());
    put.addColumn(CF1, Q1, "11".getBytes());
    put.setTTL(1);
    table.put(put);
  }

  @Test
  public void testCheckAndMutate() throws IOException, InterruptedException {
    byte[] rowKey = "row".getBytes();
    Get get = new Get(rowKey);
    Result result;
    CheckAndMutate cam;
    CheckAndMutateResult camResult;

    HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegions().stream()
      .filter(r -> r.getRegionInfo().getTable().equals(tableName)).findFirst().orElseThrow();
    RowCacheKey rowCacheKey = new RowCacheKey(region, rowKey);

    // Put a row
    Put put1 = new Put(rowKey);
    put1.addColumn(CF1, Q1, "11".getBytes());
    put1.addColumn(CF1, Q2, "12".getBytes());
    table.put(put1);

    // Validate that the row cache is populated
    result = table.get(get);
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));
    assertArrayEquals("11".getBytes(), result.getValue(CF1, Q1));
    assertArrayEquals("12".getBytes(), result.getValue(CF1, Q2));

    // The row cache is not invalidated when a checkAndMutate operation fails
    Put put2 = new Put(rowKey);
    put2.addColumn(CF1, Q2, "1212".getBytes());
    cam = CheckAndMutate.newBuilder(rowKey).ifEquals(CF1, Q2, "00".getBytes()).build(put2);
    camResult = table.checkAndMutate(cam);
    assertFalse(camResult.isSuccess());
    assertNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));

    // Validate that the row cache is populated
    result = table.get(get);
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));
    assertArrayEquals("11".getBytes(), result.getValue(CF1, Q1));
    assertArrayEquals("12".getBytes(), result.getValue(CF1, Q2));

    // The row cache is invalidated by a checkAndMutate operation
    cam = CheckAndMutate.newBuilder(rowKey).ifEquals(CF1, Q2, "12".getBytes()).build(put2);
    camResult = table.checkAndMutate(cam);
    assertTrue(camResult.isSuccess());
    assertNull(region.getBlockCache().getBlock(rowCacheKey, false, false, false));
  }

  @Test
  public void testCheckAndMutates() throws IOException, InterruptedException {
    byte[] rowKey1 = "row1".getBytes();
    byte[] rowKey2 = "row2".getBytes();
    Get get1 = new Get(rowKey1);
    Get get2 = new Get(rowKey2);
    Result result1, result2;
    List<CheckAndMutate> cams;
    List<CheckAndMutateResult> camResults;

    HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegions().stream()
      .filter(r -> r.getRegionInfo().getTable().equals(tableName)).findFirst().orElseThrow();
    RowCacheKey rowCacheKey1 = new RowCacheKey(region, rowKey1);
    RowCacheKey rowCacheKey2 = new RowCacheKey(region, rowKey2);

    // Put rows
    Put put1 = new Put(rowKey1);
    put1.addColumn(CF1, Q1, "111".getBytes());
    put1.addColumn(CF1, Q2, "112".getBytes());
    table.put(put1);
    Put put2 = new Put(rowKey2);
    put2.addColumn(CF1, Q1, "211".getBytes());
    put2.addColumn(CF1, Q2, "212".getBytes());
    table.put(put2);

    // Validate that the row caches are populated
    result1 = table.get(get1);
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey1, false, false, false));
    assertArrayEquals("111".getBytes(), result1.getValue(CF1, Q1));
    assertArrayEquals("112".getBytes(), result1.getValue(CF1, Q2));
    result2 = table.get(get2);
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey2, false, false, false));
    assertArrayEquals("211".getBytes(), result2.getValue(CF1, Q1));
    assertArrayEquals("212".getBytes(), result2.getValue(CF1, Q2));

    // The row caches are invalidated by checkAndMutate operations
    cams = new ArrayList<>();
    cams.add(CheckAndMutate.newBuilder(rowKey1).ifEquals(CF1, Q2, "112".getBytes()).build(put1));
    cams.add(CheckAndMutate.newBuilder(rowKey2).ifEquals(CF1, Q2, "212".getBytes()).build(put2));
    camResults = table.checkAndMutate(cams);
    assertTrue(camResults.get(0).isSuccess());
    assertTrue(camResults.get(1).isSuccess());
    assertNull(region.getBlockCache().getBlock(rowCacheKey1, false, false, false));
    assertNull(region.getBlockCache().getBlock(rowCacheKey2, false, false, false));
  }

  @Test
  public void testRowMutations() throws IOException, InterruptedException {
    byte[] rowKey1 = "row1".getBytes();
    byte[] rowKey2 = "row2".getBytes();
    Get get1 = new Get(rowKey1);
    Get get2 = new Get(rowKey2);
    Result result1, result2;

    HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegions().stream()
      .filter(r -> r.getRegionInfo().getTable().equals(tableName)).findFirst().orElseThrow();
    RowCacheKey rowCacheKey1 = new RowCacheKey(region, rowKey1);
    RowCacheKey rowCacheKey2 = new RowCacheKey(region, rowKey2);

    // Put rows
    Put put1 = new Put(rowKey1);
    put1.addColumn(CF1, Q1, "111".getBytes());
    put1.addColumn(CF1, Q2, "112".getBytes());
    table.put(put1);
    Put put2 = new Put(rowKey2);
    put2.addColumn(CF1, Q1, "211".getBytes());
    put2.addColumn(CF1, Q2, "212".getBytes());
    table.put(put2);

    // Validate that the row caches are populated
    result1 = table.get(get1);
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey1, false, false, false));
    assertArrayEquals("111".getBytes(), result1.getValue(CF1, Q1));
    assertArrayEquals("112".getBytes(), result1.getValue(CF1, Q2));
    result2 = table.get(get2);
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey1, false, false, false));
    assertArrayEquals("211".getBytes(), result2.getValue(CF1, Q1));
    assertArrayEquals("212".getBytes(), result2.getValue(CF1, Q2));

    // The row caches are invalidated by batch operation
    Put put12 = new Put(rowKey1);
    put12.addColumn(CF1, Q1, "111111".getBytes());
    Put put13 = new Put(rowKey1);
    put13.addColumn(CF1, Q2, "112112".getBytes());
    RowMutations rms = new RowMutations(rowKey1);
    rms.add(put12);
    rms.add(put13);
    CheckAndMutate cam =
      CheckAndMutate.newBuilder(rowKey1).ifEquals(CF1, Q1, "111".getBytes()).build(rms);
    table.checkAndMutate(cam);
    assertNull(region.getBlockCache().getBlock(rowCacheKey1, false, false, false));
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey2, false, false, false));

    // Validate that the row caches are populated
    result1 = table.get(get1);
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey1, false, false, false));
    assertArrayEquals("111111".getBytes(), result1.getValue(CF1, Q1));
    assertArrayEquals("112112".getBytes(), result1.getValue(CF1, Q2));
    result2 = table.get(get2);
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey1, false, false, false));
    assertArrayEquals("211".getBytes(), result2.getValue(CF1, Q1));
    assertArrayEquals("212".getBytes(), result2.getValue(CF1, Q2));
  }

  @Test
  public void testBatch() throws IOException, InterruptedException {
    byte[] rowKey1 = "row1".getBytes();
    byte[] rowKey2 = "row2".getBytes();
    byte[] rowKey3 = "row3".getBytes();
    Get get1 = new Get(rowKey1);
    Get get2 = new Get(rowKey2);
    Get get3 = new Get(rowKey3);
    List<Row> batchOperations;
    Object[] results;

    HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegions().stream()
      .filter(r -> r.getRegionInfo().getTable().equals(tableName)).findFirst().orElseThrow();
    RowCacheKey rowCacheKey1 = new RowCacheKey(region, rowKey1);
    RowCacheKey rowCacheKey2 = new RowCacheKey(region, rowKey2);
    RowCacheKey rowCacheKey3 = new RowCacheKey(region, rowKey3);

    // Put rows
    batchOperations = new ArrayList<>();
    Put put1 = new Put(rowKey1);
    put1.addColumn(CF1, Q1, "111".getBytes());
    put1.addColumn(CF1, Q2, "112".getBytes());
    batchOperations.add(put1);
    Put put2 = new Put(rowKey2);
    put2.addColumn(CF1, Q1, "211".getBytes());
    put2.addColumn(CF1, Q2, "212".getBytes());
    batchOperations.add(put2);
    Put put3 = new Put(rowKey3);
    put3.addColumn(CF1, Q1, "311".getBytes());
    put3.addColumn(CF1, Q2, "312".getBytes());
    batchOperations.add(put3);
    results = new Result[batchOperations.size()];
    table.batch(batchOperations, results);

    // Validate that the row caches are populated
    batchOperations = new ArrayList<>();
    batchOperations.add(get1);
    batchOperations.add(get2);
    batchOperations.add(get3);
    results = new Object[batchOperations.size()];
    table.batch(batchOperations, results);
    assertEquals(3, results.length);
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey1, false, false, false));
    assertArrayEquals("111".getBytes(), ((Result) results[0]).getValue(CF1, Q1));
    assertArrayEquals("112".getBytes(), ((Result) results[0]).getValue(CF1, Q2));
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey2, false, false, false));
    assertArrayEquals("211".getBytes(), ((Result) results[1]).getValue(CF1, Q1));
    assertArrayEquals("212".getBytes(), ((Result) results[1]).getValue(CF1, Q2));
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey3, false, false, false));
    assertArrayEquals("311".getBytes(), ((Result) results[2]).getValue(CF1, Q1));
    assertArrayEquals("312".getBytes(), ((Result) results[2]).getValue(CF1, Q2));

    // The row caches are invalidated by batch operation
    batchOperations = new ArrayList<>();
    batchOperations.add(put1);
    Put put2New = new Put(rowKey2);
    put2New.addColumn(CF1, Q1, "211211".getBytes());
    put2New.addColumn(CF1, Q2, "212".getBytes());
    CheckAndMutate cam =
      CheckAndMutate.newBuilder(rowKey2).ifEquals(CF1, Q1, "211".getBytes()).build(put2New);
    batchOperations.add(cam);
    results = new Object[batchOperations.size()];
    table.batch(batchOperations, results);
    assertEquals(2, results.length);
    assertNull(region.getBlockCache().getBlock(rowCacheKey1, false, false, false));
    assertNull(region.getBlockCache().getBlock(rowCacheKey2, false, false, false));
    assertNotNull(region.getBlockCache().getBlock(rowCacheKey3, false, false, false));
  }
}
