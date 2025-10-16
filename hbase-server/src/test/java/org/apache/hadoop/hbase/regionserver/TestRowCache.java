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
import static org.apache.hadoop.hbase.HConstants.ROW_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.ROW_CACHE_EVICTED_ROW_COUNT;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.ROW_CACHE_HIT_COUNT;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.ROW_CACHE_MISS_COUNT;
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
  private static RowCache rowCache;

  private TableName tableName;
  private Table table;
  HRegion region;
  private final Map<String, Long> counterBase = new HashMap<>();

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    // Enable row cache but reduce the block cache size to fit in 80% of the heap
    conf.setFloat(ROW_CACHE_SIZE_KEY, 0.01f);
    conf.setFloat(HFILE_BLOCK_CACHE_SIZE_KEY, 0.39f);

    SingleProcessHBaseCluster cluster = TEST_UTIL.startMiniCluster();
    cluster.waitForActiveAndReadyMaster();
    admin = TEST_UTIL.getAdmin();

    metricsHelper = CompatibilityFactory.getInstance(MetricsAssertHelper.class);
    metricsRegionServer = cluster.getRegionServer(0).getMetrics();
    serverSource = metricsRegionServer.getMetricsSource();

    rowCache = TEST_UTIL.getHBaseCluster().getRegionServer(0).getRSRpcServices()
      .getRowCacheService().getRowCache();
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
    region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegions().stream()
      .filter(r -> r.getRegionInfo().getTable().equals(tableName)).findFirst().orElseThrow();
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
  public void testGetWithRowCache() throws IOException {
    byte[] rowKey = "row".getBytes();
    Get get = new Get(rowKey);
    Result result;

    RowCacheKey rowCacheKey = new RowCacheKey(region, rowKey);

    // Put a row
    Put put = new Put(rowKey);
    put.addColumn(CF1, Q1, Bytes.toBytes(0L));
    put.addColumn(CF1, Q2, "12".getBytes());
    put.addColumn(CF2, Q1, "21".getBytes());
    put.addColumn(CF2, Q2, "22".getBytes());
    table.put(put);
    admin.flush(tableName);

    // Initialize metrics
    recomputeMetrics();
    setCounterBase("Get_num_ops", metricsHelper.getCounter("Get_num_ops", serverSource));
    setCounterBase(ROW_CACHE_HIT_COUNT,
      metricsHelper.getCounter(ROW_CACHE_HIT_COUNT, serverSource));
    setCounterBase(ROW_CACHE_MISS_COUNT,
      metricsHelper.getCounter(ROW_CACHE_MISS_COUNT, serverSource));
    setCounterBase(ROW_CACHE_EVICTED_ROW_COUNT,
      metricsHelper.getCounter(ROW_CACHE_EVICTED_ROW_COUNT, serverSource));

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
    assertCounterDiff(ROW_CACHE_HIT_COUNT, 0);
    assertCounterDiff(ROW_CACHE_MISS_COUNT, 1);

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
    assertCounterDiff(ROW_CACHE_HIT_COUNT, 1);
    assertCounterDiff(ROW_CACHE_MISS_COUNT, 0);

    // Row cache is invalidated by the put operation
    assertNotNull(rowCache.getRow(rowCacheKey, true));
    table.put(put);
    recomputeMetrics();
    assertCounterDiff(ROW_CACHE_HIT_COUNT, 1);
    assertCounterDiff(ROW_CACHE_MISS_COUNT, 0);
    assertCounterDiff(ROW_CACHE_EVICTED_ROW_COUNT, 1);

    // Get is executed without the row cache; however, the cache is re-populated as a result
    result = table.get(get);
    recomputeMetrics();
    assertArrayEquals(rowKey, result.getRow());
    assertCounterDiff("Get_num_ops", 1);
    // Ensure the get operation not from the row cache
    assertCounterDiff(ROW_CACHE_HIT_COUNT, 0);
    assertCounterDiff(ROW_CACHE_MISS_COUNT, 1);
    assertCounterDiff(ROW_CACHE_EVICTED_ROW_COUNT, 0);

    // Get again with the row cache
    result = table.get(get);
    recomputeMetrics();
    assertArrayEquals(rowKey, result.getRow());
    assertCounterDiff("Get_num_ops", 1);
    // Ensure the get operation from the row cache
    assertCounterDiff(ROW_CACHE_HIT_COUNT, 1);
    assertCounterDiff(ROW_CACHE_MISS_COUNT, 0);
    assertCounterDiff(ROW_CACHE_EVICTED_ROW_COUNT, 0);

    // Row cache is invalidated by the increment operation
    assertNotNull(rowCache.getRow(rowCacheKey, true));
    table.incrementColumnValue(rowKey, CF1, Q1, 1);
    assertNull(rowCache.getRow(rowCacheKey, true));

    // Get is executed without the row cache; however, the cache is re-populated as a result
    table.get(get);
    assertNotNull(rowCache.getRow(rowCacheKey, true));

    // Row cache is invalidated by the append operation
    assertNotNull(rowCache.getRow(rowCacheKey, true));
    Append append = new Append(rowKey);
    append.addColumn(CF1, Q1, Bytes.toBytes(0L));
    table.append(append);
    assertNull(rowCache.getRow(rowCacheKey, true));

    // Get is executed without the row cache; however, the cache is re-populated as a result
    table.get(get);
    assertNotNull(rowCache.getRow(rowCacheKey, true));

    // Row cache is invalidated by the delete operation
    assertNotNull(rowCache.getRow(rowCacheKey, true));
    Delete delete = new Delete(rowKey);
    delete.addColumn(CF1, Q1);
    table.delete(delete);
    assertNull(rowCache.getRow(rowCacheKey, true));
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
  public void testCheckAndMutate() throws IOException {
    byte[] rowKey = "row".getBytes();
    Get get = new Get(rowKey);
    Result result;
    CheckAndMutate cam;
    CheckAndMutateResult camResult;

    RowCacheKey rowCacheKey = new RowCacheKey(region, rowKey);

    // Put a row
    Put put1 = new Put(rowKey);
    put1.addColumn(CF1, Q1, "11".getBytes());
    put1.addColumn(CF1, Q2, "12".getBytes());
    table.put(put1);
    admin.flush(tableName);

    // Validate that the row cache is populated
    result = table.get(get);
    assertNotNull(rowCache.getRow(rowCacheKey, true));
    assertArrayEquals("11".getBytes(), result.getValue(CF1, Q1));
    assertArrayEquals("12".getBytes(), result.getValue(CF1, Q2));

    // The row cache is not invalidated when a checkAndMutate operation fails
    Put put2 = new Put(rowKey);
    put2.addColumn(CF1, Q2, "1212".getBytes());
    cam = CheckAndMutate.newBuilder(rowKey).ifEquals(CF1, Q2, "00".getBytes()).build(put2);
    camResult = table.checkAndMutate(cam);
    assertFalse(camResult.isSuccess());
    assertNull(rowCache.getRow(rowCacheKey, true));

    // Validate that the row cache is populated
    result = table.get(get);
    assertNotNull(rowCache.getRow(rowCacheKey, true));
    assertArrayEquals("11".getBytes(), result.getValue(CF1, Q1));
    assertArrayEquals("12".getBytes(), result.getValue(CF1, Q2));

    // The row cache is invalidated by a checkAndMutate operation
    cam = CheckAndMutate.newBuilder(rowKey).ifEquals(CF1, Q2, "12".getBytes()).build(put2);
    camResult = table.checkAndMutate(cam);
    assertTrue(camResult.isSuccess());
    assertNull(rowCache.getRow(rowCacheKey, true));
  }

  @Test
  public void testCheckAndMutates() throws IOException {
    byte[] rowKey1 = "row1".getBytes();
    byte[] rowKey2 = "row2".getBytes();
    Get get1 = new Get(rowKey1);
    Get get2 = new Get(rowKey2);
    Result result1, result2;
    List<CheckAndMutate> cams;
    List<CheckAndMutateResult> camResults;

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
    admin.flush(tableName);

    // Validate that the row caches are populated
    result1 = table.get(get1);
    assertNotNull(rowCache.getRow(rowCacheKey1, true));
    assertArrayEquals("111".getBytes(), result1.getValue(CF1, Q1));
    assertArrayEquals("112".getBytes(), result1.getValue(CF1, Q2));
    result2 = table.get(get2);
    assertNotNull(rowCache.getRow(rowCacheKey2, true));
    assertArrayEquals("211".getBytes(), result2.getValue(CF1, Q1));
    assertArrayEquals("212".getBytes(), result2.getValue(CF1, Q2));

    // The row caches are invalidated by checkAndMutate operations
    cams = new ArrayList<>();
    cams.add(CheckAndMutate.newBuilder(rowKey1).ifEquals(CF1, Q2, "112".getBytes()).build(put1));
    cams.add(CheckAndMutate.newBuilder(rowKey2).ifEquals(CF1, Q2, "212".getBytes()).build(put2));
    camResults = table.checkAndMutate(cams);
    assertTrue(camResults.get(0).isSuccess());
    assertTrue(camResults.get(1).isSuccess());
    assertNull(rowCache.getRow(rowCacheKey1, true));
    assertNull(rowCache.getRow(rowCacheKey2, true));
  }

  @Test
  public void testRowMutations() throws IOException {
    byte[] rowKey1 = "row1".getBytes();
    byte[] rowKey2 = "row2".getBytes();
    Get get1 = new Get(rowKey1);
    Get get2 = new Get(rowKey2);
    Result result1, result2;

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
    admin.flush(tableName);

    // Validate that the row caches are populated
    result1 = table.get(get1);
    assertNotNull(rowCache.getRow(rowCacheKey1, true));
    assertArrayEquals("111".getBytes(), result1.getValue(CF1, Q1));
    assertArrayEquals("112".getBytes(), result1.getValue(CF1, Q2));
    result2 = table.get(get2);
    assertNotNull(rowCache.getRow(rowCacheKey1, true));
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
    assertNull(rowCache.getRow(rowCacheKey1, true));
    assertNotNull(rowCache.getRow(rowCacheKey2, true));

    // Validate that the row caches are populated
    result1 = table.get(get1);
    assertNotNull(rowCache.getRow(rowCacheKey1, true));
    assertArrayEquals("111111".getBytes(), result1.getValue(CF1, Q1));
    assertArrayEquals("112112".getBytes(), result1.getValue(CF1, Q2));
    result2 = table.get(get2);
    assertNotNull(rowCache.getRow(rowCacheKey1, true));
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
    admin.flush(tableName);

    // Validate that the row caches are populated
    batchOperations = new ArrayList<>();
    batchOperations.add(get1);
    batchOperations.add(get2);
    batchOperations.add(get3);
    results = new Object[batchOperations.size()];
    table.batch(batchOperations, results);
    assertEquals(3, results.length);
    assertNotNull(rowCache.getRow(rowCacheKey1, true));
    assertArrayEquals("111".getBytes(), ((Result) results[0]).getValue(CF1, Q1));
    assertArrayEquals("112".getBytes(), ((Result) results[0]).getValue(CF1, Q2));
    assertNotNull(rowCache.getRow(rowCacheKey2, true));
    assertArrayEquals("211".getBytes(), ((Result) results[1]).getValue(CF1, Q1));
    assertArrayEquals("212".getBytes(), ((Result) results[1]).getValue(CF1, Q2));
    assertNotNull(rowCache.getRow(rowCacheKey3, true));
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
    assertNull(rowCache.getRow(rowCacheKey1, true));
    assertNull(rowCache.getRow(rowCacheKey2, true));
    assertNotNull(rowCache.getRow(rowCacheKey3, true));
  }

  @Test
  public void testGetFromMemstoreOnly() throws IOException, InterruptedException {
    byte[] rowKey = "row".getBytes();
    RowCacheKey rowCacheKey = new RowCacheKey(region, rowKey);

    // Put a row into memstore only, not flushed to HFile yet
    Put put = new Put(rowKey);
    put.addColumn(CF1, Q1, Bytes.toBytes(0L));
    table.put(put);

    // Get from memstore only
    Get get = new Get(rowKey);
    table.get(get);

    // Validate that the row cache is not populated
    assertNull(rowCache.getRow(rowCacheKey, true));

    // Flush memstore to HFile, then get again
    admin.flush(tableName);
    get = new Get(rowKey);
    table.get(get);

    // Validate that the row cache is populated now
    assertNotNull(rowCache.getRow(rowCacheKey, true));

    // Put another qualifier. And now the cells are in both memstore and HFile.
    put = new Put(rowKey);
    put.addColumn(CF1, Q2, Bytes.toBytes(0L));
    table.put(put);

    // Validate that the row cache is invalidated
    assertNull(rowCache.getRow(rowCacheKey, true));

    // Get from memstore and HFile
    get = new Get(rowKey);
    table.get(get);
    assertNotNull(rowCache.getRow(rowCacheKey, true));
  }
}
