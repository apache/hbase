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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@RunWith(Parameterized.class)
@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableScanMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncTableScanMetrics.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final TableName TABLE_NAME = TableName.valueOf("ScanMetrics");

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ = Bytes.toBytes("cq");

  private static final byte[] VALUE = Bytes.toBytes("value");

  private static AsyncConnection CONN;

  private static int NUM_REGIONS;

  @FunctionalInterface
  private interface ScanWithMetrics {
    Pair<List<Result>, ScanMetrics> scan(Scan scan) throws Exception;
  }

  @Parameter(0)
  public String methodName;

  @Parameter(1)
  public ScanWithMetrics method;

  @Parameters(name = "{index}: scan={0}")
  public static List<Object[]> params() {
    ScanWithMetrics doScanWithRawAsyncTable = TestAsyncTableScanMetrics::doScanWithRawAsyncTable;
    ScanWithMetrics doScanWithAsyncTableScan = TestAsyncTableScanMetrics::doScanWithAsyncTableScan;
    ScanWithMetrics doScanWithAsyncTableScanner =
        TestAsyncTableScanMetrics::doScanWithAsyncTableScanner;
    return Arrays.asList(new Object[] { "doScanWithRawAsyncTable", doScanWithRawAsyncTable },
      new Object[] { "doScanWithAsyncTableScan", doScanWithAsyncTableScan },
      new Object[] { "doScanWithAsyncTableScanner", doScanWithAsyncTableScanner });
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(3);
    // Create 3 rows in the table, with rowkeys starting with "zzz*" so that
    // scan are forced to hit all the regions.
    try (Table table = UTIL.createMultiRegionTable(TABLE_NAME, CF)) {
      table.put(Arrays.asList(new Put(Bytes.toBytes("zzz1")).addColumn(CF, CQ, VALUE),
        new Put(Bytes.toBytes("zzz2")).addColumn(CF, CQ, VALUE),
        new Put(Bytes.toBytes("zzz3")).addColumn(CF, CQ, VALUE)));
    }
    CONN = ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get();
    NUM_REGIONS = UTIL.getHBaseCluster().getRegions(TABLE_NAME).size();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  private static Pair<List<Result>, ScanMetrics> doScanWithRawAsyncTable(Scan scan)
      throws IOException, InterruptedException {
    BufferingScanResultConsumer consumer = new BufferingScanResultConsumer();
    CONN.getTable(TABLE_NAME).scan(scan, consumer);
    List<Result> results = new ArrayList<>();
    for (Result result; (result = consumer.take()) != null;) {
      results.add(result);
    }
    return Pair.newPair(results, consumer.getScanMetrics());
  }

  private static Pair<List<Result>, ScanMetrics> doScanWithAsyncTableScan(Scan scan)
      throws Exception {
    SimpleScanResultConsumerImpl consumer = new SimpleScanResultConsumerImpl();
    CONN.getTable(TABLE_NAME, ForkJoinPool.commonPool()).scan(scan, consumer);
    return Pair.newPair(consumer.getAll(), consumer.getScanMetrics());
  }

  private static Pair<List<Result>, ScanMetrics> doScanWithAsyncTableScanner(Scan scan)
      throws IOException {
    try (ResultScanner scanner =
        CONN.getTable(TABLE_NAME, ForkJoinPool.commonPool()).getScanner(scan)) {
      List<Result> results = new ArrayList<>();
      for (Result result; (result = scanner.next()) != null;) {
        results.add(result);
      }
      return Pair.newPair(results, scanner.getScanMetrics());
    }
  }

  @Test
  public void testNoScanMetrics() throws Exception {
    Pair<List<Result>, ScanMetrics> pair = method.scan(new Scan());
    assertEquals(3, pair.getFirst().size());
    assertNull(pair.getSecond());
  }

  @Test
  public void testScanMetrics() throws Exception {
    Pair<List<Result>, ScanMetrics> pair = method.scan(new Scan().setScanMetricsEnabled(true));
    List<Result> results = pair.getFirst();
    assertEquals(3, results.size());
    long bytes = results.stream().flatMap(r -> Arrays.asList(r.rawCells()).stream())
        .mapToLong(c -> PrivateCellUtil.estimatedSerializedSizeOf(c)).sum();
    ScanMetrics scanMetrics = pair.getSecond();
    assertEquals(NUM_REGIONS, scanMetrics.countOfRegions.get());
    assertEquals(bytes, scanMetrics.countOfBytesInResults.get());
    assertEquals(NUM_REGIONS, scanMetrics.countOfRPCcalls.get());
    // also assert a server side metric to ensure that we have published them into the client side
    // metrics.
    assertEquals(3, scanMetrics.countOfRowsScanned.get());
  }
}
