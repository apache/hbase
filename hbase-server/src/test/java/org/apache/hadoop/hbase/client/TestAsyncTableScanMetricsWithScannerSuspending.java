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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.TestAsyncTableScanMetrics.getBytesOfResults;
import static org.apache.hadoop.hbase.client.metrics.ScanMetrics.BYTES_IN_RESULTS_METRIC_NAME;
import static org.apache.hadoop.hbase.client.metrics.ScanMetrics.REGIONS_SCANNED_METRIC_NAME;
import static org.apache.hadoop.hbase.client.metrics.ScanMetrics.RPC_CALLS_METRIC_NAME;
import static org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics.COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.metrics.ScanMetricsRegionInfo;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableScanMetricsWithScannerSuspending {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableScanMetricsWithScannerSuspending.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final TableName TABLE_NAME =
    TableName.valueOf(TestAsyncTableScanMetricsWithScannerSuspending.class.getSimpleName());

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ = Bytes.toBytes("cq");

  private static final byte[] VALUE = Bytes.toBytes("value");

  private static AsyncConnection CONN;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
    // Create 3 rows in the table, with rowkeys starting with "xxx*", "yyy*" and "zzz*" so that
    // scan hits all the region and not all rows lie in a single region
    try (Table table = UTIL.createMultiRegionTable(TABLE_NAME, CF)) {
      table.put(Arrays.asList(new Put(Bytes.toBytes("xxx1")).addColumn(CF, CQ, VALUE),
        new Put(Bytes.toBytes("yyy1")).addColumn(CF, CQ, VALUE),
        new Put(Bytes.toBytes("zzz1")).addColumn(CF, CQ, VALUE)));
    }
    CONN = ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testScanMetricsByRegionWithScannerSuspending() throws Exception {
    // Setup scan
    Scan scan = new Scan();
    scan.withStartRow(Bytes.toBytes("xxx1"), true);
    scan.withStopRow(Bytes.toBytes("zzz1"), true);
    scan.setEnableScanMetricsByRegion(true);
    scan.setMaxResultSize(1);

    // Prepare scanner
    final AtomicInteger rowsReadCounter = new AtomicInteger(0);
    AsyncTableResultScanner scanner = new AsyncTableResultScanner(TABLE_NAME, scan, 1) {
      @Override
      public void onNext(Result[] results, ScanController controller) {
        rowsReadCounter.addAndGet(results.length);
        super.onNext(results, controller);
      }
    };

    // Do the scan so that rows get loaded in the scanner (consumer)
    CONN.getTable(TABLE_NAME).scan(scan, scanner);

    List<Result> results = new ArrayList<>();
    int expectedTotalRows = 3;
    // Assert that only 1 row has been loaded so far as maxCacheSize is set to 1 byte
    for (int i = 1; i <= expectedTotalRows; i++) {
      UTIL.waitFor(10000, 100, new Waiter.ExplainingPredicate<Exception>() {

        @Override
        public boolean evaluate() throws Exception {
          return scanner.isSuspended();
        }

        @Override
        public String explainFailure() throws Exception {
          return "The given scanner has been suspended in time";
        }
      });
      assertTrue(scanner.isSuspended());
      assertEquals(i, rowsReadCounter.get());
      results.add(scanner.next());
    }
    Assert.assertNull(scanner.next());

    // Assert on overall scan metrics and scan metrics by region
    ScanMetrics scanMetrics = scanner.getScanMetrics();
    // Assert on overall scan metrics
    long bytes = getBytesOfResults(results);
    Map<String, Long> overallMetrics = scanMetrics.getMetricsMap(false);
    assertEquals(3, (long) overallMetrics.get(REGIONS_SCANNED_METRIC_NAME));
    assertEquals(bytes, (long) overallMetrics.get(BYTES_IN_RESULTS_METRIC_NAME));
    // 1 Extra RPC call per region where no row is returned but moreResultsInRegion is set to false
    assertEquals(6, (long) overallMetrics.get(RPC_CALLS_METRIC_NAME));
    // Assert scan metrics by region were collected for the region scanned
    Map<ScanMetricsRegionInfo, Map<String, Long>> scanMetricsByRegion =
      scanMetrics.collectMetricsByRegion(false);
    assertEquals(3, scanMetricsByRegion.size());
    for (Map.Entry<ScanMetricsRegionInfo, Map<String, Long>> entry : scanMetricsByRegion
      .entrySet()) {
      ScanMetricsRegionInfo smri = entry.getKey();
      Map<String, Long> perRegionMetrics = entry.getValue();
      assertNotNull(smri.getServerName());
      assertNotNull(smri.getEncodedRegionName());
      assertEquals(1, (long) perRegionMetrics.get(REGIONS_SCANNED_METRIC_NAME));
      assertEquals(1, (long) perRegionMetrics.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
      bytes = getBytesOfResults(Collections.singletonList(results.get(0)));
      assertEquals(bytes, (long) perRegionMetrics.get(BYTES_IN_RESULTS_METRIC_NAME));
      assertEquals(2, (long) perRegionMetrics.get(RPC_CALLS_METRIC_NAME));
    }
  }
}
