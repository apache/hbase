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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@Category({ ClientTests.class, LargeTests.class })
public class TestClientSideTableScanMetrics extends FromClientSideBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClientSideTableScanMetrics.class);

  public static class SleepOnScanCoprocessor implements RegionCoprocessor, RegionObserver {
    static final int SLEEP_TIME_MS = 5;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan)
        throws IOException {
      try {
        Thread.sleep(SLEEP_TIME_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c,
        InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
      try {
        Thread.sleep(SLEEP_TIME_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return true;
    }
  }

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME =
    TableName.valueOf(TestClientSideTableScanMetrics.class.getSimpleName());

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ = Bytes.toBytes("cq");

  private static final byte[] VALUE = Bytes.toBytes("value");

  private static int NUM_REGIONS;

  private static Connection CONN;

  @Parameters(name = "{index}: scanner={0}")
  public static List<Object[]> params() {
    return Arrays.asList(new Object[] { "ForwardScanner", new Scan() },
      new Object[] { "ReverseScanner", new Scan().setReversed(true) });
  }

  @Parameter(0)
  public String scannerName;

  @Parameter(1)
  public Scan originalScan;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0);
    TEST_UTIL.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      SleepOnScanCoprocessor.class.getName());
    TEST_UTIL.startMiniCluster(2);
    try (Table table = TEST_UTIL.createMultiRegionTable(TABLE_NAME, CF)) {
      table.put(Arrays.asList(new Put(Bytes.toBytes("xxx1")).addColumn(CF, CQ, VALUE),
        new Put(Bytes.toBytes("yyy1")).addColumn(CF, CQ, VALUE),
        new Put(Bytes.toBytes("zzz1")).addColumn(CF, CQ, VALUE)));
    }
    CONN = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    NUM_REGIONS = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME).size();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  protected Scan generateScan(byte[] smallerRow, byte[] largerRow) throws IOException {
    Scan scan = new Scan(originalScan);
    if (originalScan.isReversed()) {
      scan.withStartRow(largerRow, true);
      scan.withStopRow(smallerRow, true);
    } else {
      scan.withStartRow(smallerRow, true);
      scan.withStopRow(largerRow, true);
    }
    return scan;
  }

  protected ScanMetrics assertScannedRowsAndGetScanMetrics(Scan scan, int expectedCount)
    throws IOException {
    int countOfRows = 0;
    ScanMetrics scanMetrics;
    try (Table table = CONN.getTable(TABLE_NAME); ResultScanner scanner = table.getScanner(scan)) {
      for (Result result : scanner) {
        Assert.assertFalse(result.isEmpty());
        countOfRows++;
      }
      scanMetrics = scanner.getScanMetrics();
    }
    Assert.assertEquals(expectedCount, countOfRows);
    return scanMetrics;
  }

  @Test
  public void testScanExecutionAndRpcTimeMetrics() throws Exception {
    Scan scan = generateScan(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    scan.setScanMetricsEnabled(true);
    ScanMetrics scanMetrics = assertScannedRowsAndGetScanMetrics(scan, 3);
    Assert.assertNotNull(scanMetrics);
    Map<String, Long> metricsMap = scanMetrics.getMetricsMap(false);

    long cacheLoad = metricsMap.get(ScanMetrics.CLIENT_CACHE_LOAD_WAIT_TIME_MS_METRIC_NAME);
    long scanExec = metricsMap.get(ScanMetrics.CLIENT_SCAN_EXECUTION_TIME_MS_METRIC_NAME);
    long rpcRoundTrip = metricsMap.get(ScanMetrics.CLIENT_RPC_ROUND_TRIP_TIME_MS_METRIC_NAME);

    Assert.assertTrue("clientCacheLoadWaitTimeMs should be > 0, got " + cacheLoad,
      cacheLoad > 0);
    Assert.assertTrue("clientScanExecutionTimeMs should be > 0, got " + scanExec,
      scanExec > 0);
    Assert.assertTrue("clientRpcRoundTripTimeMs should be > 0, got " + rpcRoundTrip,
      rpcRoundTrip > 0);

    Assert.assertTrue("rpcRoundTrip (" + rpcRoundTrip + ") should be <= scanExecution ("
      + scanExec + ")", rpcRoundTrip <= scanExec);
    Assert.assertTrue("scanExecution (" + scanExec + ") should be <= cacheLoad ("
      + cacheLoad + ")", scanExec <= cacheLoad);
  }

  @Test
  public void testScanPrepareTimeMetric() throws Exception {
    CONN.clearRegionLocationCache();

    Scan scan = generateScan(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    scan.setScanMetricsEnabled(true);
    ScanMetrics scanMetrics = assertScannedRowsAndGetScanMetrics(scan, 3);
    Assert.assertNotNull(scanMetrics);
    Map<String, Long> metricsMap = scanMetrics.getMetricsMap(false);

    long prepareTime = metricsMap.get(ScanMetrics.CLIENT_SCAN_PREPARE_TIME_MS_METRIC_NAME);
    Assert.assertTrue("clientScanPrepareTimeMs should be > 0, got " + prepareTime,
      prepareTime > 0);
  }

  @Test
  public void testThreadPoolWaitTimeMetric() throws Exception {
    ExecutorService singleThreadPool = Executors.newFixedThreadPool(1);
    try {
      CountDownLatch taskStarted = new CountDownLatch(1);
      CountDownLatch allowFinish = new CountDownLatch(1);
      singleThreadPool.submit(() -> {
        taskStarted.countDown();
        try {
          allowFinish.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      taskStarted.await();

      Scan scan = generateScan(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      scan.setScanMetricsEnabled(true);

      new Thread(() -> {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        allowFinish.countDown();
      }).start();

      int countOfRows = 0;
      ScanMetrics scanMetrics;
      try (Table table = CONN.getTable(TABLE_NAME, singleThreadPool);
           ResultScanner scanner = table.getScanner(scan)) {
        for (Result result : scanner) {
          Assert.assertFalse(result.isEmpty());
          countOfRows++;
        }
        scanMetrics = scanner.getScanMetrics();
      }
      Assert.assertEquals(3, countOfRows);
      Assert.assertNotNull(scanMetrics);

      long waitTime = scanMetrics.clientThreadPoolWaitTimeMs.get();
      long execTime = scanMetrics.clientThreadPoolExecutionTimeMs.get();
      Assert.assertTrue("clientThreadPoolWaitTimeMs should be > 0, got " + waitTime,
        waitTime > 0);
      Assert.assertTrue("clientThreadPoolExecutionTimeMs should be > 0, got " + execTime,
        execTime > 0);
    } finally {
      singleThreadPool.shutdownNow();
    }
  }

  @Test
  public void testTimingHierarchyBetweenClientAndServerMetrics() throws Exception {
    Scan scan = generateScan(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    scan.setScanMetricsEnabled(true);
    ScanMetrics scanMetrics = assertScannedRowsAndGetScanMetrics(scan, 3);
    Assert.assertNotNull(scanMetrics);
    Map<String, Long> metricsMap = scanMetrics.getMetricsMap(false);

    long serverProcessingTime =
      metricsMap.get(ServerSideScanMetrics.RPC_SCAN_PROCESSING_TIME_METRIC_NAME);
    long serverQueueWaitTime =
      metricsMap.get(ServerSideScanMetrics.RPC_SCAN_QUEUE_WAIT_TIME_METRIC_NAME);
    long serverTime = serverProcessingTime + serverQueueWaitTime;
    long rpcRoundTrip = metricsMap.get(ScanMetrics.CLIENT_RPC_ROUND_TRIP_TIME_MS_METRIC_NAME);
    long scanExec = metricsMap.get(ScanMetrics.CLIENT_SCAN_EXECUTION_TIME_MS_METRIC_NAME);
    long cacheLoad = metricsMap.get(ScanMetrics.CLIENT_CACHE_LOAD_WAIT_TIME_MS_METRIC_NAME);

    Assert.assertTrue("serverTime should be > 0, got " + serverTime, serverTime > 0);

    Assert.assertTrue("serverTime (" + serverTime + ") should be <= rpcRoundTrip ("
      + rpcRoundTrip + ")", serverTime <= rpcRoundTrip);
    Assert.assertTrue("rpcRoundTrip (" + rpcRoundTrip + ") should be <= scanExecution ("
      + scanExec + ")", rpcRoundTrip <= scanExec);
    Assert.assertTrue("scanExecution (" + scanExec + ") should be <= cacheLoad ("
      + cacheLoad + ")", scanExec <= cacheLoad);
  }
}
