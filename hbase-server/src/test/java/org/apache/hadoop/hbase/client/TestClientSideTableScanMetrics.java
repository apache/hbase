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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
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
import org.junit.Before;
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

  static AtomicInteger sleepTimeMs = new AtomicInteger(0);

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
        sleepTimeMs.addAndGet(SLEEP_TIME_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c,
        InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
      try {
        Thread.sleep(SLEEP_TIME_MS);
        sleepTimeMs.addAndGet(SLEEP_TIME_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return true;
    }

    @Override
    public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> c,
        InternalScanner s) throws IOException {
      try {
        Thread.sleep(SLEEP_TIME_MS);
        sleepTimeMs.addAndGet(SLEEP_TIME_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
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
        new Put(Bytes.toBytes("zzz1")).addColumn(CF, CQ, VALUE),
        new Put(Bytes.toBytes("zzz2")).addColumn(CF, CQ, VALUE),
        new Put(Bytes.toBytes("zzz3")).addColumn(CF, CQ, VALUE)));
    }
    CONN = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    NUM_REGIONS = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME).size();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void testCaseSetup() {
    sleepTimeMs.set(0);
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
    System.out.println("ScanMetrics: " + scanMetrics + ", sleepTimeMs: " + sleepTimeMs.get());
    return scanMetrics;
  }

  @Test
  public void testScanExecutionAndRpcTimeMetrics() throws Exception {
    Scan scan = generateScan(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    scan.setScanMetricsEnabled(true);
    ScanMetrics scanMetrics = assertScannedRowsAndGetScanMetrics(scan, 5);
    Assert.assertNotNull(scanMetrics);
    Map<String, Long> metricsMap = scanMetrics.getMetricsMap(false);

    long regionsScanned = metricsMap.get(ScanMetrics.REGIONS_SCANNED_METRIC_NAME);
    Assert.assertEquals(NUM_REGIONS, regionsScanned);

    long cacheLoad = metricsMap.get(ScanMetrics.CACHE_LOAD_WAIT_TIME_MS_METRIC_NAME);
    long scanExec = metricsMap.get(ScanMetrics.SCAN_EXECUTION_TIME_MS_METRIC_NAME);
    long rpcRoundTrip = metricsMap.get(ScanMetrics.RPC_ROUND_TRIP_TIME_MS_METRIC_NAME);

    // Per region two times the slowness is introduced, once for preScannerOpen and once for preScannerNext.
    Assert.assertTrue(
      cacheLoad >= SleepOnScanCoprocessor.SLEEP_TIME_MS * NUM_REGIONS * 2);
    Assert.assertTrue(
      scanExec >= SleepOnScanCoprocessor.SLEEP_TIME_MS * NUM_REGIONS * 2);
    Assert.assertTrue(
      rpcRoundTrip >= SleepOnScanCoprocessor.SLEEP_TIME_MS * NUM_REGIONS * 2);

    Assert.assertTrue("rpcRoundTrip (" + rpcRoundTrip + ") should be <= scanExecution ("
      + scanExec + ")", rpcRoundTrip <= scanExec);
    Assert.assertTrue("scanExecution (" + scanExec + ") should be <= cacheLoad ("
      + cacheLoad + ")", scanExec <= cacheLoad);
  }

  @Test
  public void testMetaLookupTimeMetric() throws Exception {
    CONN.clearRegionLocationCache();

    Scan scan = generateScan(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    scan.setScanMetricsEnabled(true);
    ScanMetrics scanMetrics = assertScannedRowsAndGetScanMetrics(scan, 5);
    Assert.assertNotNull(scanMetrics);
    Map<String, Long> metricsMap = scanMetrics.getMetricsMap(false);

    long regionsScanned = metricsMap.get(ScanMetrics.REGIONS_SCANNED_METRIC_NAME);
    Assert.assertEquals(NUM_REGIONS, regionsScanned);

    // Meta lookup time for forward scan happens in ScannerCallableWithReplicas.call() but for reverse scan it happens in ReverseScannerCallable.prepare() except the first time. Thus, asserting on minimum value of meta lookup time across both directions of scan.
    long metaLookupTime = metricsMap.get(ScanMetrics.META_LOOKUP_TIME_MS_METRIC_NAME);
    Assert.assertTrue(
      metaLookupTime >= SleepOnScanCoprocessor.SLEEP_TIME_MS * 2);

    long cacheLoad = metricsMap.get(ScanMetrics.CACHE_LOAD_WAIT_TIME_MS_METRIC_NAME);
    Assert.assertTrue(
      cacheLoad >= SleepOnScanCoprocessor.SLEEP_TIME_MS * NUM_REGIONS * 2);
    Assert.assertTrue("metaLookupTime (" + metaLookupTime + ") should be <= cacheLoad ("
      + cacheLoad + ")", metaLookupTime <= cacheLoad);
  }

  @Test
  public void testThreadPoolMetrics() throws Exception {
    int minWaitTimeMs = 50;
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
          ThreadPoolExecutor tpe = (ThreadPoolExecutor) singleThreadPool;
          while (tpe.getQueue().size() < 1) {
            Thread.sleep(10);
          }
          Thread.sleep(minWaitTimeMs);
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
      Assert.assertEquals(5, countOfRows);
      Assert.assertNotNull(scanMetrics);
      Map<String, Long> metricsMap = scanMetrics.getMetricsMap(false);

      long regionsScanned = metricsMap.get(ScanMetrics.REGIONS_SCANNED_METRIC_NAME);
      Assert.assertEquals(NUM_REGIONS, regionsScanned);

      long threadPoolWaitTimeMs = metricsMap.get(ScanMetrics.THREAD_POOL_WAIT_TIME_MS_METRIC_NAME);
      Assert.assertTrue(threadPoolWaitTimeMs >= minWaitTimeMs);
      long threadPoolExecutionTimeMs = metricsMap.get(ScanMetrics.THREAD_POOL_EXECUTION_TIME_MS_METRIC_NAME);
      Assert.assertTrue(threadPoolExecutionTimeMs >= SleepOnScanCoprocessor.SLEEP_TIME_MS * NUM_REGIONS * 2);
      long scanExecutionTimeMs = metricsMap.get(ScanMetrics.SCAN_EXECUTION_TIME_MS_METRIC_NAME);
      Assert.assertTrue(
        scanExecutionTimeMs >= SleepOnScanCoprocessor.SLEEP_TIME_MS * NUM_REGIONS * 2);
      long cacheLoadWaitTimeMs = metricsMap.get(ScanMetrics.CACHE_LOAD_WAIT_TIME_MS_METRIC_NAME);

      Assert.assertTrue("scanExecutionTimeMs (" + scanExecutionTimeMs
        + ") should be <= threadPoolExecutionTimeMs (" + threadPoolExecutionTimeMs + ")",
        scanExecutionTimeMs <= threadPoolExecutionTimeMs);
      Assert.assertTrue("threadPoolExecutionTimeMs (" + threadPoolExecutionTimeMs
        + ") should be <= cacheLoadWaitTimeMs (" + cacheLoadWaitTimeMs + ")",
        threadPoolExecutionTimeMs <= cacheLoadWaitTimeMs);
      Assert.assertTrue("threadPoolWaitTimeMs (" + threadPoolWaitTimeMs
        + ") should be <= cacheLoadWaitTimeMs (" + cacheLoadWaitTimeMs + ")",
        threadPoolWaitTimeMs <= cacheLoadWaitTimeMs);
    } finally {
      singleThreadPool.shutdownNow();
    }
  }

  @Test
  public void testScannerCloseTimeMetric() throws Exception {
    Scan scan = generateScan(Bytes.toBytes("zzz"), HConstants.EMPTY_END_ROW);
    scan.setScanMetricsEnabled(true);
    scan.setCaching(1);

    ScanMetrics scanMetrics;
    try (Table table = CONN.getTable(TABLE_NAME);
         ResultScanner scanner = table.getScanner(scan)) {
      Result result = scanner.next();
      Assert.assertNotNull(result);
      Assert.assertFalse(result.isEmpty());
      scanMetrics = scanner.getScanMetrics();
    }

    Assert.assertNotNull(scanMetrics);
    Map<String, Long> metricsMap = scanMetrics.getMetricsMap(false);
    System.out.println("ScanMetrics: " + scanMetrics + ", sleepTimeMs: " + sleepTimeMs.get());

    long regionsScanned = metricsMap.get(ScanMetrics.REGIONS_SCANNED_METRIC_NAME);
    Assert.assertEquals(1, regionsScanned);

    long scannerCloseTime = metricsMap.get(ScanMetrics.SCANNER_CLOSE_TIME_MS_METRIC_NAME);
    Assert.assertTrue(scannerCloseTime >= SleepOnScanCoprocessor.SLEEP_TIME_MS);
    long cacheLoadWaitTimeMs = metricsMap.get(ScanMetrics.CACHE_LOAD_WAIT_TIME_MS_METRIC_NAME);
    Assert.assertTrue(
      cacheLoadWaitTimeMs > SleepOnScanCoprocessor.SLEEP_TIME_MS * 2);
    Assert.assertTrue("scannerCloseTime (" + scannerCloseTime
      + ") should be <= cacheLoadWaitTimeMs (" + cacheLoadWaitTimeMs + ")",
      scannerCloseTime <= cacheLoadWaitTimeMs);
  }
}
