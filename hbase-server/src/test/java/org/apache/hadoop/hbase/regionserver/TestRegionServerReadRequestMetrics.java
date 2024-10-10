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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore // Depends on Master being able to host regions. Needs fixing.
@Category(MediumTests.class)
public class TestRegionServerReadRequestMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionServerReadRequestMetrics.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestRegionServerReadRequestMetrics.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final TableName TABLE_NAME = TableName.valueOf("test");
  private static final byte[] CF1 = Bytes.toBytes("c1");
  private static final byte[] CF2 = Bytes.toBytes("c2");

  private static final byte[] ROW1 = Bytes.toBytes("a");
  private static final byte[] ROW2 = Bytes.toBytes("b");
  private static final byte[] ROW3 = Bytes.toBytes("c");
  private static final byte[] COL1 = Bytes.toBytes("q1");
  private static final byte[] COL2 = Bytes.toBytes("q2");
  private static final byte[] COL3 = Bytes.toBytes("q3");
  private static final byte[] VAL1 = Bytes.toBytes("v1");
  private static final byte[] VAL2 = Bytes.toBytes("v2");
  private static final byte[] VAL3 = Bytes.toBytes(0L);

  private static final int MAX_TRY = 20;
  private static final int SLEEP_MS = 100;
  private static final int TTL = 1;

  private static Admin admin;
  private static Collection<ServerName> serverNames;
  private static Table table;
  private static RegionInfo regionInfo;

  private static Map<Metric, Long> requestsMap = new HashMap<>();
  private static Map<Metric, Long> requestsMapPrev = new HashMap<>();

  @BeforeClass
  public static void setUpOnce() throws Exception {
    TEST_UTIL.startMiniCluster();
    admin = TEST_UTIL.getAdmin();
    serverNames =
      admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).getLiveServerMetrics().keySet();
    table = createTable();
    putData();
    List<RegionInfo> regions = admin.getRegions(TABLE_NAME);
    assertEquals("Table " + TABLE_NAME + " should have 1 region", 1, regions.size());
    regionInfo = regions.get(0);

    for (Metric metric : Metric.values()) {
      requestsMap.put(metric, 0L);
      requestsMapPrev.put(metric, 0L);
    }
  }

  private static Table createTable() throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TABLE_NAME);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF1));
    builder
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF2).setTimeToLive(TTL).build());
    admin.createTable(builder.build());
    return TEST_UTIL.getConnection().getTable(TABLE_NAME);
  }

  private static void testReadRequests(long resultCount, long expectedReadRequests,
    long expectedFilteredReadRequests) throws IOException, InterruptedException {
    updateMetricsMap();
    System.out.println("requestsMapPrev = " + requestsMapPrev);
    System.out.println("requestsMap = " + requestsMap);

    assertEquals(expectedReadRequests,
      requestsMap.get(Metric.REGION_READ) - requestsMapPrev.get(Metric.REGION_READ));
    assertEquals(expectedFilteredReadRequests, requestsMap.get(Metric.FILTERED_REGION_READ)
      - requestsMapPrev.get(Metric.FILTERED_REGION_READ));
    assertEquals(expectedFilteredReadRequests, requestsMap.get(Metric.FILTERED_SERVER_READ)
      - requestsMapPrev.get(Metric.FILTERED_SERVER_READ));
    assertEquals(expectedReadRequests, resultCount);
  }

  private static void updateMetricsMap() throws IOException, InterruptedException {
    for (Metric metric : Metric.values()) {
      requestsMapPrev.put(metric, requestsMap.get(metric));
    }

    ServerMetrics serverMetrics = null;
    RegionMetrics regionMetricsOuter = null;
    boolean metricsUpdated = false;
    for (int i = 0; i < MAX_TRY; i++) {
      for (ServerName serverName : serverNames) {
        serverMetrics = admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
          .getLiveServerMetrics().get(serverName);

        Map<byte[], RegionMetrics> regionMetrics = serverMetrics.getRegionMetrics();
        RegionMetrics regionMetric = regionMetrics.get(regionInfo.getRegionName());
        if (regionMetric != null) {
          regionMetricsOuter = regionMetric;
          for (Metric metric : Metric.values()) {
            if (getReadRequest(serverMetrics, regionMetric, metric) > requestsMapPrev.get(metric)) {
              for (Metric metricInner : Metric.values()) {
                requestsMap.put(metricInner,
                  getReadRequest(serverMetrics, regionMetric, metricInner));
              }
              metricsUpdated = true;
              break;
            }
          }
        }
      }
      if (metricsUpdated) {
        break;
      }
      Thread.sleep(SLEEP_MS);
    }
    if (!metricsUpdated) {
      for (Metric metric : Metric.values()) {
        requestsMap.put(metric, getReadRequest(serverMetrics, regionMetricsOuter, metric));
      }
    }
  }

  private static long getReadRequest(ServerMetrics serverMetrics, RegionMetrics regionMetrics,
    Metric metric) {
    switch (metric) {
      case REGION_READ:
        return regionMetrics.getReadRequestCount();
      case SERVER_READ:
        return serverMetrics.getRegionMetrics().get(regionMetrics.getRegionName())
          .getReadRequestCount();
      case FILTERED_REGION_READ:
        return regionMetrics.getFilteredReadRequestCount();
      case FILTERED_SERVER_READ:
        return serverMetrics.getRegionMetrics().get(regionMetrics.getRegionName())
          .getFilteredReadRequestCount();
      default:
        throw new IllegalStateException();
    }
  }

  private static void putData() throws IOException {
    Put put;

    put = new Put(ROW1);
    put.addColumn(CF1, COL1, VAL1);
    put.addColumn(CF1, COL2, VAL2);
    put.addColumn(CF1, COL3, VAL3);
    table.put(put);
    put = new Put(ROW2);
    put.addColumn(CF1, COL1, VAL2); // put val2 instead of val1
    put.addColumn(CF1, COL2, VAL2);
    table.put(put);
    put = new Put(ROW3);
    put.addColumn(CF1, COL1, VAL1);
    put.addColumn(CF1, COL2, VAL2);
    table.put(put);
  }

  private static void putTTLExpiredData() throws IOException, InterruptedException {
    Put put;

    put = new Put(ROW1);
    put.addColumn(CF2, COL1, VAL1);
    put.addColumn(CF2, COL2, VAL2);
    table.put(put);

    Thread.sleep(TTL * 1000);

    put = new Put(ROW2);
    put.addColumn(CF2, COL1, VAL1);
    put.addColumn(CF2, COL2, VAL2);
    table.put(put);

    put = new Put(ROW3);
    put.addColumn(CF2, COL1, VAL1);
    put.addColumn(CF2, COL2, VAL2);
    table.put(put);
  }

  @AfterClass
  public static void tearDownOnce() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testReadRequestsCountNotFiltered() throws Exception {
    int resultCount;
    Scan scan;
    Append append;
    Put put;
    Increment increment;
    Get get;

    // test for scan
    scan = new Scan();
    try (ResultScanner scanner = table.getScanner(scan)) {
      resultCount = 0;
      for (Result ignore : scanner) {
        resultCount++;
      }
      testReadRequests(resultCount, 3, 0);
    }

    // test for scan
    scan = new Scan().withStartRow(ROW2).withStopRow(ROW3);
    try (ResultScanner scanner = table.getScanner(scan)) {
      resultCount = 0;
      for (Result ignore : scanner) {
        resultCount++;
      }
      testReadRequests(resultCount, 1, 0);
    }

    // test for get
    get = new Get(ROW2);
    Result result = table.get(get);
    resultCount = result.isEmpty() ? 0 : 1;
    testReadRequests(resultCount, 1, 0);

    // test for increment
    increment = new Increment(ROW1);
    increment.addColumn(CF1, COL3, 1);
    result = table.increment(increment);
    resultCount = result.isEmpty() ? 0 : 1;
    testReadRequests(resultCount, 1, 0);

    // test for checkAndPut
    put = new Put(ROW1);
    put.addColumn(CF1, COL2, VAL2);
    boolean checkAndPut =
      table.checkAndMutate(ROW1, CF1).qualifier(COL2).ifEquals(VAL2).thenPut(put);
    resultCount = checkAndPut ? 1 : 0;
    testReadRequests(resultCount, 1, 0);

    // test for append
    append = new Append(ROW1);
    append.addColumn(CF1, COL2, VAL2);
    result = table.append(append);
    resultCount = result.isEmpty() ? 0 : 1;
    testReadRequests(resultCount, 1, 0);

    // test for checkAndMutate
    put = new Put(ROW1);
    put.addColumn(CF1, COL1, VAL1);
    RowMutations rm = new RowMutations(ROW1);
    rm.add(put);
    boolean checkAndMutate =
      table.checkAndMutate(ROW1, CF1).qualifier(COL1).ifEquals(VAL1).thenMutate(rm);
    resultCount = checkAndMutate ? 1 : 0;
    testReadRequests(resultCount, 1, 0);
  }

  @Ignore // HBASE-19785
  @Test
  public void testReadRequestsCountWithFilter() throws Exception {
    int resultCount;
    Scan scan;

    // test for scan
    scan = new Scan();
    scan.setFilter(new SingleColumnValueFilter(CF1, COL1, CompareOperator.EQUAL, VAL1));
    try (ResultScanner scanner = table.getScanner(scan)) {
      resultCount = 0;
      for (Result ignore : scanner) {
        resultCount++;
      }
      testReadRequests(resultCount, 2, 1);
    }

    // test for scan
    scan = new Scan();
    scan.setFilter(new RowFilter(CompareOperator.EQUAL, new BinaryComparator(ROW1)));
    try (ResultScanner scanner = table.getScanner(scan)) {
      resultCount = 0;
      for (Result ignore : scanner) {
        resultCount++;
      }
      testReadRequests(resultCount, 1, 2);
    }

    // test for scan
    scan = new Scan().withStartRow(ROW2).withStopRow(ROW3);
    scan.setFilter(new RowFilter(CompareOperator.EQUAL, new BinaryComparator(ROW1)));
    try (ResultScanner scanner = table.getScanner(scan)) {
      resultCount = 0;
      for (Result ignore : scanner) {
        resultCount++;
      }
      testReadRequests(resultCount, 0, 1);
    }

    // fixme filtered get should not increase readRequestsCount
    // Get get = new Get(ROW2);
    // get.setFilter(new SingleColumnValueFilter(CF1, COL1, CompareFilter.CompareOp.EQUAL, VAL1));
    // Result result = table.get(get);
    // resultCount = result.isEmpty() ? 0 : 1;
    // testReadRequests(resultCount, 0, 1);
  }

  @Ignore // HBASE-19785
  @Test
  public void testReadRequestsCountWithDeletedRow() throws Exception {
    try {
      Delete delete = new Delete(ROW3);
      table.delete(delete);

      Scan scan = new Scan();
      try (ResultScanner scanner = table.getScanner(scan)) {
        int resultCount = 0;
        for (Result ignore : scanner) {
          resultCount++;
        }
        testReadRequests(resultCount, 2, 1);
      }
    } finally {
      Put put = new Put(ROW3);
      put.addColumn(CF1, COL1, VAL1);
      put.addColumn(CF1, COL2, VAL2);
      table.put(put);
    }
  }

  @Test
  public void testReadRequestsCountWithTTLExpiration() throws Exception {
    putTTLExpiredData();

    Scan scan = new Scan();
    scan.addFamily(CF2);
    try (ResultScanner scanner = table.getScanner(scan)) {
      int resultCount = 0;
      for (Result ignore : scanner) {
        resultCount++;
      }
      testReadRequests(resultCount, 2, 1);
    }
  }

  @Ignore // See HBASE-19785
  @Test
  public void testReadRequestsWithCoprocessor() throws Exception {
    TableName tableName = TableName.valueOf("testReadRequestsWithCoprocessor");
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF1));
    builder.setCoprocessor(ScanRegionCoprocessor.class.getName());
    admin.createTable(builder.build());

    try {
      TEST_UTIL.waitTableAvailable(tableName);
      List<RegionInfo> regionInfos = admin.getRegions(tableName);
      assertEquals("Table " + TABLE_NAME + " should have 1 region", 1, regionInfos.size());
      boolean success = true;
      int i = 0;
      for (; i < MAX_TRY; i++) {
        try {
          testReadRequests(regionInfos.get(0).getRegionName(), 3);
        } catch (Throwable t) {
          LOG.warn("Got exception when try " + i + " times", t);
          Thread.sleep(SLEEP_MS);
          success = false;
        }
        if (success) {
          break;
        }
      }
      if (i == MAX_TRY) {
        fail("Failed to get right read requests metric after try " + i + " times");
      }
    } finally {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  private void testReadRequests(byte[] regionName, int expectedReadRequests) throws Exception {
    for (ServerName serverName : serverNames) {
      ServerMetrics serverMetrics = admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
        .getLiveServerMetrics().get(serverName);
      Map<byte[], RegionMetrics> regionMetrics = serverMetrics.getRegionMetrics();
      RegionMetrics regionMetric = regionMetrics.get(regionName);
      if (regionMetric != null) {
        LOG.debug("server read request is "
          + serverMetrics.getRegionMetrics().get(regionName).getReadRequestCount()
          + ", region read request is " + regionMetric.getReadRequestCount());
        assertEquals(3, serverMetrics.getRegionMetrics().get(regionName).getReadRequestCount());
        assertEquals(3, regionMetric.getReadRequestCount());
      }
    }
  }

  public static class ScanRegionCoprocessor implements RegionCoprocessor, RegionObserver {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void postOpen(ObserverContext<? extends RegionCoprocessorEnvironment> c) {
      RegionCoprocessorEnvironment env = c.getEnvironment();
      Region region = env.getRegion();
      try {
        putData(region);
        RegionScanner scanner = region.getScanner(new Scan());
        List<Cell> result = new LinkedList<>();
        while (scanner.next(result)) {
          result.clear();
        }
      } catch (Exception e) {
        LOG.warn("Got exception in coprocessor", e);
      }
    }

    private void putData(Region region) throws Exception {
      Put put = new Put(ROW1);
      put.addColumn(CF1, COL1, VAL1);
      region.put(put);
      put = new Put(ROW2);
      put.addColumn(CF1, COL1, VAL1);
      region.put(put);
      put = new Put(ROW3);
      put.addColumn(CF1, COL1, VAL1);
      region.put(put);
    }
  }

  private enum Metric {
    REGION_READ,
    SERVER_READ,
    FILTERED_REGION_READ,
    FILTERED_SERVER_READ
  }
}
