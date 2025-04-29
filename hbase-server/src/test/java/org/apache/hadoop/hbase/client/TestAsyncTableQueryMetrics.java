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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServer;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSourceImpl;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableQueryMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableQueryMetrics.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final TableName TABLE_NAME = TableName.valueOf("ResultMetrics");

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ = Bytes.toBytes("cq");

  private static final byte[] VALUE = Bytes.toBytes("value");

  private static final byte[] ROW_1 = Bytes.toBytes("zzz1");
  private static final byte[] ROW_2 = Bytes.toBytes("zzz2");
  private static final byte[] ROW_3 = Bytes.toBytes("zzz3");

  private static AsyncConnection CONN;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(3);
    // Create 3 rows in the table, with rowkeys starting with "zzz*" so that
    // scan are forced to hit all the regions.
    try (Table table = UTIL.createMultiRegionTable(TABLE_NAME, CF)) {
      table.put(Arrays.asList(new Put(ROW_1).addColumn(CF, CQ, VALUE),
        new Put(ROW_2).addColumn(CF, CQ, VALUE), new Put(ROW_3).addColumn(CF, CQ, VALUE)));
    }
    CONN = ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get();
    CONN.getAdmin().flush(TABLE_NAME).join();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void itTestsGets() throws Exception {
    // Test a single Get
    Get g1 = new Get(ROW_1);
    g1.setQueryMetricsEnabled(true);

    long bbs = getClusterBlockBytesScanned();
    Result result = CONN.getTable(TABLE_NAME).get(g1).get();
    bbs += result.getMetrics().getBlockBytesScanned();
    Assert.assertNotNull(result.getMetrics());
    Assert.assertEquals(getClusterBlockBytesScanned(), bbs);

    // Test multigets
    Get g2 = new Get(ROW_2);
    g2.setQueryMetricsEnabled(true);

    Get g3 = new Get(ROW_3);
    g3.setQueryMetricsEnabled(true);

    List<CompletableFuture<Result>> futures = CONN.getTable(TABLE_NAME).get(List.of(g1, g2, g3));

    for (CompletableFuture<Result> future : futures) {
      result = future.join();
      Assert.assertNotNull(result.getMetrics());
      bbs += result.getMetrics().getBlockBytesScanned();
    }

    Assert.assertEquals(getClusterBlockBytesScanned(), bbs);
  }

  @Test
  public void itTestsDefaultGetNoMetrics() throws Exception {
    // Test a single Get
    Get g1 = new Get(ROW_1);

    Result result = CONN.getTable(TABLE_NAME).get(g1).get();
    Assert.assertNull(result.getMetrics());

    // Test multigets
    Get g2 = new Get(ROW_2);
    Get g3 = new Get(ROW_3);
    List<CompletableFuture<Result>> futures = CONN.getTable(TABLE_NAME).get(List.of(g1, g2, g3));
    futures.forEach(f -> Assert.assertNull(f.join().getMetrics()));

  }

  @Test
  public void itTestsScans() {
    Scan scan = new Scan();
    scan.setQueryMetricsEnabled(true);

    long bbs = getClusterBlockBytesScanned();
    try (ResultScanner scanner = CONN.getTable(TABLE_NAME).getScanner(scan)) {
      for (Result result : scanner) {
        Assert.assertNotNull(result.getMetrics());
        bbs += result.getMetrics().getBlockBytesScanned();
        Assert.assertEquals(getClusterBlockBytesScanned(), bbs);
      }
    }
  }

  @Test
  public void itTestsDefaultScanNoMetrics() {
    Scan scan = new Scan();

    try (ResultScanner scanner = CONN.getTable(TABLE_NAME).getScanner(scan)) {
      for (Result result : scanner) {
        Assert.assertNull(result.getMetrics());
      }
    }
  }

  @Test
  public void itTestsAtomicOperations() {
    CheckAndMutate cam = CheckAndMutate.newBuilder(ROW_1).ifEquals(CF, CQ, VALUE)
      .queryMetricsEnabled(true).build(new Put(ROW_1).addColumn(CF, CQ, VALUE));

    long bbs = getClusterBlockBytesScanned();
    CheckAndMutateResult result = CONN.getTable(TABLE_NAME).checkAndMutate(cam).join();
    QueryMetrics metrics = result.getMetrics();

    Assert.assertNotNull(metrics);
    Assert.assertEquals(getClusterBlockBytesScanned(), bbs + metrics.getBlockBytesScanned());

    bbs = getClusterBlockBytesScanned();
    List<CheckAndMutate> batch = new ArrayList<>();
    batch.add(cam);
    batch.add(CheckAndMutate.newBuilder(ROW_2).queryMetricsEnabled(true).ifEquals(CF, CQ, VALUE)
      .build(new Put(ROW_2).addColumn(CF, CQ, VALUE)));
    batch.add(CheckAndMutate.newBuilder(ROW_3).queryMetricsEnabled(true).ifEquals(CF, CQ, VALUE)
      .build(new Put(ROW_3).addColumn(CF, CQ, VALUE)));

    List<Object> res = CONN.getTable(TABLE_NAME).batchAll(batch).join();
    long totalBbs = res.stream()
      .mapToLong(r -> ((CheckAndMutateResult) r).getMetrics().getBlockBytesScanned()).sum();
    Assert.assertEquals(getClusterBlockBytesScanned(), bbs + totalBbs);

    bbs = getClusterBlockBytesScanned();

    // flush to force fetch from disk
    CONN.getAdmin().flush(TABLE_NAME).join();
    List<CompletableFuture<Object>> futures = CONN.getTable(TABLE_NAME).batch(batch);

    totalBbs = futures.stream().map(CompletableFuture::join)
      .mapToLong(r -> ((CheckAndMutateResult) r).getMetrics().getBlockBytesScanned()).sum();
    Assert.assertEquals(getClusterBlockBytesScanned(), bbs + totalBbs);
  }

  @Test
  public void itTestsDefaultAtomicOperations() {
    CheckAndMutate cam = CheckAndMutate.newBuilder(ROW_1).ifEquals(CF, CQ, VALUE)
      .build(new Put(ROW_1).addColumn(CF, CQ, VALUE));

    CheckAndMutateResult result = CONN.getTable(TABLE_NAME).checkAndMutate(cam).join();
    QueryMetrics metrics = result.getMetrics();

    Assert.assertNull(metrics);

    List<CheckAndMutate> batch = new ArrayList<>();
    batch.add(cam);
    batch.add(CheckAndMutate.newBuilder(ROW_2).ifEquals(CF, CQ, VALUE)
      .build(new Put(ROW_2).addColumn(CF, CQ, VALUE)));
    batch.add(CheckAndMutate.newBuilder(ROW_3).ifEquals(CF, CQ, VALUE)
      .build(new Put(ROW_3).addColumn(CF, CQ, VALUE)));

    List<Object> res = CONN.getTable(TABLE_NAME).batchAll(batch).join();
    for (Object r : res) {
      Assert.assertNull(((CheckAndMutateResult) r).getMetrics());
    }

    // flush to force fetch from disk
    CONN.getAdmin().flush(TABLE_NAME).join();
    List<CompletableFuture<Object>> futures = CONN.getTable(TABLE_NAME).batch(batch);

    for (CompletableFuture<Object> future : futures) {
      Object r = future.join();
      Assert.assertNull(((CheckAndMutateResult) r).getMetrics());
    }
  }

  private static long getClusterBlockBytesScanned() {
    long bbs = 0L;

    for (JVMClusterUtil.RegionServerThread rs : UTIL.getHBaseCluster().getRegionServerThreads()) {
      MetricsRegionServer metrics = rs.getRegionServer().getMetrics();
      MetricsRegionServerSourceImpl source =
        (MetricsRegionServerSourceImpl) metrics.getMetricsSource();

      bbs += source.getMetricsRegistry()
        .getCounter(MetricsRegionServerSource.BLOCK_BYTES_SCANNED_KEY, 0L).value();
    }

    return bbs;
  }
}
