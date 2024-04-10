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

import static org.junit.Assert.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableScannerCloseWhileSuspending {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableScannerCloseWhileSuspending.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  private static AsyncConnection CONN;

  private static AsyncTable<?> TABLE;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    TABLE = CONN.getTable(TABLE_NAME, ForkJoinPool.commonPool());
    TABLE.putAll(IntStream.range(0, 100).mapToObj(
      i -> new Put(Bytes.toBytes(String.format("%02d", i))).addColumn(FAMILY, CQ, Bytes.toBytes(i)))
      .collect(Collectors.toList())).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  private int getScannersCount() {
    return TEST_UTIL.getHBaseCluster().getRegionServerThreads().stream()
      .map(t -> t.getRegionServer()).mapToInt(rs -> rs.getRSRpcServices().getScannersCount()).sum();
  }

  @Test
  public void testCloseScannerWhileSuspending() throws Exception {
    final AtomicInteger onNextCounter = new AtomicInteger(0);
    final CountDownLatch latch = new CountDownLatch(1);
    final Scan scan = new Scan().setMaxResultSize(1);
    final AsyncTableResultScanner scanner = new AsyncTableResultScanner(TABLE_NAME, scan, 1) {
      @Override
      public void onNext(Result[] results, ScanController controller) {
        onNextCounter.incrementAndGet();
        super.onNext(results, controller);
      }

      @Override
      public void onComplete() {
        super.onComplete();
        latch.countDown();
      }
    };

    CONN.getTable(TABLE_NAME).scan(scan, scanner);

    TEST_UTIL.waitFor(10000, 100, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return scanner.isSuspended();
      }

      @Override
      public String explainFailure() throws Exception {
        return "The given scanner has been suspended in time";
      }
    });
    assertEquals(1, getScannersCount());
    assertEquals(1, onNextCounter.get());

    scanner.close();
    TEST_UTIL.waitFor(10000, 100, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return getScannersCount() == 0;
      }

      @Override
      public String explainFailure() throws Exception {
        return "Still have " + getScannersCount() + " scanners opened";
      }
    });
    latch.await();
    assertEquals(1, onNextCounter.get());
  }
}
