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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class AbstractTestAsyncTableScan {

  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected static TableName TABLE_NAME = TableName.valueOf("async");

  protected static byte[] FAMILY = Bytes.toBytes("cf");

  protected static byte[] CQ1 = Bytes.toBytes("cq1");

  protected static byte[] CQ2 = Bytes.toBytes("cq2");

  protected static int COUNT = 1000;

  protected static AsyncConnection ASYNC_CONN;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    byte[][] splitKeys = new byte[8][];
    for (int i = 111; i < 999; i += 111) {
      splitKeys[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
    }
    TEST_UTIL.createTable(TABLE_NAME, FAMILY, splitKeys);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration());
    AsyncTable table = ASYNC_CONN.getTable(TABLE_NAME);
    List<CompletableFuture<?>> futures = new ArrayList<>();
    IntStream.range(0, COUNT).forEach(
      i -> futures.add(table.put(new Put(Bytes.toBytes(String.format("%03d", i)))
          .addColumn(FAMILY, CQ1, Bytes.toBytes(i)).addColumn(FAMILY, CQ2, Bytes.toBytes(i * i)))));
    CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    ASYNC_CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  protected abstract Scan createScan();

  protected abstract List<Result> doScan(AsyncTable table, Scan scan) throws Exception;

  @Test
  public void testScanAll() throws Exception {
    List<Result> results = doScan(ASYNC_CONN.getTable(TABLE_NAME), createScan());
    assertEquals(COUNT, results.size());
    IntStream.range(0, COUNT).forEach(i -> {
      Result result = results.get(i);
      assertEquals(String.format("%03d", i), Bytes.toString(result.getRow()));
      assertEquals(i, Bytes.toInt(result.getValue(FAMILY, CQ1)));
    });
  }

  private void assertResultEquals(Result result, int i) {
    assertEquals(String.format("%03d", i), Bytes.toString(result.getRow()));
    assertEquals(i, Bytes.toInt(result.getValue(FAMILY, CQ1)));
    assertEquals(i * i, Bytes.toInt(result.getValue(FAMILY, CQ2)));
  }

  @Test
  public void testReversedScanAll() throws Exception {
    List<Result> results = doScan(ASYNC_CONN.getTable(TABLE_NAME), createScan().setReversed(true));
    assertEquals(COUNT, results.size());
    IntStream.range(0, COUNT).forEach(i -> assertResultEquals(results.get(i), COUNT - i - 1));
  }

  @Test
  public void testScanNoStopKey() throws Exception {
    int start = 345;
    List<Result> results = doScan(ASYNC_CONN.getTable(TABLE_NAME),
      createScan().setStartRow(Bytes.toBytes(String.format("%03d", start))));
    assertEquals(COUNT - start, results.size());
    IntStream.range(0, COUNT - start).forEach(i -> assertResultEquals(results.get(i), start + i));
  }

  @Test
  public void testReverseScanNoStopKey() throws Exception {
    int start = 765;
    List<Result> results = doScan(ASYNC_CONN.getTable(TABLE_NAME),
      createScan().setStartRow(Bytes.toBytes(String.format("%03d", start))).setReversed(true));
    assertEquals(start + 1, results.size());
    IntStream.range(0, start + 1).forEach(i -> assertResultEquals(results.get(i), start - i));
  }

  private void testScan(int start, int stop) throws Exception {
    List<Result> results = doScan(ASYNC_CONN.getTable(TABLE_NAME),
      createScan().setStartRow(Bytes.toBytes(String.format("%03d", start)))
          .setStopRow(Bytes.toBytes(String.format("%03d", stop))));
    assertEquals(stop - start, results.size());
    IntStream.range(0, stop - start).forEach(i -> assertResultEquals(results.get(i), start + i));
  }

  private void testReversedScan(int start, int stop) throws Exception {
    List<Result> results = doScan(ASYNC_CONN.getTable(TABLE_NAME),
      createScan().setStartRow(Bytes.toBytes(String.format("%03d", start)))
          .setStopRow(Bytes.toBytes(String.format("%03d", stop))).setReversed(true));
    assertEquals(start - stop, results.size());
    IntStream.range(0, start - stop).forEach(i -> assertResultEquals(results.get(i), start - i));
  }

  @Test
  public void testScanWithStartKeyAndStopKey() throws Exception {
    testScan(345, 567);
  }

  @Test
  public void testReversedScanWithStartKeyAndStopKey() throws Exception {
    testReversedScan(765, 543);
  }

  @Test
  public void testScanAtRegionBoundary() throws Exception {
    testScan(222, 333);
  }

  @Test
  public void testReversedScanAtRegionBoundary() throws Exception {
    testScan(222, 333);
  }
}
