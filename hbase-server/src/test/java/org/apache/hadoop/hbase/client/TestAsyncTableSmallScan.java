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
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableSmallScan {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUALIFIER = Bytes.toBytes("cq");

  private static int COUNT = 1000;

  private static AsyncConnection ASYNC_CONN;

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
    IntStream.range(0, COUNT)
        .forEach(i -> futures.add(table.put(new Put(Bytes.toBytes(String.format("%03d", i)))
            .addColumn(FAMILY, QUALIFIER, Bytes.toBytes(i)))));
    CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    ASYNC_CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testScanAll() throws InterruptedException, ExecutionException {
    AsyncTable table = ASYNC_CONN.getTable(TABLE_NAME);
    List<Result> results = table.smallScan(new Scan().setSmall(true)).get();
    assertEquals(COUNT, results.size());
    IntStream.range(0, COUNT).forEach(i -> {
      Result result = results.get(i);
      assertEquals(String.format("%03d", i), Bytes.toString(result.getRow()));
      assertEquals(i, Bytes.toInt(result.getValue(FAMILY, QUALIFIER)));
    });
  }

  @Test
  public void testReversedScanAll() throws InterruptedException, ExecutionException {
    AsyncTable table = ASYNC_CONN.getTable(TABLE_NAME);
    List<Result> results = table.smallScan(new Scan().setSmall(true).setReversed(true)).get();
    assertEquals(COUNT, results.size());
    IntStream.range(0, COUNT).forEach(i -> {
      Result result = results.get(i);
      int actualIndex = COUNT - i - 1;
      assertEquals(String.format("%03d", actualIndex), Bytes.toString(result.getRow()));
      assertEquals(actualIndex, Bytes.toInt(result.getValue(FAMILY, QUALIFIER)));
    });
  }

  @Test
  public void testScanNoStopKey() throws InterruptedException, ExecutionException {
    AsyncTable table = ASYNC_CONN.getTable(TABLE_NAME);
    int start = 345;
    List<Result> results = table
        .smallScan(new Scan(Bytes.toBytes(String.format("%03d", start))).setSmall(true)).get();
    assertEquals(COUNT - start, results.size());
    IntStream.range(0, COUNT - start).forEach(i -> {
      Result result = results.get(i);
      int actualIndex = start + i;
      assertEquals(String.format("%03d", actualIndex), Bytes.toString(result.getRow()));
      assertEquals(actualIndex, Bytes.toInt(result.getValue(FAMILY, QUALIFIER)));
    });
  }

  @Test
  public void testReverseScanNoStopKey() throws InterruptedException, ExecutionException {
    AsyncTable table = ASYNC_CONN.getTable(TABLE_NAME);
    int start = 765;
    List<Result> results = table
        .smallScan(
          new Scan(Bytes.toBytes(String.format("%03d", start))).setSmall(true).setReversed(true))
        .get();
    assertEquals(start + 1, results.size());
    IntStream.range(0, start + 1).forEach(i -> {
      Result result = results.get(i);
      int actualIndex = start - i;
      assertEquals(String.format("%03d", actualIndex), Bytes.toString(result.getRow()));
      assertEquals(actualIndex, Bytes.toInt(result.getValue(FAMILY, QUALIFIER)));
    });
  }

  private void testScan(int start, int stop) throws InterruptedException, ExecutionException {
    AsyncTable table = ASYNC_CONN.getTable(TABLE_NAME);
    List<Result> results = table.smallScan(new Scan(Bytes.toBytes(String.format("%03d", start)))
        .setStopRow(Bytes.toBytes(String.format("%03d", stop))).setSmall(true)).get();
    assertEquals(stop - start, results.size());
    IntStream.range(0, stop - start).forEach(i -> {
      Result result = results.get(i);
      int actualIndex = start + i;
      assertEquals(String.format("%03d", actualIndex), Bytes.toString(result.getRow()));
      assertEquals(actualIndex, Bytes.toInt(result.getValue(FAMILY, QUALIFIER)));
    });
  }

  private void testReversedScan(int start, int stop)
      throws InterruptedException, ExecutionException {
    AsyncTable table = ASYNC_CONN.getTable(TABLE_NAME);
    List<Result> results = table.smallScan(new Scan(Bytes.toBytes(String.format("%03d", start)))
        .setStopRow(Bytes.toBytes(String.format("%03d", stop))).setSmall(true).setReversed(true))
        .get();
    assertEquals(start - stop, results.size());
    IntStream.range(0, start - stop).forEach(i -> {
      Result result = results.get(i);
      int actualIndex = start - i;
      assertEquals(String.format("%03d", actualIndex), Bytes.toString(result.getRow()));
      assertEquals(actualIndex, Bytes.toInt(result.getValue(FAMILY, QUALIFIER)));
    });
  }

  @Test
  public void testScanWithStartKeyAndStopKey() throws InterruptedException, ExecutionException {
    testScan(345, 567);
  }

  @Test
  public void testReversedScanWithStartKeyAndStopKey()
      throws InterruptedException, ExecutionException {
    testReversedScan(765, 543);
  }

  @Test
  public void testScanAtRegionBoundary() throws InterruptedException, ExecutionException {
    testScan(222, 333);
  }

  @Test
  public void testReversedScanAtRegionBoundary() throws InterruptedException, ExecutionException {
    testScan(222, 333);
  }

  @Test
  public void testScanWithLimit() throws InterruptedException, ExecutionException {
    AsyncTable table = ASYNC_CONN.getTable(TABLE_NAME);
    int start = 111;
    int stop = 888;
    int limit = 300;
    List<Result> results = table.smallScan(new Scan(Bytes.toBytes(String.format("%03d", start)))
        .setStopRow(Bytes.toBytes(String.format("%03d", stop))).setSmall(true),
      limit).get();
    assertEquals(limit, results.size());
    IntStream.range(0, limit).forEach(i -> {
      Result result = results.get(i);
      int actualIndex = start + i;
      assertEquals(String.format("%03d", actualIndex), Bytes.toString(result.getRow()));
      assertEquals(actualIndex, Bytes.toInt(result.getValue(FAMILY, QUALIFIER)));
    });
  }

  @Test
  public void testReversedScanWithLimit() throws InterruptedException, ExecutionException {
    AsyncTable table = ASYNC_CONN.getTable(TABLE_NAME);
    int start = 888;
    int stop = 111;
    int limit = 300;
    List<Result> results = table.smallScan(
      new Scan(Bytes.toBytes(String.format("%03d", start)))
          .setStopRow(Bytes.toBytes(String.format("%03d", stop))).setSmall(true).setReversed(true),
      limit).get();
    assertEquals(limit, results.size());
    IntStream.range(0, limit).forEach(i -> {
      Result result = results.get(i);
      int actualIndex = start - i;
      assertEquals(String.format("%03d", actualIndex), Bytes.toString(result.getRow()));
      assertEquals(actualIndex, Bytes.toInt(result.getValue(FAMILY, QUALIFIER)));
    });
  }
}
