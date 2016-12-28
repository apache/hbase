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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
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
    ASYNC_CONN.getRawTable(TABLE_NAME).putAll(IntStream.range(0, COUNT)
        .mapToObj(i -> new Put(Bytes.toBytes(String.format("%03d", i)))
            .addColumn(FAMILY, CQ1, Bytes.toBytes(i)).addColumn(FAMILY, CQ2, Bytes.toBytes(i * i)))
        .collect(Collectors.toList())).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    ASYNC_CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  protected abstract Scan createScan();

  protected abstract List<Result> doScan(Scan scan) throws Exception;

  private Result convertToPartial(Result result) {
    return Result.create(result.rawCells(), result.getExists(), result.isStale(), true);
  }

  protected final List<Result> convertFromBatchResult(List<Result> results) {
    assertTrue(results.size() % 2 == 0);
    return IntStream.range(0, results.size() / 2).mapToObj(i -> {
      try {
        return Result.createCompleteResult(Arrays.asList(convertToPartial(results.get(2 * i)),
          convertToPartial(results.get(2 * i + 1))));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }).collect(Collectors.toList());
  }

  @Test
  public void testScanAll() throws Exception {
    List<Result> results = doScan(createScan());
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
    List<Result> results = doScan(createScan().setReversed(true));
    assertEquals(COUNT, results.size());
    IntStream.range(0, COUNT).forEach(i -> assertResultEquals(results.get(i), COUNT - i - 1));
  }

  @Test
  public void testScanNoStopKey() throws Exception {
    int start = 345;
    List<Result> results =
        doScan(createScan().withStartRow(Bytes.toBytes(String.format("%03d", start))));
    assertEquals(COUNT - start, results.size());
    IntStream.range(0, COUNT - start).forEach(i -> assertResultEquals(results.get(i), start + i));
  }

  @Test
  public void testReverseScanNoStopKey() throws Exception {
    int start = 765;
    List<Result> results = doScan(
      createScan().withStartRow(Bytes.toBytes(String.format("%03d", start))).setReversed(true));
    assertEquals(start + 1, results.size());
    IntStream.range(0, start + 1).forEach(i -> assertResultEquals(results.get(i), start - i));
  }

  private void testScan(int start, boolean startInclusive, int stop, boolean stopInclusive)
      throws Exception {
    List<Result> results = doScan(
      createScan().withStartRow(Bytes.toBytes(String.format("%03d", start)), startInclusive)
          .withStopRow(Bytes.toBytes(String.format("%03d", stop)), stopInclusive));
    int actualStart = startInclusive ? start : start + 1;
    int actualStop = stopInclusive ? stop + 1 : stop;
    assertEquals(actualStop - actualStart, results.size());
    IntStream.range(0, actualStop - actualStart)
        .forEach(i -> assertResultEquals(results.get(i), actualStart + i));
  }

  private void testReversedScan(int start, boolean startInclusive, int stop, boolean stopInclusive)
      throws Exception {
    List<Result> results = doScan(createScan()
        .withStartRow(Bytes.toBytes(String.format("%03d", start)), startInclusive)
        .withStopRow(Bytes.toBytes(String.format("%03d", stop)), stopInclusive).setReversed(true));
    int actualStart = startInclusive ? start : start - 1;
    int actualStop = stopInclusive ? stop - 1 : stop;
    assertEquals(actualStart - actualStop, results.size());
    IntStream.range(0, actualStart - actualStop)
        .forEach(i -> assertResultEquals(results.get(i), actualStart - i));
  }

  @Test
  public void testScanWithStartKeyAndStopKey() throws Exception {
    testScan(1, true, 998, false); // from first region to last region
    testScan(123, true, 345, true);
    testScan(234, true, 456, false);
    testScan(345, false, 567, true);
    testScan(456, false, 678, false);
  }

  @Test
  public void testReversedScanWithStartKeyAndStopKey() throws Exception {
    testReversedScan(998, true, 1, false); // from first region to first region
    testReversedScan(543, true, 321, true);
    testReversedScan(654, true, 432, false);
    testReversedScan(765, false, 543, true);
    testReversedScan(876, false, 654, false);
  }

  @Test
  public void testScanAtRegionBoundary() throws Exception {
    testScan(222, true, 333, true);
    testScan(333, true, 444, false);
    testScan(444, false, 555, true);
    testScan(555, false, 666, false);
  }

  @Test
  public void testReversedScanAtRegionBoundary() throws Exception {
    testReversedScan(333, true, 222, true);
    testReversedScan(444, true, 333, false);
    testReversedScan(555, false, 444, true);
    testReversedScan(666, false, 555, false);
  }
}
