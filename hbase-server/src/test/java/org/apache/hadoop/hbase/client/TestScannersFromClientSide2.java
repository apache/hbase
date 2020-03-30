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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Testcase for newly added feature in HBASE-17143, such as startRow and stopRow
 * inclusive/exclusive, limit for rows, etc.
 */
@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestScannersFromClientSide2 {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestScannersFromClientSide2.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("scan");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] CQ1 = Bytes.toBytes("cq1");

  private static byte[] CQ2 = Bytes.toBytes("cq2");

  @Parameter(0)
  public boolean batch;

  @Parameter(1)
  public boolean smallResultSize;

  @Parameter(2)
  public boolean allowPartial;

  @Parameters(name = "{index}: batch={0}, smallResultSize={1}, allowPartial={2}")
  public static List<Object[]> params() {
    List<Object[]> params = new ArrayList<>();
    boolean[] values = new boolean[] { false, true };
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 2; j++) {
        for (int k = 0; k < 2; k++) {
          params.add(new Object[] { values[i], values[j], values[k] });
        }
      }
    }
    return params;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    byte[][] splitKeys = new byte[8][];
    for (int i = 111; i < 999; i += 111) {
      splitKeys[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
    }
    Table table = TEST_UTIL.createTable(TABLE_NAME, FAMILY, splitKeys);
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      puts.add(new Put(Bytes.toBytes(String.format("%03d", i)))
          .addColumn(FAMILY, CQ1, Bytes.toBytes(i)).addColumn(FAMILY, CQ2, Bytes.toBytes(i * i)));
    }
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    table.put(puts);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private Scan createScan() {
    Scan scan = new Scan();
    if (batch) {
      scan.setBatch(1);
    }
    if (smallResultSize) {
      scan.setMaxResultSize(1);
    }
    if (allowPartial) {
      scan.setAllowPartialResults(true);
    }
    return scan;
  }

  private void assertResultEquals(Result result, int i) {
    assertEquals(String.format("%03d", i), Bytes.toString(result.getRow()));
    assertEquals(i, Bytes.toInt(result.getValue(FAMILY, CQ1)));
    assertEquals(i * i, Bytes.toInt(result.getValue(FAMILY, CQ2)));
  }

  private List<Result> doScan(Scan scan) throws IOException {
    List<Result> results = new ArrayList<>();
    try (Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME);
        ResultScanner scanner = table.getScanner(scan)) {
      for (Result r; (r = scanner.next()) != null;) {
        results.add(r);
      }
    }
    return assertAndCreateCompleteResults(results);
  }

  private List<Result> assertAndCreateCompleteResults(List<Result> results) throws IOException {
    if ((!batch && !allowPartial) || (allowPartial && !batch && !smallResultSize)) {
      for (Result result : results) {
        assertFalse("Should not have partial result", result.mayHaveMoreCellsInRow());
      }
      return results;
    }
    List<Result> completeResults = new ArrayList<>();
    List<Result> partialResults = new ArrayList<>();
    for (Result result : results) {
      if (!result.mayHaveMoreCellsInRow()) {
        assertFalse("Should have partial result", partialResults.isEmpty());
        partialResults.add(result);
        completeResults.add(Result.createCompleteResult(partialResults));
        partialResults.clear();
      } else {
        partialResults.add(result);
      }
    }
    assertTrue("Should not have orphan partial result", partialResults.isEmpty());
    return completeResults;
  }

  private void testScan(int start, boolean startInclusive, int stop, boolean stopInclusive,
      int limit) throws Exception {
    Scan scan =
        createScan().withStartRow(Bytes.toBytes(String.format("%03d", start)), startInclusive)
            .withStopRow(Bytes.toBytes(String.format("%03d", stop)), stopInclusive);
    if (limit > 0) {
      scan.setLimit(limit);
    }
    List<Result> results = doScan(scan);
    int actualStart = startInclusive ? start : start + 1;
    int actualStop = stopInclusive ? stop + 1 : stop;
    int count = actualStop - actualStart;
    if (limit > 0) {
      count = Math.min(count, limit);
    }
    assertEquals(count, results.size());
    for (int i = 0; i < count; i++) {
      assertResultEquals(results.get(i), actualStart + i);
    }
  }

  private void testReversedScan(int start, boolean startInclusive, int stop, boolean stopInclusive,
      int limit) throws Exception {
    Scan scan = createScan()
        .withStartRow(Bytes.toBytes(String.format("%03d", start)), startInclusive)
        .withStopRow(Bytes.toBytes(String.format("%03d", stop)), stopInclusive).setReversed(true);
    if (limit > 0) {
      scan.setLimit(limit);
    }
    List<Result> results = doScan(scan);
    int actualStart = startInclusive ? start : start - 1;
    int actualStop = stopInclusive ? stop - 1 : stop;
    int count = actualStart - actualStop;
    if (limit > 0) {
      count = Math.min(count, limit);
    }
    assertEquals(count, results.size());
    for (int i = 0; i < count; i++) {
      assertResultEquals(results.get(i), actualStart - i);
    }
  }

  @Test
  public void testScanWithLimit() throws Exception {
    testScan(1, true, 998, false, 900); // from first region to last region
    testScan(123, true, 345, true, 100);
    testScan(234, true, 456, false, 100);
    testScan(345, false, 567, true, 100);
    testScan(456, false, 678, false, 100);

  }

  @Test
  public void testScanWithLimitGreaterThanActualCount() throws Exception {
    testScan(1, true, 998, false, 1000); // from first region to last region
    testScan(123, true, 345, true, 200);
    testScan(234, true, 456, false, 200);
    testScan(345, false, 567, true, 200);
    testScan(456, false, 678, false, 200);
  }

  @Test
  public void testReversedScanWithLimit() throws Exception {
    testReversedScan(998, true, 1, false, 900); // from last region to first region
    testReversedScan(543, true, 321, true, 100);
    testReversedScan(654, true, 432, false, 100);
    testReversedScan(765, false, 543, true, 100);
    testReversedScan(876, false, 654, false, 100);
  }

  @Test
  public void testReversedScanWithLimitGreaterThanActualCount() throws Exception {
    testReversedScan(998, true, 1, false, 1000); // from last region to first region
    testReversedScan(543, true, 321, true, 200);
    testReversedScan(654, true, 432, false, 200);
    testReversedScan(765, false, 543, true, 200);
    testReversedScan(876, false, 654, false, 200);
  }

  @Test
  public void testStartRowStopRowInclusive() throws Exception {
    testScan(1, true, 998, false, -1); // from first region to last region
    testScan(123, true, 345, true, -1);
    testScan(234, true, 456, false, -1);
    testScan(345, false, 567, true, -1);
    testScan(456, false, 678, false, -1);
  }

  @Test
  public void testReversedStartRowStopRowInclusive() throws Exception {
    testReversedScan(998, true, 1, false, -1); // from last region to first region
    testReversedScan(543, true, 321, true, -1);
    testReversedScan(654, true, 432, false, -1);
    testReversedScan(765, false, 543, true, -1);
    testReversedScan(876, false, 654, false, -1);
  }
}
