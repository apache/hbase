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
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class, ClientTests.class })
public class TestBatchScanResultCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBatchScanResultCache.class);

  private static byte[] CF = Bytes.toBytes("cf");

  private BatchScanResultCache resultCache;

  @Before
  public void setUp() {
    resultCache = new BatchScanResultCache(4);
  }

  @After
  public void tearDown() {
    resultCache.clear();
    resultCache = null;
  }

  static Cell createCell(byte[] cf, int key, int cq) {
    return new KeyValue(Bytes.toBytes(key), cf, Bytes.toBytes("cq" + cq), Bytes.toBytes(key));
  }

  static Cell[] createCells(byte[] cf, int key, int numCqs) {
    Cell[] cells = new Cell[numCqs];
    for (int i = 0; i < numCqs; i++) {
      cells[i] = createCell(cf, key, i);
    }
    return cells;
  }

  private void assertResultEquals(Result result, int key, int start, int to) {
    assertEquals(to - start, result.size());
    for (int i = start; i < to; i++) {
      assertEquals(key, Bytes.toInt(result.getValue(CF, Bytes.toBytes("cq" + i))));
    }
    assertEquals(to - start == 4, result.mayHaveMoreCellsInRow());
  }

  @Test
  public void test() throws IOException {
    assertSame(ScanResultCache.EMPTY_RESULT_ARRAY,
      resultCache.addAndGet(ScanResultCache.EMPTY_RESULT_ARRAY, false));
    assertSame(ScanResultCache.EMPTY_RESULT_ARRAY,
      resultCache.addAndGet(ScanResultCache.EMPTY_RESULT_ARRAY, true));

    Cell[] cells1 = createCells(CF, 1, 10);
    Cell[] cells2 = createCells(CF, 2, 10);
    Cell[] cells3 = createCells(CF, 3, 10);
    assertEquals(0, resultCache.addAndGet(
      new Result[] { Result.create(Arrays.copyOf(cells1, 3), null, false, true) }, false).length);
    Result[] results = resultCache.addAndGet(
      new Result[] { Result.create(Arrays.copyOfRange(cells1, 3, 7), null, false, true),
          Result.create(Arrays.copyOfRange(cells1, 7, 10), null, false, true) },
      false);
    assertEquals(2, results.length);
    assertResultEquals(results[0], 1, 0, 4);
    assertResultEquals(results[1], 1, 4, 8);
    results = resultCache.addAndGet(ScanResultCache.EMPTY_RESULT_ARRAY, false);
    assertEquals(1, results.length);
    assertResultEquals(results[0], 1, 8, 10);

    results = resultCache.addAndGet(
      new Result[] { Result.create(Arrays.copyOfRange(cells2, 0, 4), null, false, true),
          Result.create(Arrays.copyOfRange(cells2, 4, 8), null, false, true),
          Result.create(Arrays.copyOfRange(cells2, 8, 10), null, false, true),
          Result.create(Arrays.copyOfRange(cells3, 0, 4), null, false, true),
          Result.create(Arrays.copyOfRange(cells3, 4, 8), null, false, true),
          Result.create(Arrays.copyOfRange(cells3, 8, 10), null, false, false) },
      false);
    assertEquals(6, results.length);
    assertResultEquals(results[0], 2, 0, 4);
    assertResultEquals(results[1], 2, 4, 8);
    assertResultEquals(results[2], 2, 8, 10);
    assertResultEquals(results[3], 3, 0, 4);
    assertResultEquals(results[4], 3, 4, 8);
    assertResultEquals(results[5], 3, 8, 10);
  }
}
