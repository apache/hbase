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
public class TestCompleteResultScanResultCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompleteResultScanResultCache.class);

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ1 = Bytes.toBytes("cq1");

  private static byte[] CQ2 = Bytes.toBytes("cq2");

  private static byte[] CQ3 = Bytes.toBytes("cq3");

  private CompleteScanResultCache resultCache;

  @Before
  public void setUp() {
    resultCache = new CompleteScanResultCache();
  }

  @After
  public void tearDown() {
    resultCache.clear();
    resultCache = null;
  }

  private static Cell createCell(int key, byte[] cq) {
    return new KeyValue(Bytes.toBytes(key), CF, cq, Bytes.toBytes(key));
  }

  @Test
  public void testNoPartial() throws IOException {
    assertSame(ScanResultCache.EMPTY_RESULT_ARRAY,
      resultCache.addAndGet(ScanResultCache.EMPTY_RESULT_ARRAY, false));
    assertSame(ScanResultCache.EMPTY_RESULT_ARRAY,
      resultCache.addAndGet(ScanResultCache.EMPTY_RESULT_ARRAY, true));
    int count = 10;
    Result[] results = new Result[count];
    for (int i = 0; i < count; i++) {
      results[i] = Result.create(Arrays.asList(createCell(i, CQ1)));
    }
    assertSame(results, resultCache.addAndGet(results, false));
  }

  @Test
  public void testCombine1() throws IOException {
    Result previousResult = Result.create(Arrays.asList(createCell(0, CQ1)), null, false, true);
    Result result1 = Result.create(Arrays.asList(createCell(1, CQ1)), null, false, true);
    Result result2 = Result.create(Arrays.asList(createCell(1, CQ2)), null, false, true);
    Result result3 = Result.create(Arrays.asList(createCell(1, CQ3)), null, false, true);
    Result[] results = resultCache.addAndGet(new Result[] { previousResult, result1 }, false);
    assertEquals(1, results.length);
    assertSame(previousResult, results[0]);

    assertEquals(0, resultCache.addAndGet(new Result[] { result2 }, false).length);
    assertEquals(0, resultCache.addAndGet(new Result[] { result3 }, false).length);
    assertEquals(0, resultCache.addAndGet(new Result[0], true).length);

    results = resultCache.addAndGet(new Result[0], false);
    assertEquals(1, results.length);
    assertEquals(1, Bytes.toInt(results[0].getRow()));
    assertEquals(3, results[0].rawCells().length);
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ1)));
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ2)));
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ3)));
  }

  @Test
  public void testCombine2() throws IOException {
    Result result1 = Result.create(Arrays.asList(createCell(1, CQ1)), null, false, true);
    Result result2 = Result.create(Arrays.asList(createCell(1, CQ2)), null, false, true);
    Result result3 = Result.create(Arrays.asList(createCell(1, CQ3)), null, false, true);
    Result nextResult1 = Result.create(Arrays.asList(createCell(2, CQ1)), null, false, true);
    Result nextToNextResult1 = Result.create(Arrays.asList(createCell(3, CQ2)), null, false, false);

    assertEquals(0, resultCache.addAndGet(new Result[] { result1 }, false).length);
    assertEquals(0, resultCache.addAndGet(new Result[] { result2 }, false).length);
    assertEquals(0, resultCache.addAndGet(new Result[] { result3 }, false).length);

    Result[] results = resultCache.addAndGet(new Result[] { nextResult1 }, false);
    assertEquals(1, results.length);
    assertEquals(1, Bytes.toInt(results[0].getRow()));
    assertEquals(3, results[0].rawCells().length);
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ1)));
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ2)));
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ3)));

    results = resultCache.addAndGet(new Result[] { nextToNextResult1 }, false);
    assertEquals(2, results.length);
    assertEquals(2, Bytes.toInt(results[0].getRow()));
    assertEquals(1, results[0].rawCells().length);
    assertEquals(2, Bytes.toInt(results[0].getValue(CF, CQ1)));
    assertEquals(3, Bytes.toInt(results[1].getRow()));
    assertEquals(1, results[1].rawCells().length);
    assertEquals(3, Bytes.toInt(results[1].getValue(CF, CQ2)));
  }

  @Test
  public void testCombine3() throws IOException {
    Result result1 = Result.create(Arrays.asList(createCell(1, CQ1)), null, false, true);
    Result result2 = Result.create(Arrays.asList(createCell(1, CQ2)), null, false, true);
    Result nextResult1 = Result.create(Arrays.asList(createCell(2, CQ1)), null, false, false);
    Result nextToNextResult1 = Result.create(Arrays.asList(createCell(3, CQ1)), null, false, true);

    assertEquals(0, resultCache.addAndGet(new Result[] { result1 }, false).length);
    assertEquals(0, resultCache.addAndGet(new Result[] { result2 }, false).length);

    Result[] results =
        resultCache.addAndGet(new Result[] { nextResult1, nextToNextResult1 }, false);
    assertEquals(2, results.length);
    assertEquals(1, Bytes.toInt(results[0].getRow()));
    assertEquals(2, results[0].rawCells().length);
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ1)));
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ2)));
    assertEquals(2, Bytes.toInt(results[1].getRow()));
    assertEquals(1, results[1].rawCells().length);
    assertEquals(2, Bytes.toInt(results[1].getValue(CF, CQ1)));

    results = resultCache.addAndGet(new Result[0], false);
    assertEquals(1, results.length);
    assertEquals(3, Bytes.toInt(results[0].getRow()));
    assertEquals(1, results[0].rawCells().length);
    assertEquals(3, Bytes.toInt(results[0].getValue(CF, CQ1)));
  }

  @Test
  public void testCombine4() throws IOException {
    Result result1 = Result.create(Arrays.asList(createCell(1, CQ1)), null, false, true);
    Result result2 = Result.create(Arrays.asList(createCell(1, CQ2)), null, false, false);
    Result nextResult1 = Result.create(Arrays.asList(createCell(2, CQ1)), null, false, true);
    Result nextResult2 = Result.create(Arrays.asList(createCell(2, CQ2)), null, false, false);

    assertEquals(0, resultCache.addAndGet(new Result[] { result1 }, false).length);

    Result[] results = resultCache.addAndGet(new Result[] { result2, nextResult1 }, false);
    assertEquals(1, results.length);
    assertEquals(1, Bytes.toInt(results[0].getRow()));
    assertEquals(2, results[0].rawCells().length);
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ1)));
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ2)));

    results = resultCache.addAndGet(new Result[] { nextResult2 }, false);
    assertEquals(1, results.length);
    assertEquals(2, Bytes.toInt(results[0].getRow()));
    assertEquals(2, results[0].rawCells().length);
    assertEquals(2, Bytes.toInt(results[0].getValue(CF, CQ1)));
    assertEquals(2, Bytes.toInt(results[0].getValue(CF, CQ2)));
  }
}
