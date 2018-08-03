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
import java.util.LinkedList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class, ClientTests.class })
public class TestCompleteResultScanResultCache {

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ1 = Bytes.toBytes("cq1");

  private static byte[] CQ2 = Bytes.toBytes("cq2");

  private static byte[] CQ3 = Bytes.toBytes("cq3");

  private CompleteScanResultCache resultCache;

  private final LinkedList<Result> cache = new LinkedList<Result>();

  @Before
  public void setUp() {
    resultCache = new CompleteScanResultCache(cache);
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
    cache.clear();

    resultCache.loadResultsToCache(ScanResultCache.EMPTY_RESULT_ARRAY, false);
    assertEquals(0, cache.size());
    resultCache.loadResultsToCache(ScanResultCache.EMPTY_RESULT_ARRAY, true);
    assertEquals(0, cache.size());

    int count = 10;
    Result[] results = new Result[count];
    for (int i = 0; i < count; i++) {
      results[i] = Result.create(Arrays.asList(createCell(i, CQ1)));
    }
    resultCache.loadResultsToCache(results, false);
    results = cache.toArray(new Result[0]);
    assertEquals(count, results.length);
    for (int i = 0; i < count; i++) {
      assertEquals(i, Bytes.toInt(results[i].getRow()));
      assertEquals(i, Bytes.toInt(results[i].getValue(CF, CQ1)));
    }
  }

  @Test
  public void testCombine1() throws IOException {
    cache.clear();

    Result previousResult = Result.create(Arrays.asList(createCell(0, CQ1)), null, false, true);
    Result result1 = Result.create(Arrays.asList(createCell(1, CQ1)), null, false, true);
    Result result2 = Result.create(Arrays.asList(createCell(1, CQ2)), null, false, true);
    Result result3 = Result.create(Arrays.asList(createCell(1, CQ3)), null, false, true);
    resultCache.loadResultsToCache(new Result[] { previousResult, result1 }, false);
    Result[] results = cache.toArray(new Result[0]);
    assertEquals(1, results.length);
    assertSame(previousResult, results[0]);

    cache.clear();
    resultCache.loadResultsToCache(new Result[] { result2 }, false);
    assertEquals(0, cache.size());
    resultCache.loadResultsToCache(new Result[] { result3 }, false);
    assertEquals(0, cache.size());
    resultCache.loadResultsToCache(new Result[0], true);
    assertEquals(0, cache.size());

    resultCache.loadResultsToCache(new Result[0], false);
    results = cache.toArray(new Result[0]);
    assertEquals(1, results.length);
    assertEquals(1, Bytes.toInt(results[0].getRow()));
    assertEquals(3, results[0].rawCells().length);
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ1)));
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ2)));
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ3)));
  }

  @Test
  public void testCombine2() throws IOException {
    cache.clear();

    Result result1 = Result.create(Arrays.asList(createCell(1, CQ1)), null, false, true);
    Result result2 = Result.create(Arrays.asList(createCell(1, CQ2)), null, false, true);
    Result result3 = Result.create(Arrays.asList(createCell(1, CQ3)), null, false, true);
    Result nextResult1 = Result.create(Arrays.asList(createCell(2, CQ1)), null, false, true);
    Result nextToNextResult1 = Result.create(Arrays.asList(createCell(3, CQ2)), null, false, false);

    resultCache.loadResultsToCache(new Result[] { result1 }, false);
    assertEquals(0, cache.size());
    resultCache.loadResultsToCache(new Result[] { result2 }, false);
    assertEquals(0, cache.size());
    resultCache.loadResultsToCache(new Result[] { result3 }, false);
    assertEquals(0, cache.size());

    resultCache.loadResultsToCache(new Result[] { nextResult1 }, false);
    Result[] results = cache.toArray(new Result[0]);
    assertEquals(1, results.length);
    assertEquals(1, Bytes.toInt(results[0].getRow()));
    assertEquals(3, results[0].rawCells().length);
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ1)));
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ2)));
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ3)));

    cache.clear();
    resultCache.loadResultsToCache(new Result[] { nextToNextResult1 }, false);
    results = cache.toArray(new Result[0]);
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
    cache.clear();

    Result result1 = Result.create(Arrays.asList(createCell(1, CQ1)), null, false, true);
    Result result2 = Result.create(Arrays.asList(createCell(1, CQ2)), null, false, true);
    Result nextResult1 = Result.create(Arrays.asList(createCell(2, CQ1)), null, false, false);
    Result nextToNextResult1 = Result.create(Arrays.asList(createCell(3, CQ1)), null, false, true);

    resultCache.loadResultsToCache(new Result[] { result1 }, false);
    assertEquals(0, cache.size());
    resultCache.loadResultsToCache(new Result[] { result2 }, false);
    assertEquals(0, cache.size());

    resultCache.loadResultsToCache(new Result[] { nextResult1, nextToNextResult1 }, false);
    Result[] results = cache.toArray(new Result[0]);
    assertEquals(2, results.length);
    assertEquals(1, Bytes.toInt(results[0].getRow()));
    assertEquals(2, results[0].rawCells().length);
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ1)));
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ2)));
    assertEquals(2, Bytes.toInt(results[1].getRow()));
    assertEquals(1, results[1].rawCells().length);
    assertEquals(2, Bytes.toInt(results[1].getValue(CF, CQ1)));

    cache.clear();
    resultCache.loadResultsToCache(new Result[0], false);
    results = cache.toArray(new Result[0]);
    assertEquals(1, results.length);
    assertEquals(3, Bytes.toInt(results[0].getRow()));
    assertEquals(1, results[0].rawCells().length);
    assertEquals(3, Bytes.toInt(results[0].getValue(CF, CQ1)));
  }

  @Test
  public void testCombine4() throws IOException {
    cache.clear();

    Result result1 = Result.create(Arrays.asList(createCell(1, CQ1)), null, false, true);
    Result result2 = Result.create(Arrays.asList(createCell(1, CQ2)), null, false, false);
    Result nextResult1 = Result.create(Arrays.asList(createCell(2, CQ1)), null, false, true);
    Result nextResult2 = Result.create(Arrays.asList(createCell(2, CQ2)), null, false, false);

    resultCache.loadResultsToCache(new Result[] { result1 }, false);
    assertEquals(0, cache.size());

    resultCache.loadResultsToCache(new Result[] { result2, nextResult1 }, false);
    Result[] results = cache.toArray(new Result[0]);
    assertEquals(1, results.length);
    assertEquals(1, Bytes.toInt(results[0].getRow()));
    assertEquals(2, results[0].rawCells().length);
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ1)));
    assertEquals(1, Bytes.toInt(results[0].getValue(CF, CQ2)));

    cache.clear();
    resultCache.loadResultsToCache(new Result[] { nextResult2 }, false);
    results = cache.toArray(new Result[0]);
    assertEquals(1, results.length);
    assertEquals(2, Bytes.toInt(results[0].getRow()));
    assertEquals(2, results[0].rawCells().length);
    assertEquals(2, Bytes.toInt(results[0].getValue(CF, CQ1)));
    assertEquals(2, Bytes.toInt(results[0].getValue(CF, CQ2)));
  }
}
