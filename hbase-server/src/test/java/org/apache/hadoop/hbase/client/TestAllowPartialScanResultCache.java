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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.IntStream;

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
public class TestAllowPartialScanResultCache {

  private static byte[] CF = Bytes.toBytes("cf");

  private AllowPartialScanResultCache resultCache;

  @Before
  public void setUp() {
    resultCache = new AllowPartialScanResultCache();
  }

  @After
  public void tearDown() {
    resultCache.clear();
    resultCache = null;
  }

  private static Cell createCell(int key, int cq) {
    return new KeyValue(Bytes.toBytes(key), CF, Bytes.toBytes("cq" + cq), Bytes.toBytes(key));
  }

  @Test
  public void test() throws IOException {
    assertSame(ScanResultCache.EMPTY_RESULT_ARRAY,
      resultCache.addAndGet(ScanResultCache.EMPTY_RESULT_ARRAY, false));
    assertSame(ScanResultCache.EMPTY_RESULT_ARRAY,
      resultCache.addAndGet(ScanResultCache.EMPTY_RESULT_ARRAY, true));

    Cell[] cells1 = IntStream.range(0, 10).mapToObj(i -> createCell(1, i)).toArray(Cell[]::new);
    Cell[] cells2 = IntStream.range(0, 10).mapToObj(i -> createCell(2, i)).toArray(Cell[]::new);

    Result[] results1 = resultCache.addAndGet(
      new Result[] { Result.create(Arrays.copyOf(cells1, 5), null, false, true) }, false);
    assertEquals(1, results1.length);
    assertEquals(1, Bytes.toInt(results1[0].getRow()));
    assertEquals(5, results1[0].rawCells().length);
    IntStream.range(0, 5).forEach(
      i -> assertEquals(1, Bytes.toInt(results1[0].getValue(CF, Bytes.toBytes("cq" + i)))));

    Result[] results2 = resultCache.addAndGet(
      new Result[] { Result.create(Arrays.copyOfRange(cells1, 1, 10), null, false, true) }, false);
    assertEquals(1, results2.length);
    assertEquals(1, Bytes.toInt(results2[0].getRow()));
    assertEquals(5, results2[0].rawCells().length);
    IntStream.range(5, 10).forEach(
      i -> assertEquals(1, Bytes.toInt(results2[0].getValue(CF, Bytes.toBytes("cq" + i)))));

    Result[] results3 = resultCache
        .addAndGet(new Result[] { Result.create(cells1), Result.create(cells2) }, false);
    assertEquals(1, results3.length);
    assertEquals(2, Bytes.toInt(results3[0].getRow()));
    assertEquals(10, results3[0].rawCells().length);
    IntStream.range(0, 10).forEach(
      i -> assertEquals(2, Bytes.toInt(results3[0].getValue(CF, Bytes.toBytes("cq" + i)))));
  }
}
