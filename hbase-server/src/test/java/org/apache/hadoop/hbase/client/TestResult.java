/*
 *
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

import static org.apache.hadoop.hbase.HBaseTestCase.assertByteEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestResult extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestResult.class.getName());

  static KeyValue[] genKVs(final byte[] row, final byte[] family,
                           final byte[] value,
                    final long timestamp,
                    final int cols) {
    KeyValue [] kvs = new KeyValue[cols];

    for (int i = 0; i < cols ; i++) {
      kvs[i] = new KeyValue(
          row, family, Bytes.toBytes(i),
          timestamp,
          Bytes.add(value, Bytes.toBytes(i)));
    }
    return kvs;
  }

  static final byte [] row = Bytes.toBytes("row");
  static final byte [] family = Bytes.toBytes("family");
  static final byte [] value = Bytes.toBytes("value");

  /**
   * Run some tests to ensure Result acts like a proper CellScanner.
   * @throws IOException
   */
  public void testResultAsCellScanner() throws IOException {
    Cell [] cells = genKVs(row, family, value, 1, 10);
    Arrays.sort(cells, KeyValue.COMPARATOR);
    Result r = Result.create(cells);
    assertSame(r, cells);
    // Assert I run over same result multiple times.
    assertSame(r.cellScanner(), cells);
    assertSame(r.cellScanner(), cells);
    // Assert we are not creating new object when doing cellscanner
    assertTrue(r == r.cellScanner());
  }

  private void assertSame(final CellScanner cellScanner, final Cell [] cells) throws IOException {
    int count = 0;
    while (cellScanner.advance()) {
      assertTrue(cells[count].equals(cellScanner.current()));
      count++;
    }
    assertEquals(cells.length, count);
  }

  public void testBasicGetColumn() throws Exception {
    KeyValue [] kvs = genKVs(row, family, value, 1, 100);

    Arrays.sort(kvs, KeyValue.COMPARATOR);

    Result r = Result.create(kvs);

    for (int i = 0; i < 100; ++i) {
      final byte[] qf = Bytes.toBytes(i);

      List<Cell> ks = r.getColumnCells(family, qf);
      assertEquals(1, ks.size());
      assertTrue(CellUtil.matchingQualifier(ks.get(0), qf));
      assertEquals(ks.get(0), r.getColumnLatestCell(family, qf));
    }
  }

  public void testMultiVersionGetColumn() throws Exception {
    KeyValue [] kvs1 = genKVs(row, family, value, 1, 100);
    KeyValue [] kvs2 = genKVs(row, family, value, 200, 100);

    KeyValue [] kvs = new KeyValue[kvs1.length+kvs2.length];
    System.arraycopy(kvs1, 0, kvs, 0, kvs1.length);
    System.arraycopy(kvs2, 0, kvs, kvs1.length, kvs2.length);

    Arrays.sort(kvs, KeyValue.COMPARATOR);

    Result r = Result.create(kvs);
    for (int i = 0; i < 100; ++i) {
      final byte[] qf = Bytes.toBytes(i);

      List<Cell> ks = r.getColumnCells(family, qf);
      assertEquals(2, ks.size());
      assertTrue(CellUtil.matchingQualifier(ks.get(0), qf));
      assertEquals(200, ks.get(0).getTimestamp());
      assertEquals(ks.get(0), r.getColumnLatestCell(family, qf));
    }
  }

  public void testBasicGetValue() throws Exception {
    KeyValue [] kvs = genKVs(row, family, value, 1, 100);

    Arrays.sort(kvs, KeyValue.COMPARATOR);

    Result r = Result.create(kvs);

    for (int i = 0; i < 100; ++i) {
      final byte[] qf = Bytes.toBytes(i);

      assertByteEquals(Bytes.add(value, Bytes.toBytes(i)), r.getValue(family, qf));
      assertTrue(r.containsColumn(family, qf));
    }
  }

  public void testMultiVersionGetValue() throws Exception {
    KeyValue [] kvs1 = genKVs(row, family, value, 1, 100);
    KeyValue [] kvs2 = genKVs(row, family, value, 200, 100);

    KeyValue [] kvs = new KeyValue[kvs1.length+kvs2.length];
    System.arraycopy(kvs1, 0, kvs, 0, kvs1.length);
    System.arraycopy(kvs2, 0, kvs, kvs1.length, kvs2.length);

    Arrays.sort(kvs, KeyValue.COMPARATOR);

    Result r = Result.create(kvs);
    for (int i = 0; i < 100; ++i) {
      final byte[] qf = Bytes.toBytes(i);

      assertByteEquals(Bytes.add(value, Bytes.toBytes(i)), r.getValue(family, qf));
      assertTrue(r.containsColumn(family, qf));
    }
  }

  public void testBasicLoadValue() throws Exception {
    KeyValue [] kvs = genKVs(row, family, value, 1, 100);

    Arrays.sort(kvs, KeyValue.COMPARATOR);

    Result r = Result.create(kvs);
    ByteBuffer loadValueBuffer = ByteBuffer.allocate(1024);

    for (int i = 0; i < 100; ++i) {
      final byte[] qf = Bytes.toBytes(i);

      loadValueBuffer.clear();
      r.loadValue(family, qf, loadValueBuffer);
      loadValueBuffer.flip();
      assertEquals(ByteBuffer.wrap(Bytes.add(value, Bytes.toBytes(i))), loadValueBuffer);
      assertEquals(ByteBuffer.wrap(Bytes.add(value, Bytes.toBytes(i))),
          r.getValueAsByteBuffer(family, qf));
    }
  }

  public void testMultiVersionLoadValue() throws Exception {
    KeyValue [] kvs1 = genKVs(row, family, value, 1, 100);
    KeyValue [] kvs2 = genKVs(row, family, value, 200, 100);

    KeyValue [] kvs = new KeyValue[kvs1.length+kvs2.length];
    System.arraycopy(kvs1, 0, kvs, 0, kvs1.length);
    System.arraycopy(kvs2, 0, kvs, kvs1.length, kvs2.length);

    Arrays.sort(kvs, KeyValue.COMPARATOR);

    ByteBuffer loadValueBuffer = ByteBuffer.allocate(1024);

    Result r = Result.create(kvs);
    for (int i = 0; i < 100; ++i) {
      final byte[] qf = Bytes.toBytes(i);

      loadValueBuffer.clear();
      r.loadValue(family, qf, loadValueBuffer);
      loadValueBuffer.flip();
      assertEquals(ByteBuffer.wrap(Bytes.add(value, Bytes.toBytes(i))), loadValueBuffer);
      assertEquals(ByteBuffer.wrap(Bytes.add(value, Bytes.toBytes(i))),
          r.getValueAsByteBuffer(family, qf));
    }
  }

  /**
   * Verify that Result.compareResults(...) behaves correctly.
   */
  public void testCompareResults() throws Exception {
    byte [] value1 = Bytes.toBytes("value1");
    byte [] qual = Bytes.toBytes("qual");

    KeyValue kv1 = new KeyValue(row, family, qual, value);
    KeyValue kv2 = new KeyValue(row, family, qual, value1);

    Result r1 = Result.create(new KeyValue[] {kv1});
    Result r2 = Result.create(new KeyValue[] {kv2});
    // no exception thrown
    Result.compareResults(r1, r1);
    try {
      // these are different (HBASE-4800)
      Result.compareResults(r1, r2);
      fail();
    } catch (Exception x) {
      assertTrue(x.getMessage().startsWith("This result was different:"));
    }
  }

  /**
   * Verifies that one can't modify instance of EMPTY_RESULT.
   */
  public void testEmptyResultIsReadonly() {
    Result emptyResult = Result.EMPTY_RESULT;
    Result otherResult = new Result();

    try {
      emptyResult.copyFrom(otherResult);
      fail("UnsupportedOperationException should have been thrown!");
    } catch (UnsupportedOperationException ex) {
      LOG.debug("As expected: " + ex.getMessage());
    }
    try {
      emptyResult.addResults(ClientProtos.RegionLoadStats.getDefaultInstance());
      fail("UnsupportedOperationException should have been thrown!");
    } catch (UnsupportedOperationException ex) {
      LOG.debug("As expected: " + ex.getMessage());
    }
    try {
      emptyResult.setExists(true);
      fail("UnsupportedOperationException should have been thrown!");
    } catch (UnsupportedOperationException ex) {
      LOG.debug("As expected: " + ex.getMessage());
    }
  }

  /**
   * Microbenchmark that compares {@link Result#getValue} and {@link Result#loadValue} performance.
   *
   * @throws Exception
   */
  public void doReadBenchmark() throws Exception {

    final int n = 5;
    final int m = 100000000;

    StringBuilder valueSB = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      valueSB.append((byte)(Math.random() * 10));
    }

    StringBuilder rowSB = new StringBuilder();
    for (int i = 0; i < 50; i++) {
      rowSB.append((byte)(Math.random() * 10));
    }

    KeyValue [] kvs = genKVs(Bytes.toBytes(rowSB.toString()), family,
        Bytes.toBytes(valueSB.toString()), 1, n);
    Arrays.sort(kvs, KeyValue.COMPARATOR);
    ByteBuffer loadValueBuffer = ByteBuffer.allocate(1024);
    Result r = Result.create(kvs);

    byte[][] qfs = new byte[n][Bytes.SIZEOF_INT];
    for (int i = 0; i < n; ++i) {
      System.arraycopy(qfs[i], 0, Bytes.toBytes(i), 0, Bytes.SIZEOF_INT);
    }

    // warm up
    for (int k = 0; k < 100000; k++) {
      for (int i = 0; i < n; ++i) {
        r.getValue(family, qfs[i]);
        loadValueBuffer.clear();
        r.loadValue(family, qfs[i], loadValueBuffer);
        loadValueBuffer.flip();
      }
    }

    System.gc();
    long start = System.nanoTime();
    for (int k = 0; k < m; k++) {
      for (int i = 0; i < n; ++i) {
        loadValueBuffer.clear();
        r.loadValue(family, qfs[i], loadValueBuffer);
        loadValueBuffer.flip();
      }
    }
    long stop = System.nanoTime();
    System.out.println("loadValue(): " + (stop - start));

    System.gc();
    start = System.nanoTime();
    for (int k = 0; k < m; k++) {
      for (int i = 0; i < n; i++) {
        r.getValue(family, qfs[i]);
      }
    }
    stop = System.nanoTime();
    System.out.println("getValue():  " + (stop - start));
  }

  /**
   * Calls non-functional test methods.
   *
   * @param args
   */
  public static void main(String[] args) {
    TestResult testResult = new TestResult();
    try {
      testResult.doReadBenchmark();
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
    }
  }
}
