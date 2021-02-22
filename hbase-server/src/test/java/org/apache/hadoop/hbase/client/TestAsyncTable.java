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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@RunWith(Parameterized.class)
@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTable.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUALIFIER = Bytes.toBytes("cq");

  private static byte[] VALUE = Bytes.toBytes("value");

  private static int MAX_KEY_VALUE_SIZE = 64 * 1024;

  private static AsyncConnection ASYNC_CONN;

  @Rule
  public TestName testName = new TestName();

  private byte[] row;

  @Parameter
  public Supplier<AsyncTable<?>> getTable;

  private static AsyncTable<?> getRawTable() {
    return ASYNC_CONN.getTable(TABLE_NAME);
  }

  private static AsyncTable<?> getTable() {
    return ASYNC_CONN.getTable(TABLE_NAME, ForkJoinPool.commonPool());
  }

  @Parameters
  public static List<Object[]> params() {
    return Arrays.asList(new Supplier<?>[] { TestAsyncTable::getRawTable },
      new Supplier<?>[] { TestAsyncTable::getTable });
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY,
      MAX_KEY_VALUE_SIZE);
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    assertFalse(ASYNC_CONN.isClosed());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(ASYNC_CONN, true);
    assertTrue(ASYNC_CONN.isClosed());
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException, InterruptedException, ExecutionException {
    row = Bytes.toBytes(testName.getMethodName().replaceAll("[^0-9A-Za-z]", "_"));
    if (ASYNC_CONN.getAdmin().isTableDisabled(TABLE_NAME).get()) {
      ASYNC_CONN.getAdmin().enableTable(TABLE_NAME).get();
    }
  }

  @Test
  public void testSimple() throws Exception {
    AsyncTable<?> table = getTable.get();
    table.put(new Put(row).addColumn(FAMILY, QUALIFIER, VALUE)).get();
    assertTrue(table.exists(new Get(row).addColumn(FAMILY, QUALIFIER)).get());
    Result result = table.get(new Get(row).addColumn(FAMILY, QUALIFIER)).get();
    assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
    table.delete(new Delete(row)).get();
    result = table.get(new Get(row).addColumn(FAMILY, QUALIFIER)).get();
    assertTrue(result.isEmpty());
    assertFalse(table.exists(new Get(row).addColumn(FAMILY, QUALIFIER)).get());
  }

  private byte[] concat(byte[] base, int index) {
    return Bytes.toBytes(Bytes.toString(base) + "-" + index);
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Test
  public void testSimpleMultiple() throws Exception {
    AsyncTable<?> table = getTable.get();
    int count = 100;
    CountDownLatch putLatch = new CountDownLatch(count);
    IntStream.range(0, count).forEach(
      i -> table.put(new Put(concat(row, i)).addColumn(FAMILY, QUALIFIER, concat(VALUE, i)))
        .thenAccept(x -> putLatch.countDown()));
    putLatch.await();
    BlockingQueue<Boolean> existsResp = new ArrayBlockingQueue<>(count);
    IntStream.range(0, count)
      .forEach(i -> table.exists(new Get(concat(row, i)).addColumn(FAMILY, QUALIFIER))
        .thenAccept(x -> existsResp.add(x)));
    for (int i = 0; i < count; i++) {
      assertTrue(existsResp.take());
    }
    BlockingQueue<Pair<Integer, Result>> getResp = new ArrayBlockingQueue<>(count);
    IntStream.range(0, count)
      .forEach(i -> table.get(new Get(concat(row, i)).addColumn(FAMILY, QUALIFIER))
        .thenAccept(x -> getResp.add(Pair.newPair(i, x))));
    for (int i = 0; i < count; i++) {
      Pair<Integer, Result> pair = getResp.take();
      assertArrayEquals(concat(VALUE, pair.getFirst()),
        pair.getSecond().getValue(FAMILY, QUALIFIER));
    }
    CountDownLatch deleteLatch = new CountDownLatch(count);
    IntStream.range(0, count).forEach(
      i -> table.delete(new Delete(concat(row, i))).thenAccept(x -> deleteLatch.countDown()));
    deleteLatch.await();
    IntStream.range(0, count)
      .forEach(i -> table.exists(new Get(concat(row, i)).addColumn(FAMILY, QUALIFIER))
        .thenAccept(x -> existsResp.add(x)));
    for (int i = 0; i < count; i++) {
      assertFalse(existsResp.take());
    }
    IntStream.range(0, count)
      .forEach(i -> table.get(new Get(concat(row, i)).addColumn(FAMILY, QUALIFIER))
        .thenAccept(x -> getResp.add(Pair.newPair(i, x))));
    for (int i = 0; i < count; i++) {
      Pair<Integer, Result> pair = getResp.take();
      assertTrue(pair.getSecond().isEmpty());
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Test
  public void testIncrement() throws InterruptedException, ExecutionException {
    AsyncTable<?> table = getTable.get();
    int count = 100;
    CountDownLatch latch = new CountDownLatch(count);
    AtomicLong sum = new AtomicLong(0L);
    IntStream.range(0, count)
      .forEach(i -> table.incrementColumnValue(row, FAMILY, QUALIFIER, 1).thenAccept(x -> {
        sum.addAndGet(x);
        latch.countDown();
      }));
    latch.await();
    assertEquals(count, Bytes.toLong(
      table.get(new Get(row).addColumn(FAMILY, QUALIFIER)).get().getValue(FAMILY, QUALIFIER)));
    assertEquals((1 + count) * count / 2, sum.get());
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Test
  public void testAppend() throws InterruptedException, ExecutionException {
    AsyncTable<?> table = getTable.get();
    int count = 10;
    CountDownLatch latch = new CountDownLatch(count);
    char suffix = ':';
    AtomicLong suffixCount = new AtomicLong(0L);
    IntStream.range(0, count)
      .forEachOrdered(i -> table
        .append(new Append(row).addColumn(FAMILY, QUALIFIER, Bytes.toBytes("" + i + suffix)))
        .thenAccept(r -> {
          suffixCount.addAndGet(
            Bytes.toString(r.getValue(FAMILY, QUALIFIER)).chars().filter(x -> x == suffix).count());
          latch.countDown();
        }));
    latch.await();
    assertEquals((1 + count) * count / 2, suffixCount.get());
    String value = Bytes.toString(
      table.get(new Get(row).addColumn(FAMILY, QUALIFIER)).get().getValue(FAMILY, QUALIFIER));
    int[] actual = Arrays.asList(value.split("" + suffix)).stream().mapToInt(Integer::parseInt)
      .sorted().toArray();
    assertArrayEquals(IntStream.range(0, count).toArray(), actual);
  }

  @Test
  public void testMutateRow() throws InterruptedException, ExecutionException, IOException {
    AsyncTable<?> table = getTable.get();
    RowMutations mutation = new RowMutations(row);
    mutation.add(new Put(row).addColumn(FAMILY, concat(QUALIFIER, 1), VALUE));
    Result result = table.mutateRow(mutation).get();
    assertTrue(result.getExists());
    assertTrue(result.isEmpty());

    result = table.get(new Get(row)).get();
    assertArrayEquals(VALUE, result.getValue(FAMILY, concat(QUALIFIER, 1)));

    mutation = new RowMutations(row);
    mutation.add(new Delete(row).addColumn(FAMILY, concat(QUALIFIER, 1)));
    mutation.add(new Put(row).addColumn(FAMILY, concat(QUALIFIER, 2), VALUE));
    mutation.add(new Increment(row).addColumn(FAMILY, concat(QUALIFIER, 3), 2L));
    mutation.add(new Append(row).addColumn(FAMILY, concat(QUALIFIER, 4), Bytes.toBytes("abc")));
    result = table.mutateRow(mutation).get();
    assertTrue(result.getExists());
    assertEquals(2L, Bytes.toLong(result.getValue(FAMILY, concat(QUALIFIER, 3))));
    assertEquals("abc", Bytes.toString(result.getValue(FAMILY, concat(QUALIFIER, 4))));

    result = table.get(new Get(row)).get();
    assertNull(result.getValue(FAMILY, concat(QUALIFIER, 1)));
    assertArrayEquals(VALUE, result.getValue(FAMILY, concat(QUALIFIER, 2)));
    assertEquals(2L, Bytes.toLong(result.getValue(FAMILY, concat(QUALIFIER, 3))));
    assertEquals("abc", Bytes.toString(result.getValue(FAMILY, concat(QUALIFIER, 4))));
  }

  // Tests for old checkAndMutate API

  @SuppressWarnings("FutureReturnValueIgnored")
  @Test
  @Deprecated
  public void testCheckAndPutForOldApi() throws InterruptedException, ExecutionException {
    AsyncTable<?> table = getTable.get();
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger successIndex = new AtomicInteger(-1);
    int count = 10;
    CountDownLatch latch = new CountDownLatch(count);
    IntStream.range(0, count)
      .forEach(i -> table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER).ifNotExists()
        .thenPut(new Put(row).addColumn(FAMILY, QUALIFIER, concat(VALUE, i))).thenAccept(x -> {
          if (x) {
            successCount.incrementAndGet();
            successIndex.set(i);
          }
          latch.countDown();
        }));
    latch.await();
    assertEquals(1, successCount.get());
    String actual = Bytes.toString(table.get(new Get(row)).get().getValue(FAMILY, QUALIFIER));
    assertTrue(actual.endsWith(Integer.toString(successIndex.get())));
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Test
  @Deprecated
  public void testCheckAndDeleteForOldApi() throws InterruptedException, ExecutionException {
    AsyncTable<?> table = getTable.get();
    int count = 10;
    CountDownLatch putLatch = new CountDownLatch(count + 1);
    table.put(new Put(row).addColumn(FAMILY, QUALIFIER, VALUE)).thenRun(() -> putLatch.countDown());
    IntStream.range(0, count)
      .forEach(i -> table.put(new Put(row).addColumn(FAMILY, concat(QUALIFIER, i), VALUE))
        .thenRun(() -> putLatch.countDown()));
    putLatch.await();

    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger successIndex = new AtomicInteger(-1);
    CountDownLatch deleteLatch = new CountDownLatch(count);
    IntStream.range(0, count)
      .forEach(i -> table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER).ifEquals(VALUE)
        .thenDelete(
          new Delete(row).addColumn(FAMILY, QUALIFIER).addColumn(FAMILY, concat(QUALIFIER, i)))
        .thenAccept(x -> {
          if (x) {
            successCount.incrementAndGet();
            successIndex.set(i);
          }
          deleteLatch.countDown();
        }));
    deleteLatch.await();
    assertEquals(1, successCount.get());
    Result result = table.get(new Get(row)).get();
    IntStream.range(0, count).forEach(i -> {
      if (i == successIndex.get()) {
        assertFalse(result.containsColumn(FAMILY, concat(QUALIFIER, i)));
      } else {
        assertArrayEquals(VALUE, result.getValue(FAMILY, concat(QUALIFIER, i)));
      }
    });
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Test
  @Deprecated
  public void testCheckAndMutateForOldApi() throws InterruptedException, ExecutionException {
    AsyncTable<?> table = getTable.get();
    int count = 10;
    CountDownLatch putLatch = new CountDownLatch(count + 1);
    table.put(new Put(row).addColumn(FAMILY, QUALIFIER, VALUE)).thenRun(() -> putLatch.countDown());
    IntStream.range(0, count)
      .forEach(i -> table.put(new Put(row).addColumn(FAMILY, concat(QUALIFIER, i), VALUE))
        .thenRun(() -> putLatch.countDown()));
    putLatch.await();

    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger successIndex = new AtomicInteger(-1);
    CountDownLatch mutateLatch = new CountDownLatch(count);
    IntStream.range(0, count).forEach(i -> {
      RowMutations mutation = new RowMutations(row);
      try {
        mutation.add((Mutation) new Delete(row).addColumn(FAMILY, QUALIFIER));
        mutation
          .add((Mutation) new Put(row).addColumn(FAMILY, concat(QUALIFIER, i), concat(VALUE, i)));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER).ifEquals(VALUE).thenMutate(mutation)
        .thenAccept(x -> {
          if (x) {
            successCount.incrementAndGet();
            successIndex.set(i);
          }
          mutateLatch.countDown();
        });
    });
    mutateLatch.await();
    assertEquals(1, successCount.get());
    Result result = table.get(new Get(row)).get();
    IntStream.range(0, count).forEach(i -> {
      if (i == successIndex.get()) {
        assertArrayEquals(concat(VALUE, i), result.getValue(FAMILY, concat(QUALIFIER, i)));
      } else {
        assertArrayEquals(VALUE, result.getValue(FAMILY, concat(QUALIFIER, i)));
      }
    });
  }

  @Test
  @Deprecated
  public void testCheckAndMutateWithTimeRangeForOldApi() throws Exception {
    AsyncTable<?> table = getTable.get();
    final long ts = System.currentTimeMillis() / 2;
    Put put = new Put(row);
    put.addColumn(FAMILY, QUALIFIER, ts, VALUE);

    boolean ok =
      table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER).ifNotExists().thenPut(put).get();
    assertTrue(ok);

    ok = table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER).timeRange(TimeRange.at(ts + 10000))
      .ifEquals(VALUE).thenPut(put).get();
    assertFalse(ok);

    ok = table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER).timeRange(TimeRange.at(ts))
      .ifEquals(VALUE).thenPut(put).get();
    assertTrue(ok);

    RowMutations rm = new RowMutations(row).add((Mutation) put);

    ok = table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER).timeRange(TimeRange.at(ts + 10000))
      .ifEquals(VALUE).thenMutate(rm).get();
    assertFalse(ok);

    ok = table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER).timeRange(TimeRange.at(ts))
      .ifEquals(VALUE).thenMutate(rm).get();
    assertTrue(ok);

    Delete delete = new Delete(row).addColumn(FAMILY, QUALIFIER);

    ok = table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER).timeRange(TimeRange.at(ts + 10000))
      .ifEquals(VALUE).thenDelete(delete).get();
    assertFalse(ok);

    ok = table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER).timeRange(TimeRange.at(ts))
      .ifEquals(VALUE).thenDelete(delete).get();
    assertTrue(ok);
  }

  @Test
  @Deprecated
  public void testCheckAndMutateWithSingleFilterForOldApi() throws Throwable {
    AsyncTable<?> table = getTable.get();

    // Put one row
    Put put = new Put(row);
    put.addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"));
    put.addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"));
    put.addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"));
    table.put(put).get();

    // Put with success
    boolean ok = table.checkAndMutate(row, new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
        CompareOperator.EQUAL, Bytes.toBytes("a")))
      .thenPut(new Put(row).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))
      .get();
    assertTrue(ok);

    Result result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("D"))).get();
    assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

    // Put with failure
    ok = table.checkAndMutate(row, new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
        CompareOperator.EQUAL, Bytes.toBytes("b")))
      .thenPut(new Put(row).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e")))
      .get();
    assertFalse(ok);

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("E"))).get());

    // Delete with success
    ok = table.checkAndMutate(row, new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
        CompareOperator.EQUAL, Bytes.toBytes("a")))
      .thenDelete(new Delete(row).addColumns(FAMILY, Bytes.toBytes("D")))
      .get();
    assertTrue(ok);

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("D"))).get());

    // Mutate with success
    ok = table.checkAndMutate(row, new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"),
        CompareOperator.EQUAL, Bytes.toBytes("b")))
      .thenMutate(new RowMutations(row)
        .add((Mutation) new Put(row)
          .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))
        .add((Mutation) new Delete(row).addColumns(FAMILY, Bytes.toBytes("A"))))
      .get();
    assertTrue(ok);

    result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("D"))).get();
    assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("A"))).get());
  }

  @Test
  @Deprecated
  public void testCheckAndMutateWithMultipleFiltersForOldApi() throws Throwable {
    AsyncTable<?> table = getTable.get();

    // Put one row
    Put put = new Put(row);
    put.addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"));
    put.addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"));
    put.addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"));
    table.put(put).get();

    // Put with success
    boolean ok = table.checkAndMutate(row, new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))
      ))
      .thenPut(new Put(row).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))
      .get();
    assertTrue(ok);

    Result result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("D"))).get();
    assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

    // Put with failure
    ok = table.checkAndMutate(row, new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("c"))
      ))
      .thenPut(new Put(row).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e")))
      .get();
    assertFalse(ok);

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("E"))).get());

    // Delete with success
    ok = table.checkAndMutate(row, new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))
      ))
      .thenDelete(new Delete(row).addColumns(FAMILY, Bytes.toBytes("D")))
      .get();
    assertTrue(ok);

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("D"))).get());

    // Mutate with success
    ok = table.checkAndMutate(row, new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))
      ))
      .thenMutate(new RowMutations(row)
        .add((Mutation) new Put(row)
          .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))
        .add((Mutation) new Delete(row).addColumns(FAMILY, Bytes.toBytes("A"))))
      .get();
    assertTrue(ok);

    result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("D"))).get();
    assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("A"))).get());
  }

  @Test
  @Deprecated
  public void testCheckAndMutateWithTimestampFilterForOldApi() throws Throwable {
    AsyncTable<?> table = getTable.get();

    // Put with specifying the timestamp
    table.put(new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), 100, Bytes.toBytes("a"))).get();

    // Put with success
    boolean ok = table.checkAndMutate(row, new FilterList(
        new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(FAMILY)),
        new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("A"))),
        new TimestampsFilter(Collections.singletonList(100L))
      ))
      .thenPut(new Put(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")))
      .get();
    assertTrue(ok);

    Result result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // Put with failure
    ok = table.checkAndMutate(row, new FilterList(
        new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(FAMILY)),
        new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("A"))),
        new TimestampsFilter(Collections.singletonList(101L))
      ))
      .thenPut(new Put(row).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")))
      .get();
    assertFalse(ok);

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("C"))).get());
  }

  @Test
  @Deprecated
  public void testCheckAndMutateWithFilterAndTimeRangeForOldApi() throws Throwable {
    AsyncTable<?> table = getTable.get();

    // Put with specifying the timestamp
    table.put(new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), 100, Bytes.toBytes("a")))
      .get();

    // Put with success
    boolean ok = table.checkAndMutate(row, new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
        CompareOperator.EQUAL, Bytes.toBytes("a")))
      .timeRange(TimeRange.between(0, 101))
      .thenPut(new Put(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")))
      .get();
    assertTrue(ok);

    Result result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // Put with failure
    ok = table.checkAndMutate(row, new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
        CompareOperator.EQUAL, Bytes.toBytes("a")))
      .timeRange(TimeRange.between(0, 100))
      .thenPut(new Put(row).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")))
      .get();
    assertFalse(ok);

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("C"))).get());
  }

  @Test(expected = NullPointerException.class)
  @Deprecated
  public void testCheckAndMutateWithoutConditionForOldApi() {
    getTable.get().checkAndMutate(row, FAMILY)
      .thenPut(new Put(row).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")));
  }

  // Tests for new CheckAndMutate API

  @SuppressWarnings("FutureReturnValueIgnored")
  @Test
  public void testCheckAndPut() throws InterruptedException, ExecutionException {
    AsyncTable<?> table = getTable.get();
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger successIndex = new AtomicInteger(-1);
    int count = 10;
    CountDownLatch latch = new CountDownLatch(count);

    IntStream.range(0, count)
      .forEach(i -> table.checkAndMutate(CheckAndMutate.newBuilder(row)
          .ifNotExists(FAMILY, QUALIFIER)
          .build(new Put(row).addColumn(FAMILY, QUALIFIER, concat(VALUE, i))))
        .thenAccept(x -> {
          if (x.isSuccess()) {
            successCount.incrementAndGet();
            successIndex.set(i);
          }
          assertNull(x.getResult());
          latch.countDown();
        }));
    latch.await();
    assertEquals(1, successCount.get());
    String actual = Bytes.toString(table.get(new Get(row)).get().getValue(FAMILY, QUALIFIER));
    assertTrue(actual.endsWith(Integer.toString(successIndex.get())));
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Test
  public void testCheckAndDelete() throws InterruptedException, ExecutionException {
    AsyncTable<?> table = getTable.get();
    int count = 10;
    CountDownLatch putLatch = new CountDownLatch(count + 1);
    table.put(new Put(row).addColumn(FAMILY, QUALIFIER, VALUE)).thenRun(() -> putLatch.countDown());
    IntStream.range(0, count)
      .forEach(i -> table.put(new Put(row).addColumn(FAMILY, concat(QUALIFIER, i), VALUE))
        .thenRun(() -> putLatch.countDown()));
    putLatch.await();

    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger successIndex = new AtomicInteger(-1);
    CountDownLatch deleteLatch = new CountDownLatch(count);

    IntStream.range(0, count)
      .forEach(i -> table.checkAndMutate(CheckAndMutate.newBuilder(row)
          .ifEquals(FAMILY, QUALIFIER, VALUE)
          .build(
            new Delete(row).addColumn(FAMILY, QUALIFIER).addColumn(FAMILY, concat(QUALIFIER, i))))
        .thenAccept(x -> {
          if (x.isSuccess()) {
            successCount.incrementAndGet();
            successIndex.set(i);
          }
          assertNull(x.getResult());
          deleteLatch.countDown();
        }));
    deleteLatch.await();
    assertEquals(1, successCount.get());
    Result result = table.get(new Get(row)).get();
    IntStream.range(0, count).forEach(i -> {
      if (i == successIndex.get()) {
        assertFalse(result.containsColumn(FAMILY, concat(QUALIFIER, i)));
      } else {
        assertArrayEquals(VALUE, result.getValue(FAMILY, concat(QUALIFIER, i)));
      }
    });
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Test
  public void testCheckAndMutate() throws InterruptedException, ExecutionException {
    AsyncTable<?> table = getTable.get();
    int count = 10;
    CountDownLatch putLatch = new CountDownLatch(count + 1);
    table.put(new Put(row).addColumn(FAMILY, QUALIFIER, VALUE)).thenRun(() -> putLatch.countDown());
    IntStream.range(0, count)
      .forEach(i -> table.put(new Put(row).addColumn(FAMILY, concat(QUALIFIER, i), VALUE))
        .thenRun(() -> putLatch.countDown()));
    putLatch.await();

    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger successIndex = new AtomicInteger(-1);
    CountDownLatch mutateLatch = new CountDownLatch(count);
    IntStream.range(0, count).forEach(i -> {
      RowMutations mutation = new RowMutations(row);
      try {
        mutation.add((Mutation) new Delete(row).addColumn(FAMILY, QUALIFIER));
        mutation
          .add((Mutation) new Put(row).addColumn(FAMILY, concat(QUALIFIER, i), concat(VALUE, i)));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      table.checkAndMutate(CheckAndMutate.newBuilder(row)
          .ifEquals(FAMILY, QUALIFIER, VALUE)
          .build(mutation))
        .thenAccept(x -> {
          if (x.isSuccess()) {
            successCount.incrementAndGet();
            successIndex.set(i);
          }
          assertNull(x.getResult());
          mutateLatch.countDown();
        });
    });
    mutateLatch.await();
    assertEquals(1, successCount.get());
    Result result = table.get(new Get(row)).get();
    IntStream.range(0, count).forEach(i -> {
      if (i == successIndex.get()) {
        assertArrayEquals(concat(VALUE, i), result.getValue(FAMILY, concat(QUALIFIER, i)));
      } else {
        assertArrayEquals(VALUE, result.getValue(FAMILY, concat(QUALIFIER, i)));
      }
    });
  }

  @Test
  public void testCheckAndMutateWithTimeRange() throws Exception {
    AsyncTable<?> table = getTable.get();
    final long ts = System.currentTimeMillis() / 2;
    Put put = new Put(row);
    put.addColumn(FAMILY, QUALIFIER, ts, VALUE);

    CheckAndMutateResult result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifNotExists(FAMILY, QUALIFIER)
      .build(put)).get();
    assertTrue(result.isSuccess());
    assertNull(result.getResult());

    result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, QUALIFIER, VALUE)
      .timeRange(TimeRange.at(ts + 10000))
      .build(put)).get();
    assertFalse(result.isSuccess());
    assertNull(result.getResult());

    result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, QUALIFIER, VALUE)
      .timeRange(TimeRange.at(ts))
      .build(put)).get();
    assertTrue(result.isSuccess());
    assertNull(result.getResult());

    RowMutations rm = new RowMutations(row).add((Mutation) put);

    result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, QUALIFIER, VALUE)
      .timeRange(TimeRange.at(ts + 10000))
      .build(rm)).get();
    assertFalse(result.isSuccess());
    assertNull(result.getResult());

    result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, QUALIFIER, VALUE)
      .timeRange(TimeRange.at(ts))
      .build(rm)).get();
    assertTrue(result.isSuccess());
    assertNull(result.getResult());

    Delete delete = new Delete(row).addColumn(FAMILY, QUALIFIER);

    result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, QUALIFIER, VALUE)
      .timeRange(TimeRange.at(ts + 10000))
      .build(delete)).get();
    assertFalse(result.isSuccess());
    assertNull(result.getResult());

    result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, QUALIFIER, VALUE)
      .timeRange(TimeRange.at(ts))
      .build(delete)).get();
    assertTrue(result.isSuccess());
    assertNull(result.getResult());
  }

  @Test
  public void testCheckAndMutateWithSingleFilter() throws Throwable {
    AsyncTable<?> table = getTable.get();

    // Put one row
    Put put = new Put(row);
    put.addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"));
    put.addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"));
    put.addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"));
    table.put(put).get();

    // Put with success
    CheckAndMutateResult result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
        CompareOperator.EQUAL, Bytes.toBytes("a")))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))).get();
    assertTrue(result.isSuccess());
    assertNull(result.getResult());

    Result r = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("D"))).get();
    assertEquals("d", Bytes.toString(r.getValue(FAMILY, Bytes.toBytes("D"))));

    // Put with failure
    result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
        CompareOperator.EQUAL, Bytes.toBytes("b")))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e")))).get();
    assertFalse(result.isSuccess());
    assertNull(result.getResult());

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("E"))).get());

    // Delete with success
    result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
        CompareOperator.EQUAL, Bytes.toBytes("a")))
      .build(new Delete(row).addColumns(FAMILY, Bytes.toBytes("D")))).get();
    assertTrue(result.isSuccess());
    assertNull(result.getResult());

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("D"))).get());

    // Mutate with success
    result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"),
        CompareOperator.EQUAL, Bytes.toBytes("b")))
      .build(new RowMutations(row)
        .add((Mutation) new Put(row)
          .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))
        .add((Mutation) new Delete(row).addColumns(FAMILY, Bytes.toBytes("A"))))).get();
    assertTrue(result.isSuccess());
    assertNull(result.getResult());

    r = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("D"))).get();
    assertEquals("d", Bytes.toString(r.getValue(FAMILY, Bytes.toBytes("D"))));

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("A"))).get());
  }

  @Test
  public void testCheckAndMutateWithMultipleFilters() throws Throwable {
    AsyncTable<?> table = getTable.get();

    // Put one row
    Put put = new Put(row);
    put.addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"));
    put.addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"));
    put.addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"));
    table.put(put).get();

    // Put with success
    CheckAndMutateResult result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))).get();
    assertTrue(result.isSuccess());
    assertNull(result.getResult());

    Result r = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("D"))).get();
    assertEquals("d", Bytes.toString(r.getValue(FAMILY, Bytes.toBytes("D"))));

    // Put with failure
    result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("c"))))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e")))).get();
    assertFalse(result.isSuccess());
    assertNull(result.getResult());

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("E"))).get());

    // Delete with success
    result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))))
      .build(new Delete(row).addColumns(FAMILY, Bytes.toBytes("D")))).get();
    assertTrue(result.isSuccess());
    assertNull(result.getResult());

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("D"))).get());

    // Mutate with success
    result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))))
      .build(new RowMutations(row)
        .add((Mutation) new Put(row)
          .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))
        .add((Mutation) new Delete(row).addColumns(FAMILY, Bytes.toBytes("A"))))).get();
    assertTrue(result.isSuccess());
    assertNull(result.getResult());

    r = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("D"))).get();
    assertEquals("d", Bytes.toString(r.getValue(FAMILY, Bytes.toBytes("D"))));

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("A"))).get());
  }

  @Test
  public void testCheckAndMutateWithTimestampFilter() throws Throwable {
    AsyncTable<?> table = getTable.get();

    // Put with specifying the timestamp
    table.put(new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), 100, Bytes.toBytes("a"))).get();

    // Put with success
    CheckAndMutateResult result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(FAMILY)),
        new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("A"))),
        new TimestampsFilter(Collections.singletonList(100L))))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")))).get();
    assertTrue(result.isSuccess());
    assertNull(result.getResult());

    Result r = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals("b", Bytes.toString(r.getValue(FAMILY, Bytes.toBytes("B"))));

    // Put with failure
    result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(FAMILY)),
        new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("A"))),
        new TimestampsFilter(Collections.singletonList(101L))))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")))).get();
    assertFalse(result.isSuccess());
    assertNull(result.getResult());

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("C"))).get());
  }

  @Test
  public void testCheckAndMutateWithFilterAndTimeRange() throws Throwable {
    AsyncTable<?> table = getTable.get();

    // Put with specifying the timestamp
    table.put(new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), 100, Bytes.toBytes("a")))
      .get();

    // Put with success
    CheckAndMutateResult result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
        CompareOperator.EQUAL, Bytes.toBytes("a")))
      .timeRange(TimeRange.between(0, 101))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")))).get();
    assertTrue(result.isSuccess());
    assertNull(result.getResult());

    Result r = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals("b", Bytes.toString(r.getValue(FAMILY, Bytes.toBytes("B"))));

    // Put with failure
    result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"),
        CompareOperator.EQUAL, Bytes.toBytes("a")))
      .timeRange(TimeRange.between(0, 100))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"))))
      .get();
    assertFalse(result.isSuccess());
    assertNull(result.getResult());

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("C"))).get());
  }

  @Test
  public void testCheckAndIncrement() throws Throwable {
    AsyncTable<?> table = getTable.get();

    table.put(new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))).get();

    // CheckAndIncrement with correct value
    CheckAndMutateResult res = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
      .build(new Increment(row).addColumn(FAMILY, Bytes.toBytes("B"), 1))).get();
    assertTrue(res.isSuccess());
    assertEquals(1, Bytes.toLong(res.getResult().getValue(FAMILY, Bytes.toBytes("B"))));

    Result result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals(1, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // CheckAndIncrement with wrong value
    res = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("b"))
      .build(new Increment(row).addColumn(FAMILY, Bytes.toBytes("B"), 1))).get();
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals(1, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("B"))));

    table.put(new Put(row).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")));

    // CheckAndIncrement with a filter and correct value
    res = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("C"), CompareOperator.EQUAL,
          Bytes.toBytes("c"))))
      .build(new Increment(row).addColumn(FAMILY, Bytes.toBytes("B"), 2))).get();
    assertTrue(res.isSuccess());
    assertEquals(3, Bytes.toLong(res.getResult().getValue(FAMILY, Bytes.toBytes("B"))));

    result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals(3, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // CheckAndIncrement with a filter and correct value
    res = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("b")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("C"), CompareOperator.EQUAL,
          Bytes.toBytes("d"))))
      .build(new Increment(row).addColumn(FAMILY, Bytes.toBytes("B"), 2))).get();
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals(3, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("B"))));
  }

  @Test
  public void testCheckAndAppend() throws Throwable {
    AsyncTable<?> table = getTable.get();

    table.put(new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))).get();

    // CheckAndAppend with correct value
    CheckAndMutateResult res = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
      .build(new Append(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")))).get();
    assertTrue(res.isSuccess());
    assertEquals("b", Bytes.toString(res.getResult().getValue(FAMILY, Bytes.toBytes("B"))));

    Result result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // CheckAndAppend with correct value
    res = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("b"))
      .build(new Append(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")))).get();
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    table.put(new Put(row).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")));

    // CheckAndAppend with a filter and correct value
    res = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("C"), CompareOperator.EQUAL,
          Bytes.toBytes("c"))))
      .build(new Append(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("bb")))).get();
    assertTrue(res.isSuccess());
    assertEquals("bbb", Bytes.toString(res.getResult().getValue(FAMILY, Bytes.toBytes("B"))));

    result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals("bbb", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // CheckAndAppend with a filter and wrong value
    res = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("b")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("C"), CompareOperator.EQUAL,
          Bytes.toBytes("d"))))
      .build(new Append(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("bb")))).get();
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals("bbb", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));
  }

  @Test
  public void testCheckAndRowMutations() throws Throwable {
    final byte[] q1 = Bytes.toBytes("q1");
    final byte[] q2 = Bytes.toBytes("q2");
    final byte[] q3 = Bytes.toBytes("q3");
    final byte[] q4 = Bytes.toBytes("q4");
    final String v1 = "v1";

    AsyncTable<?> table = getTable.get();

    // Initial values
    table.putAll(Arrays.asList(
      new Put(row).addColumn(FAMILY, q2, Bytes.toBytes("toBeDeleted")),
      new Put(row).addColumn(FAMILY, q3, Bytes.toBytes(5L)),
      new Put(row).addColumn(FAMILY, q4, Bytes.toBytes("a")))).get();

    // Do CheckAndRowMutations
    CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(row)
      .ifNotExists(FAMILY, q1)
      .build(new RowMutations(row).add(Arrays.asList(
        new Put(row).addColumn(FAMILY, q1, Bytes.toBytes(v1)),
        new Delete(row).addColumns(FAMILY, q2),
        new Increment(row).addColumn(FAMILY, q3, 1),
        new Append(row).addColumn(FAMILY, q4, Bytes.toBytes("b"))))
      );

    CheckAndMutateResult result = table.checkAndMutate(checkAndMutate).get();
    assertTrue(result.isSuccess());
    assertEquals(6L, Bytes.toLong(result.getResult().getValue(FAMILY, q3)));
    assertEquals("ab", Bytes.toString(result.getResult().getValue(FAMILY, q4)));

    // Verify the value
    Result r = table.get(new Get(row)).get();
    assertEquals(v1, Bytes.toString(r.getValue(FAMILY, q1)));
    assertNull(r.getValue(FAMILY, q2));
    assertEquals(6L, Bytes.toLong(r.getValue(FAMILY, q3)));
    assertEquals("ab", Bytes.toString(r.getValue(FAMILY, q4)));

    // Do CheckAndRowMutations again
    checkAndMutate = CheckAndMutate.newBuilder(row)
      .ifNotExists(FAMILY, q1)
      .build(new RowMutations(row).add(Arrays.asList(
        new Delete(row).addColumns(FAMILY, q1),
        new Put(row).addColumn(FAMILY, q2, Bytes.toBytes(v1)),
        new Increment(row).addColumn(FAMILY, q3, 1),
        new Append(row).addColumn(FAMILY, q4, Bytes.toBytes("b"))))
      );

    result = table.checkAndMutate(checkAndMutate).get();
    assertFalse(result.isSuccess());
    assertNull(result.getResult());

    // Verify the value
    r = table.get(new Get(row)).get();
    assertEquals(v1, Bytes.toString(r.getValue(FAMILY, q1)));
    assertNull(r.getValue(FAMILY, q2));
    assertEquals(6L, Bytes.toLong(r.getValue(FAMILY, q3)));
    assertEquals("ab", Bytes.toString(r.getValue(FAMILY, q4)));
  }

  // Tests for batch version of checkAndMutate

  @Test
  public void testCheckAndMutateBatch() throws Throwable {
    AsyncTable<?> table = getTable.get();
    byte[] row2 = Bytes.toBytes(Bytes.toString(row) + "2");
    byte[] row3 = Bytes.toBytes(Bytes.toString(row) + "3");
    byte[] row4 = Bytes.toBytes(Bytes.toString(row) + "4");

    table.putAll(Arrays.asList(
      new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a")),
      new Put(row2).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")),
      new Put(row3).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")),
      new Put(row4).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))).get();

    // Test for Put
    CheckAndMutate checkAndMutate1 = CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("e")));

    CheckAndMutate checkAndMutate2 = CheckAndMutate.newBuilder(row2)
      .ifEquals(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("a"))
      .build(new Put(row2).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("f")));

    List<CheckAndMutateResult> results =
      table.checkAndMutateAll(Arrays.asList(checkAndMutate1, checkAndMutate2)).get();

    assertTrue(results.get(0).isSuccess());
    assertNull(results.get(0).getResult());
    assertFalse(results.get(1).isSuccess());
    assertNull(results.get(1).getResult());

    Result result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("A"))).get();
    assertEquals("e", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("A"))));

    result = table.get(new Get(row2).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // Test for Delete
    checkAndMutate1 = CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("e"))
      .build(new Delete(row));

    checkAndMutate2 = CheckAndMutate.newBuilder(row2)
      .ifEquals(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("a"))
      .build(new Delete(row2));

    results = table.checkAndMutateAll(Arrays.asList(checkAndMutate1, checkAndMutate2)).get();

    assertTrue(results.get(0).isSuccess());
    assertNull(results.get(0).getResult());
    assertFalse(results.get(1).isSuccess());
    assertNull(results.get(1).getResult());

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("A"))).get());

    result = table.get(new Get(row2).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // Test for RowMutations
    checkAndMutate1 = CheckAndMutate.newBuilder(row3)
      .ifEquals(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"))
      .build(new RowMutations(row3)
        .add((Mutation) new Put(row3)
          .addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("f")))
        .add((Mutation) new Delete(row3).addColumns(FAMILY, Bytes.toBytes("C"))));

    checkAndMutate2 = CheckAndMutate.newBuilder(row4)
      .ifEquals(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("f"))
      .build(new RowMutations(row4)
        .add((Mutation) new Put(row4)
          .addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("f")))
        .add((Mutation) new Delete(row4).addColumns(FAMILY, Bytes.toBytes("D"))));

    results = table.checkAndMutateAll(Arrays.asList(checkAndMutate1, checkAndMutate2)).get();

    assertTrue(results.get(0).isSuccess());
    assertNull(results.get(0).getResult());
    assertFalse(results.get(1).isSuccess());
    assertNull(results.get(1).getResult());

    result = table.get(new Get(row3)).get();
    assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("F"))));
    assertNull(result.getValue(FAMILY, Bytes.toBytes("D")));

    result = table.get(new Get(row4)).get();
    assertNull(result.getValue(FAMILY, Bytes.toBytes("F")));
    assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));
  }

  @Test
  public void testCheckAndMutateBatch2() throws Throwable {
    AsyncTable<?> table = getTable.get();
    byte[] row2 = Bytes.toBytes(Bytes.toString(row) + "2");
    byte[] row3 = Bytes.toBytes(Bytes.toString(row) + "3");
    byte[] row4 = Bytes.toBytes(Bytes.toString(row) + "4");

    table.putAll(Arrays.asList(
      new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a")),
      new Put(row2).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")),
      new Put(row3).addColumn(FAMILY, Bytes.toBytes("C"), 100, Bytes.toBytes("c")),
      new Put(row4).addColumn(FAMILY, Bytes.toBytes("D"), 100, Bytes.toBytes("d")))).get();

    // Test for ifNotExists()
    CheckAndMutate checkAndMutate1 = CheckAndMutate.newBuilder(row)
      .ifNotExists(FAMILY, Bytes.toBytes("B"))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("e")));

    CheckAndMutate checkAndMutate2 = CheckAndMutate.newBuilder(row2)
      .ifNotExists(FAMILY, Bytes.toBytes("B"))
      .build(new Put(row2).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("f")));

    List<CheckAndMutateResult> results =
      table.checkAndMutateAll(Arrays.asList(checkAndMutate1, checkAndMutate2)).get();

    assertTrue(results.get(0).isSuccess());
    assertNull(results.get(0).getResult());
    assertFalse(results.get(1).isSuccess());
    assertNull(results.get(1).getResult());

    Result result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("A"))).get();
    assertEquals("e", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("A"))));

    result = table.get(new Get(row2).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // Test for ifMatches()
    checkAndMutate1 = CheckAndMutate.newBuilder(row)
      .ifMatches(FAMILY, Bytes.toBytes("A"), CompareOperator.NOT_EQUAL, Bytes.toBytes("a"))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a")));

    checkAndMutate2 = CheckAndMutate.newBuilder(row2)
      .ifMatches(FAMILY, Bytes.toBytes("B"), CompareOperator.GREATER, Bytes.toBytes("b"))
      .build(new Put(row2).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("f")));

    results = table.checkAndMutateAll(Arrays.asList(checkAndMutate1, checkAndMutate2)).get();

    assertTrue(results.get(0).isSuccess());
    assertNull(results.get(0).getResult());
    assertFalse(results.get(1).isSuccess());
    assertNull(results.get(1).getResult());

    result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("A"))).get();
    assertEquals("a", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("A"))));

    result = table.get(new Get(row2).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // Test for timeRange()
    checkAndMutate1 = CheckAndMutate.newBuilder(row3)
      .ifEquals(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"))
      .timeRange(TimeRange.between(0, 101))
      .build(new Put(row3).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("e")));

    checkAndMutate2 = CheckAndMutate.newBuilder(row4)
      .ifEquals(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d"))
      .timeRange(TimeRange.between(0, 100))
      .build(new Put(row4).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("f")));

    results = table.checkAndMutateAll(Arrays.asList(checkAndMutate1, checkAndMutate2)).get();

    assertTrue(results.get(0).isSuccess());
    assertNull(results.get(0).getResult());
    assertFalse(results.get(1).isSuccess());
    assertNull(results.get(1).getResult());

    result = table.get(new Get(row3).addColumn(FAMILY, Bytes.toBytes("C"))).get();
    assertEquals("e", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))));

    result = table.get(new Get(row4).addColumn(FAMILY, Bytes.toBytes("D"))).get();
    assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));
  }

  @Test
  public void testCheckAndMutateBatchWithFilter() throws Throwable {
    AsyncTable<?> table = getTable.get();
    byte[] row2 = Bytes.toBytes(Bytes.toString(row) + "2");

    table.putAll(Arrays.asList(
      new Put(row)
        .addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
        .addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"))
        .addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")),
      new Put(row2)
        .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d"))
        .addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e"))
        .addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("f")))).get();

    // Test for Put
    CheckAndMutate checkAndMutate1 = CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("g")));

    CheckAndMutate checkAndMutate2 = CheckAndMutate.newBuilder(row2)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("D"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("E"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))))
      .build(new Put(row2).addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("h")));

    List<CheckAndMutateResult> results =
      table.checkAndMutateAll(Arrays.asList(checkAndMutate1, checkAndMutate2)).get();

    assertTrue(results.get(0).isSuccess());
    assertNull(results.get(0).getResult());
    assertFalse(results.get(1).isSuccess());
    assertNull(results.get(1).getResult());

    Result result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("C"))).get();
    assertEquals("g", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))));

    result = table.get(new Get(row2).addColumn(FAMILY, Bytes.toBytes("F"))).get();
    assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("F"))));

    // Test for Delete
    checkAndMutate1 = CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))))
      .build(new Delete(row).addColumns(FAMILY, Bytes.toBytes("C")));

    checkAndMutate2 = CheckAndMutate.newBuilder(row2)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("D"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("E"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))))
      .build(new Delete(row2).addColumn(FAMILY, Bytes.toBytes("F")));

    results = table.checkAndMutateAll(Arrays.asList(checkAndMutate1, checkAndMutate2)).get();

    assertTrue(results.get(0).isSuccess());
    assertNull(results.get(0).getResult());
    assertFalse(results.get(1).isSuccess());
    assertNull(results.get(1).getResult());

    assertFalse(table.exists(new Get(row).addColumn(FAMILY, Bytes.toBytes("C"))).get());

    result = table.get(new Get(row2).addColumn(FAMILY, Bytes.toBytes("F"))).get();
    assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("F"))));

    // Test for RowMutations
    checkAndMutate1 = CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))))
      .build(new RowMutations(row)
        .add((Mutation) new Put(row)
          .addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")))
        .add((Mutation) new Delete(row).addColumns(FAMILY, Bytes.toBytes("A"))));

    checkAndMutate2 = CheckAndMutate.newBuilder(row2)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("D"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("E"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))))
      .build(new RowMutations(row2)
        .add((Mutation) new Put(row2)
          .addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("g")))
        .add((Mutation) new Delete(row2).addColumns(FAMILY, Bytes.toBytes("D"))));

    results = table.checkAndMutateAll(Arrays.asList(checkAndMutate1, checkAndMutate2)).get();

    assertTrue(results.get(0).isSuccess());
    assertNull(results.get(0).getResult());
    assertFalse(results.get(1).isSuccess());
    assertNull(results.get(1).getResult());

    result = table.get(new Get(row)).get();
    assertNull(result.getValue(FAMILY, Bytes.toBytes("A")));
    assertEquals("c", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))));

    result = table.get(new Get(row2)).get();
    assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));
    assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("F"))));
  }

  @Test
  public void testCheckAndMutateBatchWithFilterAndTimeRange() throws Throwable {
    AsyncTable<?> table = getTable.get();
    byte[] row2 = Bytes.toBytes(Bytes.toString(row) + "2");

    table.putAll(Arrays.asList(
      new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), 100, Bytes.toBytes("a"))
        .addColumn(FAMILY, Bytes.toBytes("B"), 100, Bytes.toBytes("b"))
        .addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")),
      new Put(row2).addColumn(FAMILY, Bytes.toBytes("D"), 100, Bytes.toBytes("d"))
        .addColumn(FAMILY, Bytes.toBytes("E"), 100, Bytes.toBytes("e"))
        .addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("f")))).get();

    CheckAndMutate checkAndMutate1 = CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))))
      .timeRange(TimeRange.between(0, 101))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("g")));

    CheckAndMutate checkAndMutate2 = CheckAndMutate.newBuilder(row2)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("D"), CompareOperator.EQUAL,
          Bytes.toBytes("d")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("E"), CompareOperator.EQUAL,
          Bytes.toBytes("e"))))
      .timeRange(TimeRange.between(0, 100))
      .build(new Put(row2).addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("h")));

    List<CheckAndMutateResult> results =
      table.checkAndMutateAll(Arrays.asList(checkAndMutate1, checkAndMutate2)).get();

    assertTrue(results.get(0).isSuccess());
    assertNull(results.get(0).getResult());
    assertFalse(results.get(1).isSuccess());
    assertNull(results.get(1).getResult());

    Result result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("C"))).get();
    assertEquals("g", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))));

    result = table.get(new Get(row2).addColumn(FAMILY, Bytes.toBytes("F"))).get();
    assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("F"))));
  }

  @Test
  public void testCheckAndIncrementBatch() throws Throwable {
    AsyncTable<?> table = getTable.get();
    byte[] row2 = Bytes.toBytes(Bytes.toString(row) + "2");

    table.putAll(Arrays.asList(
      new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
        .addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes(0L)),
      new Put(row2).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"))
        .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes(0L)))).get();

    // CheckAndIncrement with correct value
    CheckAndMutate checkAndMutate1 = CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
      .build(new Increment(row).addColumn(FAMILY, Bytes.toBytes("B"), 1));

    // CheckAndIncrement with wrong value
    CheckAndMutate checkAndMutate2 = CheckAndMutate.newBuilder(row2)
      .ifEquals(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("d"))
      .build(new Increment(row2).addColumn(FAMILY, Bytes.toBytes("D"), 1));

    List<CheckAndMutateResult> results =
      table.checkAndMutateAll(Arrays.asList(checkAndMutate1, checkAndMutate2)).get();

    assertTrue(results.get(0).isSuccess());
    assertEquals(1, Bytes.toLong(results.get(0).getResult()
      .getValue(FAMILY, Bytes.toBytes("B"))));
    assertFalse(results.get(1).isSuccess());
    assertNull(results.get(1).getResult());

    Result result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals(1, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("B"))));

    result = table.get(new Get(row2).addColumn(FAMILY, Bytes.toBytes("D"))).get();
    assertEquals(0, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("D"))));
  }

  @Test
  public void testCheckAndAppendBatch() throws Throwable {
    AsyncTable<?> table = getTable.get();
    byte[] row2 = Bytes.toBytes(Bytes.toString(row) + "2");

    table.putAll(Arrays.asList(
      new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
        .addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")),
      new Put(row2).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"))
        .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))).get();

    // CheckAndAppend with correct value
    CheckAndMutate checkAndMutate1 = CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
      .build(new Append(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")));

    // CheckAndAppend with wrong value
    CheckAndMutate checkAndMutate2 = CheckAndMutate.newBuilder(row2)
      .ifEquals(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("d"))
      .build(new Append(row2).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")));

    List<CheckAndMutateResult> results =
      table.checkAndMutateAll(Arrays.asList(checkAndMutate1, checkAndMutate2)).get();

    assertTrue(results.get(0).isSuccess());
    assertEquals("bb", Bytes.toString(results.get(0).getResult()
      .getValue(FAMILY, Bytes.toBytes("B"))));
    assertFalse(results.get(1).isSuccess());
    assertNull(results.get(1).getResult());

    Result result = table.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B"))).get();
    assertEquals("bb", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    result = table.get(new Get(row2).addColumn(FAMILY, Bytes.toBytes("D"))).get();
    assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));
  }

  @Test
  public void testCheckAndRowMutationsBatch() throws Throwable {
    AsyncTable<?> table = getTable.get();
    byte[] row2 = Bytes.toBytes(Bytes.toString(row) + "2");

    table.putAll(Arrays.asList(
      new Put(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"))
        .addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes(1L))
        .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")),
      new Put(row2).addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("f"))
        .addColumn(FAMILY, Bytes.toBytes("G"), Bytes.toBytes(1L))
        .addColumn(FAMILY, Bytes.toBytes("H"), Bytes.toBytes("h")))
    ).get();

    // CheckAndIncrement with correct value
    CheckAndMutate checkAndMutate1 = CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"))
      .build(new RowMutations(row).add(Arrays.asList(
        new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a")),
        new Delete(row).addColumns(FAMILY, Bytes.toBytes("B")),
        new Increment(row).addColumn(FAMILY, Bytes.toBytes("C"), 1L),
        new Append(row).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d"))
      )));

    // CheckAndIncrement with wrong value
    CheckAndMutate checkAndMutate2 = CheckAndMutate.newBuilder(row2)
      .ifEquals(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("a"))
      .build(new RowMutations(row2).add(Arrays.asList(
        new Put(row2).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e")),
        new Delete(row2).addColumns(FAMILY, Bytes.toBytes("F")),
        new Increment(row2).addColumn(FAMILY, Bytes.toBytes("G"), 1L),
        new Append(row2).addColumn(FAMILY, Bytes.toBytes("H"), Bytes.toBytes("h"))
      )));

    List<CheckAndMutateResult> results =
      table.checkAndMutateAll(Arrays.asList(checkAndMutate1, checkAndMutate2)).get();

    assertTrue(results.get(0).isSuccess());
    assertEquals(2, Bytes.toLong(results.get(0).getResult()
      .getValue(FAMILY, Bytes.toBytes("C"))));
    assertEquals("dd", Bytes.toString(results.get(0).getResult()
      .getValue(FAMILY, Bytes.toBytes("D"))));

    assertFalse(results.get(1).isSuccess());
    assertNull(results.get(1).getResult());

    Result result = table.get(new Get(row)).get();
    assertEquals("a", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("A"))));
    assertNull(result.getValue(FAMILY, Bytes.toBytes("B")));
    assertEquals(2, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("C"))));
    assertEquals("dd", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

    result = table.get(new Get(row2)).get();
    assertNull(result.getValue(FAMILY, Bytes.toBytes("E")));
    assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("F"))));
    assertEquals(1, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("G"))));
    assertEquals("h", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("H"))));
  }

  @Test
  public void testDisabled() throws InterruptedException, ExecutionException {
    ASYNC_CONN.getAdmin().disableTable(TABLE_NAME).get();
    try {
      getTable.get().get(new Get(row)).get();
      fail("Should fail since table has been disabled");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertThat(cause, instanceOf(TableNotEnabledException.class));
      assertThat(cause.getMessage(), containsString(TABLE_NAME.getNameAsString()));
    }
  }

  @Test
  public void testInvalidPut() {
    try {
      getTable.get().put(new Put(Bytes.toBytes(0)));
      fail("Should fail since the put does not contain any cells");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("No columns to insert"));
    }

    try {
      getTable.get()
        .put(new Put(Bytes.toBytes(0)).addColumn(FAMILY, QUALIFIER, new byte[MAX_KEY_VALUE_SIZE]));
      fail("Should fail since the put exceeds the max key value size");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("KeyValue size too large"));
    }
  }

  @Test
  public void testInvalidPutInRowMutations() throws IOException {
    final byte[] row = Bytes.toBytes(0);
    try {
      getTable.get().mutateRow(new RowMutations(row).add((Mutation) new Put(row)));
      fail("Should fail since the put does not contain any cells");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("No columns to insert"));
    }

    try {
      getTable.get()
        .mutateRow(new RowMutations(row).add((Mutation) new Put(row)
          .addColumn(FAMILY, QUALIFIER, new byte[MAX_KEY_VALUE_SIZE])));
      fail("Should fail since the put exceeds the max key value size");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("KeyValue size too large"));
    }
  }

  @Test
  public void testInvalidPutInRowMutationsInCheckAndMutate() throws IOException {
    final byte[] row = Bytes.toBytes(0);
    try {
      getTable.get().checkAndMutate(CheckAndMutate.newBuilder(row)
        .ifNotExists(FAMILY, QUALIFIER)
        .build(new RowMutations(row).add((Mutation) new Put(row))));
      fail("Should fail since the put does not contain any cells");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("No columns to insert"));
    }

    try {
      getTable.get().checkAndMutate(CheckAndMutate.newBuilder(row)
        .ifNotExists(FAMILY, QUALIFIER)
        .build(new RowMutations(row).add((Mutation) new Put(row)
          .addColumn(FAMILY, QUALIFIER, new byte[MAX_KEY_VALUE_SIZE]))));
      fail("Should fail since the put exceeds the max key value size");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("KeyValue size too large"));
    }
  }
}
