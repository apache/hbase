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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
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
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    IOUtils.closeQuietly(ASYNC_CONN);
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

  @Test
  public void testAppend() throws InterruptedException, ExecutionException {
    AsyncTable<?> table = getTable.get();
    int count = 10;
    CountDownLatch latch = new CountDownLatch(count);
    char suffix = ':';
    AtomicLong suffixCount = new AtomicLong(0L);
    IntStream.range(0, count).forEachOrdered(
      i -> table.append(new Append(row).addColumn(FAMILY, QUALIFIER, Bytes.toBytes("" + i + suffix)))
          .thenAccept(r -> {
            suffixCount.addAndGet(Bytes.toString(r.getValue(FAMILY, QUALIFIER)).chars()
                .filter(x -> x == suffix).count());
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
  public void testCheckAndPut() throws InterruptedException, ExecutionException {
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

  @Test
  public void testMutateRow() throws InterruptedException, ExecutionException, IOException {
    AsyncTable<?> table = getTable.get();
    RowMutations mutation = new RowMutations(row);
    mutation.add((Mutation) new Put(row).addColumn(FAMILY, concat(QUALIFIER, 1), VALUE));
    table.mutateRow(mutation).get();
    Result result = table.get(new Get(row)).get();
    assertArrayEquals(VALUE, result.getValue(FAMILY, concat(QUALIFIER, 1)));

    mutation = new RowMutations(row);
    mutation.add((Mutation) new Delete(row).addColumn(FAMILY, concat(QUALIFIER, 1)));
    mutation.add((Mutation) new Put(row).addColumn(FAMILY, concat(QUALIFIER, 2), VALUE));
    table.mutateRow(mutation).get();
    result = table.get(new Get(row)).get();
    assertNull(result.getValue(FAMILY, concat(QUALIFIER, 1)));
    assertArrayEquals(VALUE, result.getValue(FAMILY, concat(QUALIFIER, 2)));
  }

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
  public void testCheckAndMutateWithTimeRange() throws Exception {
    AsyncTable<?> table = getTable.get();
    final long ts = System.currentTimeMillis() / 2;
    Put put = new Put(row);
    put.addColumn(FAMILY, QUALIFIER, ts, VALUE);

    boolean ok = table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER)
      .ifNotExists()
      .thenPut(put)
      .get();
    assertTrue(ok);

    ok = table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER)
      .timeRange(TimeRange.at(ts + 10000))
      .ifEquals(VALUE)
      .thenPut(put)
      .get();
    assertFalse(ok);

    ok = table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER)
      .timeRange(TimeRange.at(ts))
      .ifEquals(VALUE)
      .thenPut(put)
      .get();
    assertTrue(ok);

    RowMutations rm = new RowMutations(row)
      .add((Mutation) put);
    ok = table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER)
      .timeRange(TimeRange.at(ts + 10000))
      .ifEquals(VALUE)
      .thenMutate(rm)
      .get();
    assertFalse(ok);

    ok = table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER)
      .timeRange(TimeRange.at(ts))
      .ifEquals(VALUE)
      .thenMutate(rm)
      .get();
    assertTrue(ok);

    Delete delete = new Delete(row)
      .addColumn(FAMILY, QUALIFIER);

    ok = table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER)
      .timeRange(TimeRange.at(ts + 10000))
      .ifEquals(VALUE)
      .thenDelete(delete)
      .get();
    assertFalse(ok);

    ok = table.checkAndMutate(row, FAMILY).qualifier(QUALIFIER)
      .timeRange(TimeRange.at(ts))
      .ifEquals(VALUE)
      .thenDelete(delete)
      .get();
    assertTrue(ok);
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
}
