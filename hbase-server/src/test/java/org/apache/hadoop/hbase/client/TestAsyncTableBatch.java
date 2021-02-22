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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncTableBatch {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncTableBatch.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");
  private static byte[] CQ1 = Bytes.toBytes("cq1");

  private static int COUNT = 1000;

  private static AsyncConnection CONN;

  private static byte[][] SPLIT_KEYS;

  private static int MAX_KEY_VALUE_SIZE = 64 * 1024;

  @Parameter(0)
  public String tableType;

  @Parameter(1)
  public Function<TableName, AsyncTable<?>> tableGetter;

  private static AsyncTable<?> getRawTable(TableName tableName) {
    return CONN.getTable(tableName);
  }

  private static AsyncTable<?> getTable(TableName tableName) {
    return CONN.getTable(tableName, ForkJoinPool.commonPool());
  }

  @Parameters(name = "{index}: type={0}")
  public static List<Object[]> params() {
    Function<TableName, AsyncTable<?>> rawTableGetter = TestAsyncTableBatch::getRawTable;
    Function<TableName, AsyncTable<?>> tableGetter = TestAsyncTableBatch::getTable;
    return Arrays.asList(new Object[] { "raw", rawTableGetter },
      new Object[] { "normal", tableGetter });
  }

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setInt(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY,
      MAX_KEY_VALUE_SIZE);
    TEST_UTIL.startMiniCluster(3);
    SPLIT_KEYS = new byte[8][];
    for (int i = 111; i < 999; i += 111) {
      SPLIT_KEYS[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
    }
    CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUpBeforeTest() throws IOException, InterruptedException {
    TEST_UTIL.createTable(TABLE_NAME, FAMILY, SPLIT_KEYS);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
  }

  @After
  public void tearDownAfterTest() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    if (admin.isTableEnabled(TABLE_NAME)) {
      admin.disableTable(TABLE_NAME);
    }
    admin.deleteTable(TABLE_NAME);
  }

  private byte[] getRow(int i) {
    return Bytes.toBytes(String.format("%03d", i));
  }

  @Test
  public void test()
      throws InterruptedException, ExecutionException, IOException, TimeoutException {
    AsyncTable<?> table = tableGetter.apply(TABLE_NAME);
    table.putAll(IntStream.range(0, COUNT)
        .mapToObj(i -> new Put(getRow(i)).addColumn(FAMILY, CQ, Bytes.toBytes(i)))
        .collect(Collectors.toList())).get();
    List<Result> results = table.getAll(IntStream.range(0, COUNT)
        .mapToObj(i -> Arrays.asList(new Get(getRow(i)), new Get(Arrays.copyOf(getRow(i), 4))))
        .flatMap(l -> l.stream()).collect(Collectors.toList())).get();
    assertEquals(2 * COUNT, results.size());
    for (int i = 0; i < COUNT; i++) {
      assertEquals(i, Bytes.toInt(results.get(2 * i).getValue(FAMILY, CQ)));
      assertTrue(results.get(2 * i + 1).isEmpty());
    }
    Admin admin = TEST_UTIL.getAdmin();
    admin.flush(TABLE_NAME);
    List<Future<?>> splitFutures =
      TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME).stream().map(r -> {
        byte[] startKey = r.getRegionInfo().getStartKey();
        int number = startKey.length == 0 ? 55 : Integer.parseInt(Bytes.toString(startKey));
        byte[] splitPoint = Bytes.toBytes(String.format("%03d", number + 55));
        try {
          return admin.splitRegionAsync(r.getRegionInfo().getRegionName(), splitPoint);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }).collect(Collectors.toList());
    for (Future<?> future : splitFutures) {
      future.get(30, TimeUnit.SECONDS);
    }
    table.deleteAll(
      IntStream.range(0, COUNT).mapToObj(i -> new Delete(getRow(i))).collect(Collectors.toList()))
        .get();
    results = table
        .getAll(
          IntStream.range(0, COUNT).mapToObj(i -> new Get(getRow(i))).collect(Collectors.toList()))
        .get();
    assertEquals(COUNT, results.size());
    results.forEach(r -> assertTrue(r.isEmpty()));
  }

  @Test
  public void testWithRegionServerFailover() throws Exception {
    AsyncTable<?> table = tableGetter.apply(TABLE_NAME);
    table.putAll(IntStream.range(0, COUNT)
        .mapToObj(i -> new Put(getRow(i)).addColumn(FAMILY, CQ, Bytes.toBytes(i)))
        .collect(Collectors.toList())).get();
    TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).abort("Aborting for tests");
    Thread.sleep(100);
    table.putAll(IntStream.range(COUNT, 2 * COUNT)
        .mapToObj(i -> new Put(getRow(i)).addColumn(FAMILY, CQ, Bytes.toBytes(i)))
        .collect(Collectors.toList())).get();
    List<Result> results = table.getAll(
      IntStream.range(0, 2 * COUNT).mapToObj(i -> new Get(getRow(i))).collect(Collectors.toList()))
        .get();
    assertEquals(2 * COUNT, results.size());
    results.forEach(r -> assertFalse(r.isEmpty()));
    table.deleteAll(IntStream.range(0, 2 * COUNT).mapToObj(i -> new Delete(getRow(i)))
        .collect(Collectors.toList())).get();
    results = table.getAll(
      IntStream.range(0, 2 * COUNT).mapToObj(i -> new Get(getRow(i))).collect(Collectors.toList()))
        .get();
    assertEquals(2 * COUNT, results.size());
    results.forEach(r -> assertTrue(r.isEmpty()));
  }

  @Test
  public void testMixed() throws InterruptedException, ExecutionException, IOException {
    AsyncTable<?> table = tableGetter.apply(TABLE_NAME);
    table.putAll(IntStream.range(0, 7)
        .mapToObj(i -> new Put(Bytes.toBytes(i)).addColumn(FAMILY, CQ, Bytes.toBytes((long) i)))
        .collect(Collectors.toList())).get();
    List<Row> actions = new ArrayList<>();
    actions.add(new Get(Bytes.toBytes(0)));
    actions.add(new Put(Bytes.toBytes(1)).addColumn(FAMILY, CQ, Bytes.toBytes(2L)));
    actions.add(new Delete(Bytes.toBytes(2)));
    actions.add(new Increment(Bytes.toBytes(3)).addColumn(FAMILY, CQ, 1));
    actions.add(new Append(Bytes.toBytes(4)).addColumn(FAMILY, CQ, Bytes.toBytes(4)));
    RowMutations rm = new RowMutations(Bytes.toBytes(5));
    rm.add((Mutation) new Put(Bytes.toBytes(5)).addColumn(FAMILY, CQ, Bytes.toBytes(100L)));
    rm.add((Mutation) new Put(Bytes.toBytes(5)).addColumn(FAMILY, CQ1, Bytes.toBytes(200L)));
    actions.add(rm);
    actions.add(new Get(Bytes.toBytes(6)));

    List<Object> results = table.batchAll(actions).get();
    assertEquals(7, results.size());
    Result getResult = (Result) results.get(0);
    assertEquals(0, Bytes.toLong(getResult.getValue(FAMILY, CQ)));
    assertEquals(2, Bytes.toLong(table.get(new Get(Bytes.toBytes(1))).get().getValue(FAMILY, CQ)));
    assertTrue(table.get(new Get(Bytes.toBytes(2))).get().isEmpty());
    Result incrementResult = (Result) results.get(3);
    assertEquals(4, Bytes.toLong(incrementResult.getValue(FAMILY, CQ)));
    Result appendResult = (Result) results.get(4);
    byte[] appendValue = appendResult.getValue(FAMILY, CQ);
    assertEquals(12, appendValue.length);
    assertEquals(4, Bytes.toLong(appendValue));
    assertEquals(4, Bytes.toInt(appendValue, 8));
    assertEquals(100,
      Bytes.toLong(table.get(new Get(Bytes.toBytes(5))).get().getValue(FAMILY, CQ)));
    assertEquals(200,
      Bytes.toLong(table.get(new Get(Bytes.toBytes(5))).get().getValue(FAMILY, CQ1)));
    getResult = (Result) results.get(6);
    assertEquals(6, Bytes.toLong(getResult.getValue(FAMILY, CQ)));
  }

  public static final class ErrorInjectObserver implements RegionCoprocessor, RegionObserver {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
        List<Cell> results) throws IOException {
      if (e.getEnvironment().getRegionInfo().getEndKey().length == 0) {
        throw new DoNotRetryRegionException("Inject Error");
      }
    }
  }

  @Test
  public void testPartialSuccess() throws IOException, InterruptedException, ExecutionException {
    Admin admin = TEST_UTIL.getAdmin();
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(admin.getDescriptor(TABLE_NAME))
        .setCoprocessor(ErrorInjectObserver.class.getName()).build();
    admin.modifyTable(htd);
    AsyncTable<?> table = tableGetter.apply(TABLE_NAME);
    table.putAll(Arrays.asList(SPLIT_KEYS).stream().map(k -> new Put(k).addColumn(FAMILY, CQ, k))
        .collect(Collectors.toList())).get();
    List<CompletableFuture<Result>> futures = table
        .get(Arrays.asList(SPLIT_KEYS).stream().map(k -> new Get(k)).collect(Collectors.toList()));
    for (int i = 0; i < SPLIT_KEYS.length - 1; i++) {
      assertArrayEquals(SPLIT_KEYS[i], futures.get(i).get().getValue(FAMILY, CQ));
    }
    try {
      futures.get(SPLIT_KEYS.length - 1).get();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(RetriesExhaustedException.class));
    }
  }

  @Test
  public void testPartialSuccessOnSameRegion() throws InterruptedException, ExecutionException {
    AsyncTable<?> table = tableGetter.apply(TABLE_NAME);
    List<CompletableFuture<Object>> futures = table.batch(Arrays.asList(
      new Put(Bytes.toBytes("put")).addColumn(Bytes.toBytes("not-exists"), CQ,
        Bytes.toBytes("bad")),
      new Increment(Bytes.toBytes("inc")).addColumn(FAMILY, CQ, 1),
      new Put(Bytes.toBytes("put")).addColumn(FAMILY, CQ, Bytes.toBytes("good"))));
    try {
      futures.get(0).get();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(RetriesExhaustedException.class));
      assertThat(e.getCause().getCause(), instanceOf(NoSuchColumnFamilyException.class));
    }
    assertEquals(1, Bytes.toLong(((Result) futures.get(1).get()).getValue(FAMILY, CQ)));
    assertTrue(((Result) futures.get(2).get()).isEmpty());
    assertEquals("good",
      Bytes.toString(table.get(new Get(Bytes.toBytes("put"))).get().getValue(FAMILY, CQ)));
  }

  @Test
  public void testInvalidPut() {
    AsyncTable<?> table = tableGetter.apply(TABLE_NAME);
    try {
      table.batch(Arrays.asList(new Delete(Bytes.toBytes(0)), new Put(Bytes.toBytes(0))));
      fail("Should fail since the put does not contain any cells");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("No columns to insert"));
    }

    try {
      table.batch(
        Arrays.asList(new Put(Bytes.toBytes(0)).addColumn(FAMILY, CQ, new byte[MAX_KEY_VALUE_SIZE]),
          new Delete(Bytes.toBytes(0))));
      fail("Should fail since the put exceeds the max key value size");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("KeyValue size too large"));
    }
  }

  @Test
  public void testInvalidPutInRowMutations() throws IOException {
    final byte[] row = Bytes.toBytes(0);

    AsyncTable<?> table = tableGetter.apply(TABLE_NAME);
    try {
      table.batch(Arrays.asList(new Delete(row), new RowMutations(row)
        .add((Mutation) new Put(row))));
      fail("Should fail since the put does not contain any cells");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("No columns to insert"));
    }

    try {
      table.batch(
        Arrays.asList(new RowMutations(row).add((Mutation) new Put(row)
            .addColumn(FAMILY, CQ, new byte[MAX_KEY_VALUE_SIZE])),
          new Delete(row)));
      fail("Should fail since the put exceeds the max key value size");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("KeyValue size too large"));
    }
  }

  @Test
  public void testInvalidPutInRowMutationsInCheckAndMutate() throws IOException {
    final byte[] row = Bytes.toBytes(0);

    AsyncTable<?> table = tableGetter.apply(TABLE_NAME);
    try {
      table.batch(Arrays.asList(new Delete(row), CheckAndMutate.newBuilder(row)
        .ifNotExists(FAMILY, CQ)
        .build(new RowMutations(row).add((Mutation) new Put(row)))));
      fail("Should fail since the put does not contain any cells");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("No columns to insert"));
    }

    try {
      table.batch(
        Arrays.asList(CheckAndMutate.newBuilder(row)
            .ifNotExists(FAMILY, CQ)
            .build(new RowMutations(row).add((Mutation) new Put(row)
              .addColumn(FAMILY, CQ, new byte[MAX_KEY_VALUE_SIZE]))),
          new Delete(row)));
      fail("Should fail since the put exceeds the max key value size");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("KeyValue size too large"));
    }
  }

  @Test
  public void testWithCheckAndMutate() throws Exception {
    AsyncTable<?> table = tableGetter.apply(TABLE_NAME);

    byte[] row1 = Bytes.toBytes("row1");
    byte[] row2 = Bytes.toBytes("row2");
    byte[] row3 = Bytes.toBytes("row3");
    byte[] row4 = Bytes.toBytes("row4");
    byte[] row5 = Bytes.toBytes("row5");
    byte[] row6 = Bytes.toBytes("row6");
    byte[] row7 = Bytes.toBytes("row7");

    table.putAll(Arrays.asList(
      new Put(row1).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a")),
      new Put(row2).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")),
      new Put(row3).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")),
      new Put(row4).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")),
      new Put(row5).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e")),
      new Put(row6).addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes(10L)),
      new Put(row7).addColumn(FAMILY, Bytes.toBytes("G"), Bytes.toBytes("g")))).get();

    CheckAndMutate checkAndMutate1 = CheckAndMutate.newBuilder(row1)
      .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
      .build(new RowMutations(row1)
        .add((Mutation) new Put(row1).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("g")))
        .add((Mutation) new Delete(row1).addColumns(FAMILY, Bytes.toBytes("A")))
        .add(new Increment(row1).addColumn(FAMILY, Bytes.toBytes("C"), 3L))
        .add(new Append(row1).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d"))));
    Get get = new Get(row2).addColumn(FAMILY, Bytes.toBytes("B"));
    RowMutations mutations = new RowMutations(row3)
      .add((Mutation) new Delete(row3).addColumns(FAMILY, Bytes.toBytes("C")))
      .add((Mutation) new Put(row3).addColumn(FAMILY, Bytes.toBytes("F"), Bytes.toBytes("f")))
      .add(new Increment(row3).addColumn(FAMILY, Bytes.toBytes("A"), 5L))
      .add(new Append(row3).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")));
    CheckAndMutate checkAndMutate2 = CheckAndMutate.newBuilder(row4)
      .ifEquals(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("a"))
      .build(new Put(row4).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("h")));
    Put put = new Put(row5).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("f"));
    CheckAndMutate checkAndMutate3 = CheckAndMutate.newBuilder(row6)
      .ifEquals(FAMILY, Bytes.toBytes("F"), Bytes.toBytes(10L))
      .build(new Increment(row6).addColumn(FAMILY, Bytes.toBytes("F"), 1));
    CheckAndMutate checkAndMutate4 = CheckAndMutate.newBuilder(row7)
      .ifEquals(FAMILY, Bytes.toBytes("G"), Bytes.toBytes("g"))
      .build(new Append(row7).addColumn(FAMILY, Bytes.toBytes("G"), Bytes.toBytes("g")));

    List<Row> actions = Arrays.asList(checkAndMutate1, get, mutations, checkAndMutate2, put,
      checkAndMutate3, checkAndMutate4);
    List<Object> results = table.batchAll(actions).get();

    CheckAndMutateResult checkAndMutateResult = (CheckAndMutateResult) results.get(0);
    assertTrue(checkAndMutateResult.isSuccess());
    assertEquals(3L,
      Bytes.toLong(checkAndMutateResult.getResult().getValue(FAMILY, Bytes.toBytes("C"))));
    assertEquals("d",
      Bytes.toString(checkAndMutateResult.getResult().getValue(FAMILY, Bytes.toBytes("D"))));

    assertEquals("b",
      Bytes.toString(((Result) results.get(1)).getValue(FAMILY, Bytes.toBytes("B"))));

    Result result = (Result) results.get(2);
    assertTrue(result.getExists());
    assertEquals(5L, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("A"))));
    assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    checkAndMutateResult = (CheckAndMutateResult) results.get(3);
    assertFalse(checkAndMutateResult.isSuccess());
    assertNull(checkAndMutateResult.getResult());

    assertTrue(((Result) results.get(4)).isEmpty());

    checkAndMutateResult = (CheckAndMutateResult) results.get(5);
    assertTrue(checkAndMutateResult.isSuccess());
    assertEquals(11, Bytes.toLong(checkAndMutateResult.getResult()
      .getValue(FAMILY, Bytes.toBytes("F"))));

    checkAndMutateResult = (CheckAndMutateResult) results.get(6);
    assertTrue(checkAndMutateResult.isSuccess());
    assertEquals("gg", Bytes.toString(checkAndMutateResult.getResult()
      .getValue(FAMILY, Bytes.toBytes("G"))));

    result = table.get(new Get(row1)).get();
    assertEquals("g", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));
    assertNull(result.getValue(FAMILY, Bytes.toBytes("A")));
    assertEquals(3L, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("C"))));
    assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

    result = table.get(new Get(row3)).get();
    assertNull(result.getValue(FAMILY, Bytes.toBytes("C")));
    assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("F"))));
    assertNull(Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))));
    assertEquals(5L, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("A"))));
    assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    result = table.get(new Get(row4)).get();
    assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

    result = table.get(new Get(row5)).get();
    assertEquals("f", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("E"))));

    result = table.get(new Get(row6)).get();
    assertEquals(11, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("F"))));

    result = table.get(new Get(row7)).get();
    assertEquals("gg", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("G"))));
  }
}
