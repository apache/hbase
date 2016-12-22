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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncTableBatch {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  private static int COUNT = 1000;

  private static AsyncConnection CONN;

  private static byte[][] SPLIT_KEYS;

  @Parameter(0)
  public String tableType;

  @Parameter(1)
  public Function<TableName, AsyncTableBase> tableGetter;

  private static RawAsyncTable getRawTable(TableName tableName) {
    return CONN.getRawTable(tableName);
  }

  private static AsyncTable getTable(TableName tableName) {
    return CONN.getTable(tableName, ForkJoinPool.commonPool());
  }

  @Parameters(name = "{index}: type={0}")
  public static List<Object[]> params() {
    Function<TableName, AsyncTableBase> rawTableGetter = TestAsyncTableBatch::getRawTable;
    Function<TableName, AsyncTableBase> tableGetter = TestAsyncTableBatch::getTable;
    return Arrays.asList(new Object[] { "raw", rawTableGetter },
      new Object[] { "normal", tableGetter });
  }

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    SPLIT_KEYS = new byte[8][];
    for (int i = 111; i < 999; i += 111) {
      SPLIT_KEYS[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
    }
    CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration());
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
  public void test() throws InterruptedException, ExecutionException, IOException {
    AsyncTableBase table = tableGetter.apply(TABLE_NAME);
    table.putAll(IntStream.range(0, COUNT)
        .mapToObj(i -> new Put(getRow(i)).addColumn(FAMILY, CQ, Bytes.toBytes(i)))
        .collect(Collectors.toList())).get();
    List<Result> results =
        table
            .getAll(IntStream.range(0, COUNT)
                .mapToObj(
                  i -> Arrays.asList(new Get(getRow(i)), new Get(Arrays.copyOf(getRow(i), 4))))
                .flatMap(l -> l.stream()).collect(Collectors.toList()))
            .get();
    assertEquals(2 * COUNT, results.size());
    for (int i = 0; i < COUNT; i++) {
      assertEquals(i, Bytes.toInt(results.get(2 * i).getValue(FAMILY, CQ)));
      assertTrue(results.get(2 * i + 1).isEmpty());
    }
    Admin admin = TEST_UTIL.getAdmin();
    admin.flush(TABLE_NAME);
    TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME).forEach(r -> {
      byte[] startKey = r.getRegionInfo().getStartKey();
      int number = startKey.length == 0 ? 55 : Integer.parseInt(Bytes.toString(startKey));
      byte[] splitPoint = Bytes.toBytes(String.format("%03d", number + 55));
      try {
        admin.splitRegion(r.getRegionInfo().getRegionName(), splitPoint);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
    // we are not going to test the function of split so no assertion here. Just wait for a while
    // and then start our work.
    Thread.sleep(5000);
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
  public void testMixed() throws InterruptedException, ExecutionException {
    AsyncTableBase table = tableGetter.apply(TABLE_NAME);
    table.putAll(IntStream.range(0, 5)
        .mapToObj(i -> new Put(Bytes.toBytes(i)).addColumn(FAMILY, CQ, Bytes.toBytes((long) i)))
        .collect(Collectors.toList())).get();
    List<Row> actions = new ArrayList<>();
    actions.add(new Get(Bytes.toBytes(0)));
    actions.add(new Put(Bytes.toBytes(1)).addColumn(FAMILY, CQ, Bytes.toBytes((long) 2)));
    actions.add(new Delete(Bytes.toBytes(2)));
    actions.add(new Increment(Bytes.toBytes(3)).addColumn(FAMILY, CQ, 1));
    actions.add(new Append(Bytes.toBytes(4)).add(FAMILY, CQ, Bytes.toBytes(4)));
    List<Object> results = table.batchAll(actions).get();
    assertEquals(5, results.size());
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
  }

  public static final class ErrorInjectObserver extends BaseRegionObserver {

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
    HTableDescriptor htd = admin.getTableDescriptor(TABLE_NAME);
    htd.addCoprocessor(ErrorInjectObserver.class.getName());
    admin.modifyTable(TABLE_NAME, htd);
    AsyncTableBase table = tableGetter.apply(TABLE_NAME);
    table.putAll(Arrays.asList(SPLIT_KEYS).stream().map(k -> new Put(k).addColumn(FAMILY, CQ, k))
        .collect(Collectors.toList())).get();
    List<CompletableFuture<Result>> futures = table
        .get(Arrays.asList(SPLIT_KEYS).stream().map(k -> new Get(k)).collect(Collectors.toList()));
    for (int i = 0; i < SPLIT_KEYS.length - 1; i++) {
      assertArrayEquals(SPLIT_KEYS[i], futures.get(i).get().getValue(FAMILY, CQ));
    }
    try {
      futures.get(SPLIT_KEYS.length - 1).get();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(RetriesExhaustedException.class));
    }
  }
}
