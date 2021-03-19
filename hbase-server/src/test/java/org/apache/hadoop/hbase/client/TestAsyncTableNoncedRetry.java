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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableNoncedRetry {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableNoncedRetry.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME = TableName.valueOf("async");

  private static final byte[] FAMILY = Bytes.toBytes("cf");

  private static final byte[] QUALIFIER = Bytes.toBytes("cq");

  private static final byte[] QUALIFIER2 = Bytes.toBytes("cq2");

  private static final byte[] QUALIFIER3 = Bytes.toBytes("cq3");

  private static final byte[] VALUE = Bytes.toBytes("value");

  private static AsyncConnection ASYNC_CONN;

  @Rule
  public TestName testName = new TestName();

  private byte[] row;

  private static final AtomicInteger CALLED = new AtomicInteger();

  private static final long SLEEP_TIME = 2000;

  private static final long RPC_TIMEOUT = SLEEP_TIME / 4 * 3; // three fourths of the sleep time

  // The number of miniBatchOperations that are executed in a RegionServer
  private static int miniBatchOperationCount;

  public static final class SleepOnceCP implements RegionObserver, RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) {
      // We sleep when the last of the miniBatchOperations is executed
      if (CALLED.getAndIncrement() == miniBatchOperationCount - 1) {
        Threads.sleepWithoutInterrupt(SLEEP_TIME);
      }
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.getAdmin()
      .createTable(TableDescriptorBuilder.newBuilder(TABLE_NAME)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setCoprocessor(SleepOnceCP.class.getName()).build());
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(ASYNC_CONN, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    row = Bytes.toBytes(testName.getMethodName().replaceAll("[^0-9A-Za-z]", "_"));
    CALLED.set(0);
  }

  @Test
  public void testAppend() throws InterruptedException, ExecutionException {
    assertEquals(0, CALLED.get());
    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setRpcTimeout(RPC_TIMEOUT, TimeUnit.MILLISECONDS).build();

    miniBatchOperationCount = 1;
    Result result = table.append(new Append(row).addColumn(FAMILY, QUALIFIER, VALUE)).get();

    // make sure we called twice and the result is still correct
    assertEquals(2, CALLED.get());
    assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
  }

  @Test
  public void testAppendWhenReturnResultsEqualsFalse() throws InterruptedException,
    ExecutionException {
    assertEquals(0, CALLED.get());
    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setRpcTimeout(RPC_TIMEOUT, TimeUnit.MILLISECONDS).build();

    miniBatchOperationCount = 1;
    Result result = table.append(new Append(row).addColumn(FAMILY, QUALIFIER, VALUE)
      .setReturnResults(false)).get();

    // make sure we called twice and the result is still correct
    assertEquals(2, CALLED.get());
    assertTrue(result.isEmpty());
  }

  @Test
  public void testIncrement() throws InterruptedException, ExecutionException {
    assertEquals(0, CALLED.get());
    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setRpcTimeout(RPC_TIMEOUT, TimeUnit.MILLISECONDS).build();

    miniBatchOperationCount = 1;
    long result = table.incrementColumnValue(row, FAMILY, QUALIFIER, 1L).get();

    // make sure we called twice and the result is still correct
    assertEquals(2, CALLED.get());
    assertEquals(1L, result);
  }

  @Test
  public void testIncrementWhenReturnResultsEqualsFalse() throws InterruptedException,
    ExecutionException {
    assertEquals(0, CALLED.get());
    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setRpcTimeout(RPC_TIMEOUT, TimeUnit.MILLISECONDS).build();

    miniBatchOperationCount = 1;
    Result result = table.increment(new Increment(row).addColumn(FAMILY, QUALIFIER, 1L)
      .setReturnResults(false)).get();

    // make sure we called twice and the result is still correct
    assertEquals(2, CALLED.get());
    assertTrue(result.isEmpty());
  }

  @Test
  public void testIncrementInRowMutations()
    throws InterruptedException, ExecutionException, IOException {
    assertEquals(0, CALLED.get());
    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setWriteRpcTimeout(RPC_TIMEOUT, TimeUnit.MILLISECONDS).build();

    miniBatchOperationCount = 1;
    Result result = table.mutateRow(new RowMutations(row)
      .add(new Increment(row).addColumn(FAMILY, QUALIFIER, 1L))
      .add((Mutation) new Delete(row).addColumn(FAMILY, QUALIFIER2))).get();

    // make sure we called twice and the result is still correct
    assertEquals(2, CALLED.get());
    assertEquals(1L, Bytes.toLong(result.getValue(FAMILY, QUALIFIER)));
  }

  @Test
  public void testAppendInRowMutations()
    throws InterruptedException, ExecutionException, IOException {
    assertEquals(0, CALLED.get());
    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setWriteRpcTimeout(RPC_TIMEOUT, TimeUnit.MILLISECONDS).build();

    miniBatchOperationCount = 1;
    Result result = table.mutateRow(new RowMutations(row)
      .add(new Append(row).addColumn(FAMILY, QUALIFIER, VALUE))
      .add((Mutation) new Delete(row).addColumn(FAMILY, QUALIFIER2))).get();

    // make sure we called twice and the result is still correct
    assertEquals(2, CALLED.get());
    assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
  }

  @Test
  public void testIncrementAndAppendInRowMutations()
    throws InterruptedException, ExecutionException, IOException {
    assertEquals(0, CALLED.get());
    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setWriteRpcTimeout(RPC_TIMEOUT, TimeUnit.MILLISECONDS).build();

    miniBatchOperationCount = 1;
    Result result = table.mutateRow(new RowMutations(row)
      .add(new Increment(row).addColumn(FAMILY, QUALIFIER, 1L))
      .add(new Append(row).addColumn(FAMILY, QUALIFIER2, VALUE))).get();

    // make sure we called twice and the result is still correct
    assertEquals(2, CALLED.get());
    assertEquals(1L, Bytes.toLong(result.getValue(FAMILY, QUALIFIER)));
    assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER2));
  }

  @Test
  public void testIncrementInCheckAndMutate() throws InterruptedException, ExecutionException {
    assertEquals(0, CALLED.get());
    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setRpcTimeout(RPC_TIMEOUT, TimeUnit.MILLISECONDS).build();

    miniBatchOperationCount = 1;
    CheckAndMutateResult result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifNotExists(FAMILY, QUALIFIER2)
      .build(new Increment(row).addColumn(FAMILY, QUALIFIER, 1L))).get();

    // make sure we called twice and the result is still correct
    assertEquals(2, CALLED.get());
    assertTrue(result.isSuccess());
    assertEquals(1L, Bytes.toLong(result.getResult().getValue(FAMILY, QUALIFIER)));
  }

  @Test
  public void testAppendInCheckAndMutate() throws InterruptedException, ExecutionException {
    assertEquals(0, CALLED.get());
    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setRpcTimeout(RPC_TIMEOUT, TimeUnit.MILLISECONDS).build();

    miniBatchOperationCount = 1;
    CheckAndMutateResult result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifNotExists(FAMILY, QUALIFIER2)
      .build(new Append(row).addColumn(FAMILY, QUALIFIER, VALUE))).get();

    // make sure we called twice and the result is still correct
    assertEquals(2, CALLED.get());
    assertTrue(result.isSuccess());
    assertArrayEquals(VALUE, result.getResult().getValue(FAMILY, QUALIFIER));
  }

  @Test
  public void testIncrementInRowMutationsInCheckAndMutate() throws InterruptedException,
    ExecutionException, IOException {
    assertEquals(0, CALLED.get());
    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setRpcTimeout(RPC_TIMEOUT, TimeUnit.MILLISECONDS).build();

    miniBatchOperationCount = 1;
    CheckAndMutateResult result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifNotExists(FAMILY, QUALIFIER3)
      .build(new RowMutations(row).add(new Increment(row).addColumn(FAMILY, QUALIFIER, 1L))
        .add((Mutation) new Delete(row).addColumn(FAMILY, QUALIFIER2)))).get();

    // make sure we called twice and the result is still correct
    assertEquals(2, CALLED.get());
    assertTrue(result.isSuccess());
    assertEquals(1L, Bytes.toLong(result.getResult().getValue(FAMILY, QUALIFIER)));
  }

  @Test
  public void testAppendInRowMutationsInCheckAndMutate() throws InterruptedException,
    ExecutionException, IOException {
    assertEquals(0, CALLED.get());
    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setRpcTimeout(RPC_TIMEOUT, TimeUnit.MILLISECONDS).build();

    miniBatchOperationCount = 1;
    CheckAndMutateResult result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifNotExists(FAMILY, QUALIFIER3)
      .build(new RowMutations(row).add(new Append(row).addColumn(FAMILY, QUALIFIER, VALUE))
        .add((Mutation) new Delete(row).addColumn(FAMILY, QUALIFIER2)))).get();

    // make sure we called twice and the result is still correct
    assertEquals(2, CALLED.get());
    assertTrue(result.isSuccess());
    assertArrayEquals(VALUE, result.getResult().getValue(FAMILY, QUALIFIER));
  }

  @Test
  public void testIncrementAndAppendInRowMutationsInCheckAndMutate() throws InterruptedException,
    ExecutionException, IOException {
    assertEquals(0, CALLED.get());
    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setRpcTimeout(RPC_TIMEOUT, TimeUnit.MILLISECONDS).build();

    miniBatchOperationCount = 1;
    CheckAndMutateResult result = table.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifNotExists(FAMILY, QUALIFIER3)
      .build(new RowMutations(row).add(new Increment(row).addColumn(FAMILY, QUALIFIER, 1L))
        .add(new Append(row).addColumn(FAMILY, QUALIFIER2, VALUE)))).get();

    // make sure we called twice and the result is still correct
    assertEquals(2, CALLED.get());
    assertTrue(result.isSuccess());
    assertEquals(1L, Bytes.toLong(result.getResult().getValue(FAMILY, QUALIFIER)));
    assertArrayEquals(VALUE, result.getResult().getValue(FAMILY, QUALIFIER2));
  }

  @Test
  public void testBatch() throws InterruptedException,
    ExecutionException, IOException {
    byte[] row2 = Bytes.toBytes(Bytes.toString(row) + "2");
    byte[] row3 = Bytes.toBytes(Bytes.toString(row) + "3");
    byte[] row4 = Bytes.toBytes(Bytes.toString(row) + "4");
    byte[] row5 = Bytes.toBytes(Bytes.toString(row) + "5");
    byte[] row6 = Bytes.toBytes(Bytes.toString(row) + "6");

    assertEquals(0, CALLED.get());

    AsyncTable<?> table = ASYNC_CONN.getTableBuilder(TABLE_NAME)
      .setRpcTimeout(RPC_TIMEOUT, TimeUnit.MILLISECONDS).build();

    miniBatchOperationCount = 6;
    List<Object> results = table.batchAll(Arrays.asList(
      new Append(row).addColumn(FAMILY, QUALIFIER, VALUE),
      new Increment(row2).addColumn(FAMILY, QUALIFIER, 1L),
      new RowMutations(row3)
        .add(new Increment(row3).addColumn(FAMILY, QUALIFIER, 1L))
        .add(new Append(row3).addColumn(FAMILY, QUALIFIER2, VALUE)),
      CheckAndMutate.newBuilder(row4)
        .ifNotExists(FAMILY, QUALIFIER2)
        .build(new Increment(row4).addColumn(FAMILY, QUALIFIER, 1L)),
      CheckAndMutate.newBuilder(row5)
        .ifNotExists(FAMILY, QUALIFIER2)
        .build(new Append(row5).addColumn(FAMILY, QUALIFIER, VALUE)),
      CheckAndMutate.newBuilder(row6)
        .ifNotExists(FAMILY, QUALIFIER3)
        .build(new RowMutations(row6).add(new Increment(row6).addColumn(FAMILY, QUALIFIER, 1L))
          .add(new Append(row6).addColumn(FAMILY, QUALIFIER2, VALUE))))).get();

    // make sure we called twice and the result is still correct

    // should be called 12 times as 6 miniBatchOperations are called twice
    assertEquals(12, CALLED.get());

    assertArrayEquals(VALUE, ((Result) results.get(0)).getValue(FAMILY, QUALIFIER));

    assertEquals(1L, Bytes.toLong(((Result) results.get(1)).getValue(FAMILY, QUALIFIER)));

    assertEquals(1L, Bytes.toLong(((Result) results.get(2)).getValue(FAMILY, QUALIFIER)));
    assertArrayEquals(VALUE, ((Result) results.get(2)).getValue(FAMILY, QUALIFIER2));

    CheckAndMutateResult result;

    result = (CheckAndMutateResult) results.get(3);
    assertTrue(result.isSuccess());
    assertEquals(1L, Bytes.toLong(result.getResult().getValue(FAMILY, QUALIFIER)));

    result = (CheckAndMutateResult) results.get(4);
    assertTrue(result.isSuccess());
    assertArrayEquals(VALUE, result.getResult().getValue(FAMILY, QUALIFIER));

    result = (CheckAndMutateResult) results.get(5);
    assertTrue(result.isSuccess());
    assertEquals(1L, Bytes.toLong(result.getResult().getValue(FAMILY, QUALIFIER)));
    assertArrayEquals(VALUE, result.getResult().getValue(FAMILY, QUALIFIER2));
  }
}
