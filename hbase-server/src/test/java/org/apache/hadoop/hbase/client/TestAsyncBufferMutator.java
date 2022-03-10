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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;
import org.apache.hbase.thirdparty.io.netty.util.Timeout;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncBufferMutator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncBufferMutator.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static TableName MULTI_REGION_TABLE_NAME = TableName.valueOf("async-multi-region");

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  private static int COUNT = 100;

  private static byte[] VALUE = new byte[1024];

  private static AsyncConnection CONN;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE_NAME, CF);
    TEST_UTIL.createMultiRegionTable(MULTI_REGION_TABLE_NAME, CF);
    CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    Bytes.random(VALUE);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testWithMultiRegionTable() throws InterruptedException {
    test(MULTI_REGION_TABLE_NAME);
  }

  @Test
  public void testWithSingleRegionTable() throws InterruptedException {
    test(TABLE_NAME);
  }

  private void test(TableName tableName) throws InterruptedException {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    try (AsyncBufferedMutator mutator =
      CONN.getBufferedMutatorBuilder(tableName).setWriteBufferSize(16 * 1024).build()) {
      List<CompletableFuture<Void>> fs = mutator.mutate(IntStream.range(0, COUNT / 2)
        .mapToObj(i -> new Put(Bytes.toBytes(i)).addColumn(CF, CQ, VALUE))
        .collect(Collectors.toList()));
      // exceeded the write buffer size, a flush will be called directly
      fs.forEach(f -> f.join());
      IntStream.range(COUNT / 2, COUNT).forEach(i -> {
        futures.add(mutator.mutate(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, VALUE)));
      });
      // the first future should have been sent out.
      futures.get(0).join();
      Thread.sleep(2000);
      // the last one should still be in write buffer
      assertFalse(futures.get(futures.size() - 1).isDone());
    }
    // mutator.close will call mutator.flush automatically so all tasks should have been done.
    futures.forEach(f -> f.join());
    AsyncTable<?> table = CONN.getTable(tableName);
    IntStream.range(0, COUNT).mapToObj(i -> new Get(Bytes.toBytes(i))).map(g -> table.get(g).join())
      .forEach(r -> {
        assertArrayEquals(VALUE, r.getValue(CF, CQ));
      });
  }

  @Test
  public void testClosedMutate() throws InterruptedException {
    AsyncBufferedMutator mutator = CONN.getBufferedMutator(TABLE_NAME);
    mutator.close();
    Put put = new Put(Bytes.toBytes(0)).addColumn(CF, CQ, VALUE);
    try {
      mutator.mutate(put).get();
      fail("Close check failed");
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(IOException.class));
      assertTrue(e.getCause().getMessage().startsWith("Already closed"));
    }
    for (CompletableFuture<Void> f : mutator.mutate(Arrays.asList(put))) {
      try {
        f.get();
        fail("Close check failed");
      } catch (ExecutionException e) {
        assertThat(e.getCause(), instanceOf(IOException.class));
        assertTrue(e.getCause().getMessage().startsWith("Already closed"));
      }
    }
  }

  @Test
  public void testNoPeriodicFlush() throws InterruptedException, ExecutionException {
    try (AsyncBufferedMutator mutator =
      CONN.getBufferedMutatorBuilder(TABLE_NAME).disableWriteBufferPeriodicFlush().build()) {
      Put put = new Put(Bytes.toBytes(0)).addColumn(CF, CQ, VALUE);
      CompletableFuture<?> future = mutator.mutate(put);
      Thread.sleep(2000);
      // assert that we have not flushed it out
      assertFalse(future.isDone());
      mutator.flush();
      future.get();
    }
    AsyncTable<?> table = CONN.getTable(TABLE_NAME);
    assertArrayEquals(VALUE, table.get(new Get(Bytes.toBytes(0))).get().getValue(CF, CQ));
  }

  @Test
  public void testPeriodicFlush() throws InterruptedException, ExecutionException {
    AsyncBufferedMutator mutator = CONN.getBufferedMutatorBuilder(TABLE_NAME)
      .setWriteBufferPeriodicFlush(1, TimeUnit.SECONDS).build();
    Put put = new Put(Bytes.toBytes(0)).addColumn(CF, CQ, VALUE);
    CompletableFuture<?> future = mutator.mutate(put);
    future.get();
    AsyncTable<?> table = CONN.getTable(TABLE_NAME);
    assertArrayEquals(VALUE, table.get(new Get(Bytes.toBytes(0))).get().getValue(CF, CQ));
  }

  // a bit deep into the implementation
  @Test
  public void testCancelPeriodicFlush() throws InterruptedException, ExecutionException {
    Put put = new Put(Bytes.toBytes(0)).addColumn(CF, CQ, VALUE);
    try (AsyncBufferedMutatorImpl mutator = (AsyncBufferedMutatorImpl) CONN
      .getBufferedMutatorBuilder(TABLE_NAME).setWriteBufferPeriodicFlush(1, TimeUnit.SECONDS)
      .setWriteBufferSize(10 * put.heapSize()).build()) {
      List<CompletableFuture<?>> futures = new ArrayList<>();
      futures.add(mutator.mutate(put));
      Timeout task = mutator.periodicFlushTask;
      // we should have scheduled a periodic flush task
      assertNotNull(task);
      for (int i = 1;; i++) {
        futures.add(mutator.mutate(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, VALUE)));
        if (mutator.periodicFlushTask == null) {
          break;
        }
      }
      assertTrue(task.isCancelled());
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
      AsyncTable<?> table = CONN.getTable(TABLE_NAME);
      for (int i = 0; i < futures.size(); i++) {
        assertArrayEquals(VALUE, table.get(new Get(Bytes.toBytes(i))).get().getValue(CF, CQ));
      }
    }
  }

  @Test
  public void testCancelPeriodicFlushByManuallyFlush()
      throws InterruptedException, ExecutionException {
    try (AsyncBufferedMutatorImpl mutator =
      (AsyncBufferedMutatorImpl) CONN.getBufferedMutatorBuilder(TABLE_NAME)
        .setWriteBufferPeriodicFlush(1, TimeUnit.SECONDS).build()) {
      CompletableFuture<?> future =
        mutator.mutate(new Put(Bytes.toBytes(0)).addColumn(CF, CQ, VALUE));
      Timeout task = mutator.periodicFlushTask;
      // we should have scheduled a periodic flush task
      assertNotNull(task);
      mutator.flush();
      assertTrue(task.isCancelled());
      future.get();
      AsyncTable<?> table = CONN.getTable(TABLE_NAME);
      assertArrayEquals(VALUE, table.get(new Get(Bytes.toBytes(0))).get().getValue(CF, CQ));
    }
  }

  @Test
  public void testCancelPeriodicFlushByClose() throws InterruptedException, ExecutionException {
    CompletableFuture<?> future;
    Timeout task;
    try (AsyncBufferedMutatorImpl mutator =
      (AsyncBufferedMutatorImpl) CONN.getBufferedMutatorBuilder(TABLE_NAME)
        .setWriteBufferPeriodicFlush(1, TimeUnit.SECONDS).build()) {
      future = mutator.mutate(new Put(Bytes.toBytes(0)).addColumn(CF, CQ, VALUE));
      task = mutator.periodicFlushTask;
      // we should have scheduled a periodic flush task
      assertNotNull(task);
    }
    assertTrue(task.isCancelled());
    future.get();
    AsyncTable<?> table = CONN.getTable(TABLE_NAME);
    assertArrayEquals(VALUE, table.get(new Get(Bytes.toBytes(0))).get().getValue(CF, CQ));
  }

  private static final class AsyncBufferMutatorForTest extends AsyncBufferedMutatorImpl {

    private int flushCount;

    AsyncBufferMutatorForTest(HashedWheelTimer periodicalFlushTimer, AsyncTable<?> table,
        long writeBufferSize, long periodicFlushTimeoutNs, int maxKeyValueSize) {
      super(periodicalFlushTimer, table, writeBufferSize, periodicFlushTimeoutNs, maxKeyValueSize);
    }

    @Override
    protected void internalFlush() {
      flushCount++;
      super.internalFlush();
    }
  }

  @Test
  public void testRaceBetweenNormalFlushAndPeriodicFlush()
      throws InterruptedException, ExecutionException {
    Put put = new Put(Bytes.toBytes(0)).addColumn(CF, CQ, VALUE);
    try (AsyncBufferMutatorForTest mutator =
      new AsyncBufferMutatorForTest(AsyncConnectionImpl.RETRY_TIMER, CONN.getTable(TABLE_NAME),
        10 * put.heapSize(), TimeUnit.MILLISECONDS.toNanos(200), 1024 * 1024)) {
      CompletableFuture<?> future = mutator.mutate(put);
      Timeout task = mutator.periodicFlushTask;
      // we should have scheduled a periodic flush task
      assertNotNull(task);
      synchronized (mutator) {
        // synchronized on mutator to prevent periodic flush to be executed
        Thread.sleep(500);
        // the timeout should be issued
        assertTrue(task.isExpired());
        // but no flush is issued as we hold the lock
        assertEquals(0, mutator.flushCount);
        assertFalse(future.isDone());
        // manually flush, then release the lock
        mutator.flush();
      }
      // this is a bit deep into the implementation in netty but anyway let's add a check here to
      // confirm that an issued timeout can not be canceled by netty framework.
      assertFalse(task.isCancelled());
      // and the mutation is done
      future.get();
      AsyncTable<?> table = CONN.getTable(TABLE_NAME);
      assertArrayEquals(VALUE, table.get(new Get(Bytes.toBytes(0))).get().getValue(CF, CQ));
      // only the manual flush, the periodic flush should have been canceled by us
      assertEquals(1, mutator.flushCount);
    }
  }
}
