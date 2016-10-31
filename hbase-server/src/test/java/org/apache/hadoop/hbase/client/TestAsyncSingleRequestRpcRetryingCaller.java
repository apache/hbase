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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncSingleRequestRpcRetryingCaller {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUALIFIER = Bytes.toBytes("cq");

  private static byte[] ROW = Bytes.toBytes("row");

  private static byte[] VALUE = Bytes.toBytes("value");

  private AsyncConnectionImpl asyncConn;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(2);
    TEST_UTIL.getAdmin().setBalancerRunning(false, true);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() {
    if (asyncConn != null) {
      asyncConn.close();
      asyncConn = null;
    }
  }

  private void initConn(int startLogErrorsCnt, long pauseMs, int maxRetires) throws IOException {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY, startLogErrorsCnt);
    conf.setLong(HConstants.HBASE_CLIENT_PAUSE, pauseMs);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, maxRetires);
    asyncConn = new AsyncConnectionImpl(conf, User.getCurrent());
  }

  @Test
  public void testRegionMove() throws InterruptedException, ExecutionException, IOException {
    initConn(0, 100, 30);
    // This will leave a cached entry in location cache
    HRegionLocation loc = asyncConn.getRegionLocator(TABLE_NAME).getRegionLocation(ROW).get();
    int index = TEST_UTIL.getHBaseCluster().getServerWith(loc.getRegionInfo().getRegionName());
    TEST_UTIL.getAdmin().move(loc.getRegionInfo().getEncodedNameAsBytes(), Bytes.toBytes(
      TEST_UTIL.getHBaseCluster().getRegionServer(1 - index).getServerName().getServerName()));
    AsyncTable table = asyncConn.getTable(TABLE_NAME);
    table.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE)).get();

    // move back
    TEST_UTIL.getAdmin().move(loc.getRegionInfo().getEncodedNameAsBytes(),
      Bytes.toBytes(loc.getServerName().getServerName()));
    Result result = table.get(new Get(ROW).addColumn(FAMILY, QUALIFIER)).get();
    assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
  }

  private <T> CompletableFuture<T> failedFuture() {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException("Inject error!"));
    return future;
  }

  @Test
  public void testMaxRetries() throws IOException, InterruptedException {
    initConn(0, 10, 2);
    try {
      asyncConn.callerFactory.single().table(TABLE_NAME).row(ROW).operationTimeout(1, TimeUnit.DAYS)
          .action((controller, loc, stub) -> failedFuture()).call().get();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(RetriesExhaustedException.class));
    }
  }

  @Test
  public void testOperationTimeout() throws IOException, InterruptedException {
    initConn(0, 100, Integer.MAX_VALUE);
    long startNs = System.nanoTime();
    try {
      asyncConn.callerFactory.single().table(TABLE_NAME).row(ROW)
          .operationTimeout(1, TimeUnit.SECONDS).action((controller, loc, stub) -> failedFuture())
          .call().get();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(RetriesExhaustedException.class));
    }
    long costNs = System.nanoTime() - startNs;
    assertTrue(costNs >= TimeUnit.SECONDS.toNanos(1));
    assertTrue(costNs < TimeUnit.SECONDS.toNanos(2));
  }

  @Test
  public void testLocateError() throws IOException, InterruptedException, ExecutionException {
    initConn(0, 100, 5);
    AtomicBoolean errorTriggered = new AtomicBoolean(false);
    AtomicInteger count = new AtomicInteger(0);
    HRegionLocation loc = asyncConn.getRegionLocator(TABLE_NAME).getRegionLocation(ROW).get();
    AsyncRegionLocator mockedLocator = new AsyncRegionLocator(asyncConn) {
      @Override
      CompletableFuture<HRegionLocation> getRegionLocation(TableName tableName, byte[] row) {
        if (tableName.equals(TABLE_NAME)) {
          CompletableFuture<HRegionLocation> future = new CompletableFuture<>();
          if (count.getAndIncrement() == 0) {
            errorTriggered.set(true);
            future.completeExceptionally(new RuntimeException("Inject error!"));
          } else {
            future.complete(loc);
          }
          return future;
        } else {
          return super.getRegionLocation(tableName, row);
        }
      }

      @Override
      CompletableFuture<HRegionLocation> getPreviousRegionLocation(TableName tableName,
          byte[] startRowOfCurrentRegion) {
        return super.getPreviousRegionLocation(tableName, startRowOfCurrentRegion);
      }

      @Override
      void updateCachedLocation(HRegionLocation loc, Throwable exception) {
      }
    };
    try (AsyncConnectionImpl mockedConn =
        new AsyncConnectionImpl(asyncConn.getConfiguration(), User.getCurrent()) {

          @Override
          AsyncRegionLocator getLocator() {
            return mockedLocator;
          }
        }) {
      AsyncTable table = new AsyncTableImpl(mockedConn, TABLE_NAME);
      table.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE)).get();
      assertTrue(errorTriggered.get());
      errorTriggered.set(false);
      count.set(0);
      Result result = table.get(new Get(ROW).addColumn(FAMILY, QUALIFIER)).get();
      assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
      assertTrue(errorTriggered.get());
    }
  }
}
