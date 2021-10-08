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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncSingleRequestRpcRetryingCaller {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncSingleRequestRpcRetryingCaller.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUALIFIER = Bytes.toBytes("cq");

  private static byte[] ROW = Bytes.toBytes("row");

  private static byte[] VALUE = Bytes.toBytes("value");

  private static AsyncConnectionImpl CONN;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(2);
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    ConnectionRegistry registry =
        ConnectionRegistryFactory.getRegistry(TEST_UTIL.getConfiguration());
    CONN = new AsyncConnectionImpl(TEST_UTIL.getConfiguration(), registry,
      registry.getClusterId().get(), User.getCurrent());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(CONN, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRegionMove() throws InterruptedException, ExecutionException, IOException {
    // This will leave a cached entry in location cache
    HRegionLocation loc = CONN.getRegionLocator(TABLE_NAME).getRegionLocation(ROW).get();
    int index = TEST_UTIL.getHBaseCluster().getServerWith(loc.getRegion().getRegionName());
    TEST_UTIL.getAdmin().move(loc.getRegion().getEncodedNameAsBytes(),
      TEST_UTIL.getHBaseCluster().getRegionServer(1 - index).getServerName());
    AsyncTable<?> table = CONN.getTableBuilder(TABLE_NAME).setRetryPause(100, TimeUnit.MILLISECONDS)
      .setMaxRetries(30).build();
    table.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE)).get();

    // move back
    TEST_UTIL.getAdmin().move(loc.getRegion().getEncodedNameAsBytes(), loc.getServerName());
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
    try {
      CONN.callerFactory.single().table(TABLE_NAME).row(ROW).operationTimeout(1, TimeUnit.DAYS)
        .maxAttempts(3).pause(10, TimeUnit.MILLISECONDS)
        .action((controller, loc, stub) -> failedFuture()).call().get();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(RetriesExhaustedException.class));
    }
  }

  @Test
  public void testOperationTimeout() throws IOException, InterruptedException {
    long startNs = System.nanoTime();
    try {
      CONN.callerFactory.single().table(TABLE_NAME).row(ROW).operationTimeout(1, TimeUnit.SECONDS)
        .pause(100, TimeUnit.MILLISECONDS).maxAttempts(Integer.MAX_VALUE)
        .action((controller, loc, stub) -> failedFuture()).call().get();
      fail();
    } catch (ExecutionException e) {
      e.printStackTrace();
      assertThat(e.getCause(), instanceOf(RetriesExhaustedException.class));
    }
    long costNs = System.nanoTime() - startNs;
    assertTrue(costNs >= TimeUnit.SECONDS.toNanos(1));
    assertTrue(costNs < TimeUnit.SECONDS.toNanos(2));
  }

  @Test
  public void testLocateError() throws IOException, InterruptedException, ExecutionException {
    AtomicBoolean errorTriggered = new AtomicBoolean(false);
    AtomicInteger count = new AtomicInteger(0);
    HRegionLocation loc = CONN.getRegionLocator(TABLE_NAME).getRegionLocation(ROW).get();
    AsyncRegionLocator mockedLocator =
      new AsyncRegionLocator(CONN, AsyncConnectionImpl.RETRY_TIMER) {
        @Override
        CompletableFuture<HRegionLocation> getRegionLocation(TableName tableName, byte[] row,
            int replicaId, RegionLocateType locateType, long timeoutNs) {
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
            return super.getRegionLocation(tableName, row, replicaId, locateType, timeoutNs);
          }
        }

        @Override
        void updateCachedLocationOnError(HRegionLocation loc, Throwable exception) {
        }
      };
    try (AsyncConnectionImpl mockedConn = new AsyncConnectionImpl(CONN.getConfiguration(),
      CONN.registry, CONN.registry.getClusterId().get(), User.getCurrent()) {

      @Override
      AsyncRegionLocator getLocator() {
        return mockedLocator;
      }
    }) {
      AsyncTable<?> table = mockedConn.getTableBuilder(TABLE_NAME)
        .setRetryPause(100, TimeUnit.MILLISECONDS).setMaxRetries(5).build();
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
