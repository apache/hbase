/*
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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncClientPauseForRpcThrottling {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncClientPauseForRpcThrottling.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableName TABLE_NAME = TableName.valueOf("RpcThrottling");

  private static byte[] FAMILY = Bytes.toBytes("Family");

  private static byte[] QUALIFIER = Bytes.toBytes("Qualifier");

  private static AsyncConnection CONN;
  private static final AtomicBoolean THROTTLE = new AtomicBoolean(false);
  private static final AtomicInteger FORCE_RETRIES = new AtomicInteger(0);
  private static final long WAIT_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);
  private static final int RETRY_COUNT = 3;
  private static final int MAX_MULTIPLIER_EXPECTATION = 2;

  public static final class ThrottlingRSRpcServicesForTest extends RSRpcServices {

    public ThrottlingRSRpcServicesForTest(HRegionServer rs) throws IOException {
      super(rs);
    }

    @Override
    public ClientProtos.GetResponse get(RpcController controller, ClientProtos.GetRequest request)
      throws ServiceException {
      maybeForceRetry();
      maybeThrottle();
      return super.get(controller, request);
    }

    @Override
    public ClientProtos.MultiResponse multi(RpcController rpcc, ClientProtos.MultiRequest request)
      throws ServiceException {
      maybeForceRetry();
      maybeThrottle();
      return super.multi(rpcc, request);
    }

    @Override
    public ClientProtos.ScanResponse scan(RpcController controller,
      ClientProtos.ScanRequest request) throws ServiceException {
      maybeForceRetry();
      maybeThrottle();
      return super.scan(controller, request);
    }

    private void maybeForceRetry() throws ServiceException {
      if (FORCE_RETRIES.get() > 0) {
        FORCE_RETRIES.addAndGet(-1);
        throw new ServiceException(new RegionTooBusyException("Retry"));
      }
    }

    private void maybeThrottle() throws ServiceException {
      if (THROTTLE.get()) {
        THROTTLE.set(false);
        throw new ServiceException(new RpcThrottlingException("number of requests exceeded - wait "
          + TimeUnit.NANOSECONDS.toMillis(WAIT_INTERVAL_NANOS) + "ms"));
      }
    }
  }

  public static final class ThrottlingRegionServerForTest extends HRegionServer {

    public ThrottlingRegionServerForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
      return new ThrottlingRSRpcServicesForTest(this);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    assertTrue(
      "The MAX_MULTIPLIER_EXPECTATION must be less than HConstants.RETRY_BACKOFF[RETRY_COUNT] "
        + "in order for our tests to adequately verify that we aren't "
        + "multiplying throttled pauses based on the retry count.",
      MAX_MULTIPLIER_EXPECTATION < HConstants.RETRY_BACKOFF[RETRY_COUNT]);

    UTIL.getConfiguration().setLong(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    UTIL.startMiniCluster(1);
    UTIL.getMiniHBaseCluster().getConfiguration().setClass(HConstants.REGION_SERVER_IMPL,
      ThrottlingRegionServerForTest.class, HRegionServer.class);
    HRegionServer regionServer = UTIL.getMiniHBaseCluster().startRegionServer().getRegionServer();

    try (Table table = UTIL.createTable(TABLE_NAME, FAMILY)) {
      UTIL.waitTableAvailable(TABLE_NAME);
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUALIFIER, Bytes.toBytes(i)));
      }
    }

    UTIL.getAdmin().move(UTIL.getAdmin().getRegions(TABLE_NAME).get(0).getEncodedNameAsBytes(),
      regionServer.getServerName());
    Configuration conf = new Configuration(UTIL.getConfiguration());
    CONN = ConnectionFactory.createAsyncConnection(conf).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.getAdmin().disableTable(TABLE_NAME);
    UTIL.getAdmin().deleteTable(TABLE_NAME);
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  private void assertTime(Callable<Void> callable, long time, boolean isGreater) throws Exception {
    long costNs = getCostNs(callable);
    if (isGreater) {
      assertTrue(costNs > time);
    } else {
      assertTrue(costNs <= time);
    }
  }

  private void assertTimeBetween(Callable<Void> callable, long minNs, long maxNs) throws Exception {
    long costNs = getCostNs(callable);
    assertTrue(costNs > minNs);
    assertTrue(costNs < maxNs);
  }

  private long getCostNs(Callable<Void> callable) throws Exception {
    long startNs = System.nanoTime();
    callable.call();
    return System.nanoTime() - startNs;
  }

  @Test
  public void itWaitsForThrottledGet() throws Exception {
    boolean isThrottled = true;
    THROTTLE.set(isThrottled);
    AsyncTable<AdvancedScanResultConsumer> table = CONN.getTable(TABLE_NAME);
    assertTime(() -> {
      table.get(new Get(Bytes.toBytes(0))).get();
      return null;
    }, WAIT_INTERVAL_NANOS, isThrottled);
  }

  @Test
  public void itDoesNotWaitForUnthrottledGet() throws Exception {
    boolean isThrottled = false;
    THROTTLE.set(isThrottled);
    AsyncTable<AdvancedScanResultConsumer> table = CONN.getTable(TABLE_NAME);
    assertTime(() -> {
      table.get(new Get(Bytes.toBytes(0))).get();
      return null;
    }, WAIT_INTERVAL_NANOS, isThrottled);
  }

  @Test
  public void itDoesNotWaitForThrottledGetExceedingTimeout() throws Exception {
    AsyncTable<AdvancedScanResultConsumer> table =
      CONN.getTableBuilder(TABLE_NAME).setOperationTimeout(1, TimeUnit.MILLISECONDS).build();
    boolean isThrottled = true;
    THROTTLE.set(isThrottled);
    assertTime(() -> {
      assertThrows(ExecutionException.class, () -> table.get(new Get(Bytes.toBytes(0))).get());
      return null;
    }, WAIT_INTERVAL_NANOS, false);
  }

  @Test
  public void itDoesNotMultiplyThrottledGetWait() throws Exception {
    THROTTLE.set(true);
    FORCE_RETRIES.set(RETRY_COUNT);

    AsyncTable<AdvancedScanResultConsumer> table =
      CONN.getTableBuilder(TABLE_NAME).setOperationTimeout(1, TimeUnit.MINUTES)
        .setMaxRetries(RETRY_COUNT + 1).setRetryPause(1, TimeUnit.NANOSECONDS).build();

    assertTimeBetween(() -> {
      table.get(new Get(Bytes.toBytes(0))).get();
      return null;
    }, WAIT_INTERVAL_NANOS, MAX_MULTIPLIER_EXPECTATION * WAIT_INTERVAL_NANOS);
  }

  @Test
  public void itWaitsForThrottledBatch() throws Exception {
    boolean isThrottled = true;
    THROTTLE.set(isThrottled);
    assertTime(() -> {
      List<CompletableFuture<?>> futures = new ArrayList<>();
      try (AsyncBufferedMutator mutator = CONN.getBufferedMutator(TABLE_NAME)) {
        for (int i = 100; i < 110; i++) {
          futures.add(mutator
            .mutate(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUALIFIER, Bytes.toBytes(i))));
        }
      }
      return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    }, WAIT_INTERVAL_NANOS, isThrottled);
  }

  @Test
  public void itDoesNotWaitForUnthrottledBatch() throws Exception {
    boolean isThrottled = false;
    THROTTLE.set(isThrottled);
    assertTime(() -> {
      List<CompletableFuture<?>> futures = new ArrayList<>();
      try (AsyncBufferedMutator mutator = CONN.getBufferedMutator(TABLE_NAME)) {
        for (int i = 100; i < 110; i++) {
          futures.add(mutator
            .mutate(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUALIFIER, Bytes.toBytes(i))));
        }
      }
      return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    }, WAIT_INTERVAL_NANOS, isThrottled);
  }

  @Test
  public void itDoesNotWaitForThrottledBatchExceedingTimeout() throws Exception {
    boolean isThrottled = true;
    THROTTLE.set(isThrottled);
    assertTime(() -> {
      List<CompletableFuture<?>> futures = new ArrayList<>();
      try (AsyncBufferedMutator mutator = CONN.getBufferedMutatorBuilder(TABLE_NAME)
        .setOperationTimeout(1, TimeUnit.MILLISECONDS).build()) {
        for (int i = 100; i < 110; i++) {
          futures.add(mutator
            .mutate(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUALIFIER, Bytes.toBytes(i))));
        }
      }
      assertThrows(ExecutionException.class,
        () -> CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get());
      return null;
    }, WAIT_INTERVAL_NANOS, false);
  }

  @Test
  public void itDoesNotMultiplyThrottledBatchWait() throws Exception {
    THROTTLE.set(true);
    FORCE_RETRIES.set(RETRY_COUNT);

    assertTimeBetween(() -> {
      List<CompletableFuture<?>> futures = new ArrayList<>();
      try (AsyncBufferedMutator mutator =
        CONN.getBufferedMutatorBuilder(TABLE_NAME).setOperationTimeout(1, TimeUnit.MINUTES)
          .setMaxRetries(RETRY_COUNT + 1).setRetryPause(1, TimeUnit.NANOSECONDS).build()) {
        for (int i = 100; i < 110; i++) {
          futures.add(mutator
            .mutate(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUALIFIER, Bytes.toBytes(i))));
        }
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
      return null;
    }, WAIT_INTERVAL_NANOS, MAX_MULTIPLIER_EXPECTATION * WAIT_INTERVAL_NANOS);
  }

  @Test
  public void itWaitsForThrottledScan() throws Exception {
    boolean isThrottled = true;
    THROTTLE.set(isThrottled);
    assertTime(() -> {
      try (
        ResultScanner scanner = CONN.getTable(TABLE_NAME).getScanner(new Scan().setCaching(80))) {
        for (int i = 0; i < 100; i++) {
          Result result = scanner.next();
          assertArrayEquals(Bytes.toBytes(i), result.getValue(FAMILY, QUALIFIER));
        }
      }
      return null;
    }, WAIT_INTERVAL_NANOS, isThrottled);
  }

  @Test
  public void itDoesNotWaitForUnthrottledScan() throws Exception {
    boolean isThrottled = false;
    THROTTLE.set(isThrottled);
    assertTime(() -> {
      try (
        ResultScanner scanner = CONN.getTable(TABLE_NAME).getScanner(new Scan().setCaching(80))) {
        for (int i = 0; i < 100; i++) {
          Result result = scanner.next();
          assertArrayEquals(Bytes.toBytes(i), result.getValue(FAMILY, QUALIFIER));
        }
      }
      return null;
    }, WAIT_INTERVAL_NANOS, isThrottled);
  }

  @Test
  public void itDoesNotWaitForThrottledScanExceedingTimeout() throws Exception {
    AsyncTable<AdvancedScanResultConsumer> table =
      CONN.getTableBuilder(TABLE_NAME).setScanTimeout(1, TimeUnit.MILLISECONDS).build();
    boolean isThrottled = true;
    THROTTLE.set(isThrottled);
    assertTime(() -> {
      try (ResultScanner scanner = table.getScanner(new Scan().setCaching(80))) {
        for (int i = 0; i < 100; i++) {
          assertThrows(RetriesExhaustedException.class, scanner::next);
        }
      }
      return null;
    }, WAIT_INTERVAL_NANOS, false);
  }

  @Test
  public void itDoesNotMultiplyThrottledScanWait() throws Exception {
    THROTTLE.set(true);
    FORCE_RETRIES.set(RETRY_COUNT);

    AsyncTable<AdvancedScanResultConsumer> table =
      CONN.getTableBuilder(TABLE_NAME).setOperationTimeout(1, TimeUnit.MINUTES)
        .setMaxRetries(RETRY_COUNT + 1).setRetryPause(1, TimeUnit.NANOSECONDS).build();

    assertTimeBetween(() -> {
      try (ResultScanner scanner = table.getScanner(new Scan().setCaching(80))) {
        for (int i = 0; i < 100; i++) {
          Result result = scanner.next();
          assertArrayEquals(Bytes.toBytes(i), result.getValue(FAMILY, QUALIFIER));
        }
      }
      return null;
    }, WAIT_INTERVAL_NANOS, MAX_MULTIPLIER_EXPECTATION * WAIT_INTERVAL_NANOS);
  }
}
