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
package org.apache.hadoop.hbase.quotas;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;

/**
 * Tests of QuotaCache that don't require a minicluster, unlike in TestQuotaCache
 */
@Tag(RegionServerTests.TAG)
@Tag(SmallTests.TAG)
public class TestQuotaCache2 {

  private static final Configuration conf = HBaseConfiguration.create();

  @Test
  public void testPreserveLimiterAvailability() throws Exception {
    // establish old cache with a limiter for 100 read bytes per second
    QuotaState oldState = new QuotaState();
    Map<String, QuotaState> oldCache = new HashMap<>();
    oldCache.put("my_table", oldState);
    QuotaProtos.Throttle throttle1 = QuotaProtos.Throttle.newBuilder()
      .setReadSize(QuotaProtos.TimedQuota.newBuilder().setTimeUnit(HBaseProtos.TimeUnit.SECONDS)
        .setSoftLimit(100).setScope(QuotaProtos.QuotaScope.MACHINE).build())
      .build();
    QuotaLimiter limiter1 = TimeBasedLimiter.fromThrottle(conf, throttle1);
    oldState.setGlobalLimiter(limiter1);

    // consume one byte from the limiter, so 99 will be left
    limiter1.consumeRead(1, 1, false);

    // establish new cache, also with a limiter for 100 read bytes per second
    QuotaState newState = new QuotaState();
    Map<String, QuotaState> newCache = new HashMap<>();
    newCache.put("my_table", newState);
    QuotaProtos.Throttle throttle2 = QuotaProtos.Throttle.newBuilder()
      .setReadSize(QuotaProtos.TimedQuota.newBuilder().setTimeUnit(HBaseProtos.TimeUnit.SECONDS)
        .setSoftLimit(100).setScope(QuotaProtos.QuotaScope.MACHINE).build())
      .build();
    QuotaLimiter limiter2 = TimeBasedLimiter.fromThrottle(conf, throttle2);
    newState.setGlobalLimiter(limiter2);

    // update new cache from old cache
    QuotaCache.updateNewCacheFromOld(oldCache, newCache);

    // verify that the 99 available bytes from the limiter was carried over
    TimeBasedLimiter updatedLimiter =
      (TimeBasedLimiter) newCache.get("my_table").getGlobalLimiter();
    assertEquals(99, updatedLimiter.getReadAvailable());
  }

  @Test
  public void testClobberLimiterLimit() throws Exception {
    // establish old cache with a limiter for 100 read bytes per second
    QuotaState oldState = new QuotaState();
    Map<String, QuotaState> oldCache = new HashMap<>();
    oldCache.put("my_table", oldState);
    QuotaProtos.Throttle throttle1 = QuotaProtos.Throttle.newBuilder()
      .setReadSize(QuotaProtos.TimedQuota.newBuilder().setTimeUnit(HBaseProtos.TimeUnit.SECONDS)
        .setSoftLimit(100).setScope(QuotaProtos.QuotaScope.MACHINE).build())
      .build();
    QuotaLimiter limiter1 = TimeBasedLimiter.fromThrottle(conf, throttle1);
    oldState.setGlobalLimiter(limiter1);

    // establish new cache, also with a limiter for 100 read bytes per second
    QuotaState newState = new QuotaState();
    Map<String, QuotaState> newCache = new HashMap<>();
    newCache.put("my_table", newState);
    QuotaProtos.Throttle throttle2 = QuotaProtos.Throttle.newBuilder()
      .setReadSize(QuotaProtos.TimedQuota.newBuilder().setTimeUnit(HBaseProtos.TimeUnit.SECONDS)
        .setSoftLimit(50).setScope(QuotaProtos.QuotaScope.MACHINE).build())
      .build();
    QuotaLimiter limiter2 = TimeBasedLimiter.fromThrottle(conf, throttle2);
    newState.setGlobalLimiter(limiter2);

    // update new cache from old cache
    QuotaCache.updateNewCacheFromOld(oldCache, newCache);

    // verify that the 99 available bytes from the limiter was carried over
    TimeBasedLimiter updatedLimiter =
      (TimeBasedLimiter) newCache.get("my_table").getGlobalLimiter();
    assertEquals(50, updatedLimiter.getReadLimit());
  }

  @Test
  public void testForgetsDeletedQuota() {
    QuotaState oldState = new QuotaState();
    Map<String, QuotaState> oldCache = new HashMap<>();
    oldCache.put("my_table1", oldState);

    QuotaState newState = new QuotaState();
    Map<String, QuotaState> newCache = new HashMap<>();
    newCache.put("my_table2", newState);

    QuotaCache.updateNewCacheFromOld(oldCache, newCache);

    assertTrue(newCache.containsKey("my_table2"));
    assertFalse(newCache.containsKey("my_table1"));
  }

  @Test
  public void testLearnsNewQuota() {
    Map<String, QuotaState> oldCache = new HashMap<>();

    QuotaState newState = new QuotaState();
    Map<String, QuotaState> newCache = new HashMap<>();
    newCache.put("my_table1", newState);

    QuotaCache.updateNewCacheFromOld(oldCache, newCache);

    assertTrue(newCache.containsKey("my_table1"));
  }

  @Test
  public void testUserSpecificOverridesDefaultNewQuota() {
    // establish old cache with a limiter for 100 read bytes per second
    QuotaState oldState = new QuotaState();
    Map<String, QuotaState> oldCache = new HashMap<>();
    oldCache.put("my_table", oldState);
    QuotaProtos.Throttle throttle1 = QuotaProtos.Throttle.newBuilder()
      .setReadSize(QuotaProtos.TimedQuota.newBuilder().setTimeUnit(HBaseProtos.TimeUnit.SECONDS)
        .setSoftLimit(100).setScope(QuotaProtos.QuotaScope.MACHINE).build())
      .build();
    QuotaLimiter limiter1 = TimeBasedLimiter.fromThrottle(conf, throttle1);
    oldState.setGlobalLimiter(limiter1);

    // establish new cache, with a limiter for 999 read bytes per second
    QuotaState newState = new QuotaState();
    Map<String, QuotaState> newCache = new HashMap<>();
    newCache.put("my_table", newState);
    QuotaProtos.Throttle throttle2 = QuotaProtos.Throttle.newBuilder()
      .setReadSize(QuotaProtos.TimedQuota.newBuilder().setTimeUnit(HBaseProtos.TimeUnit.SECONDS)
        .setSoftLimit(999).setScope(QuotaProtos.QuotaScope.MACHINE).build())
      .build();
    QuotaLimiter limiter2 = TimeBasedLimiter.fromThrottle(conf, throttle2);
    newState.setGlobalLimiter(limiter2);

    // update new cache from old cache
    QuotaCache.updateNewCacheFromOld(oldCache, newCache);

    // verify that the 999 available bytes from the limiter was carried over
    TimeBasedLimiter updatedLimiter =
      (TimeBasedLimiter) newCache.get("my_table").getGlobalLimiter();
    assertEquals(999, updatedLimiter.getReadAvailable());
  }

  @Test
  public void testLookupDoesNotWaitForInitialLoad() throws Exception {
    CountDownLatch quotaTableAvailable = new CountDownLatch(1);
    Connection connection = mockConnection(invocation -> {
      quotaTableAvailable.await();
      return emptyQuotaTable();
    });
    QuotaCache quotaCache = startQuotaCache(connection);
    try {
      TableName table = TableName.valueOf("my_table");
      assertTimeoutPreemptively(Duration.ofSeconds(5),
        () -> assertTrue(quotaCache.getTableLimiter(table).isBypass()));
      assertFalse(quotaCache.isInitialized());

      quotaTableAvailable.countDown();
      Waiter.waitFor(conf, 10_000, quotaCache::isInitialized);
    } finally {
      quotaTableAvailable.countDown();
      quotaCache.stop("test");
    }
  }

  @Test
  public void testFailedInitialLoadIsRetried() throws Exception {
    AtomicBoolean quotaTableAvailable = new AtomicBoolean(false);
    Connection connection = mockConnection(invocation -> {
      if (!quotaTableAvailable.get()) {
        throw new IOException("hbase:quota is not available");
      }
      return emptyQuotaTable();
    });
    QuotaCache quotaCache = startQuotaCache(connection);
    try {
      quotaCache.getTableLimiter(TableName.valueOf("my_table"));
      verify(connection, timeout(10_000).atLeast(1)).getTable(any(TableName.class));
      assertFalse(quotaCache.isInitialized());

      quotaTableAvailable.set(true);
      Waiter.waitFor(conf, 10_000, quotaCache::isInitialized);
    } finally {
      quotaCache.stop("test");
    }
  }

  private static QuotaCache startQuotaCache(Connection connection) throws IOException {
    RegionServerServices rsServices = mock(RegionServerServices.class);
    when(rsServices.getConfiguration()).thenReturn(conf);
    when(rsServices.getChoreService()).thenReturn(mock(ChoreService.class));
    when(rsServices.getConnection()).thenReturn(connection);
    QuotaCache quotaCache = new QuotaCache(rsServices);
    quotaCache.start();
    return quotaCache;
  }

  private static Connection mockConnection(Answer<Table> quotaTable) throws IOException {
    Connection connection = mock(Connection.class);
    when(connection.getTable(any(TableName.class))).thenAnswer(quotaTable);
    when(connection.getAdmin()).thenThrow(new IOException("master is not available"));
    return connection;
  }

  private static Table emptyQuotaTable() throws IOException {
    ResultScanner scanner = mock(ResultScanner.class);
    when(scanner.iterator()).thenReturn(Collections.emptyIterator());
    Table table = mock(Table.class);
    when(table.getScanner(any(Scan.class))).thenReturn(scanner);
    when(table.get(any(Get.class))).thenReturn(Result.EMPTY_RESULT);
    return table;
  }
}
