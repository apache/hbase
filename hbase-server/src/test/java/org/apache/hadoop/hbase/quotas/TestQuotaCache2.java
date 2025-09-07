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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;

/**
 * Tests of QuotaCache that don't require a minicluster, unlike in TestQuotaCache
 */
@Category({ RegionServerTests.class, SmallTests.class })
public class TestQuotaCache2 {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestQuotaCache.class);

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
    QuotaLimiter limiter1 = TimeBasedLimiter.fromThrottle(throttle1);
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
    QuotaLimiter limiter2 = TimeBasedLimiter.fromThrottle(throttle2);
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
    QuotaLimiter limiter1 = TimeBasedLimiter.fromThrottle(throttle1);
    oldState.setGlobalLimiter(limiter1);

    // establish new cache, also with a limiter for 100 read bytes per second
    QuotaState newState = new QuotaState();
    Map<String, QuotaState> newCache = new HashMap<>();
    newCache.put("my_table", newState);
    QuotaProtos.Throttle throttle2 = QuotaProtos.Throttle.newBuilder()
      .setReadSize(QuotaProtos.TimedQuota.newBuilder().setTimeUnit(HBaseProtos.TimeUnit.SECONDS)
        .setSoftLimit(50).setScope(QuotaProtos.QuotaScope.MACHINE).build())
      .build();
    QuotaLimiter limiter2 = TimeBasedLimiter.fromThrottle(throttle2);
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
}
