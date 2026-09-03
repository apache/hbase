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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.ForwardingCache;

@Tag(RegionServerTests.TAG)
@Tag(SmallTests.TAG)
public class TestSyncFutureCache {

  @Test
  public void testSyncFutureCacheLifeCycle() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    SyncFutureCache cache = new SyncFutureCache(conf);
    try {
      SyncFuture future0 = cache.getIfPresentOrNew().reset(0, false);
      assertNotNull(future0);
      // Get another future from the same thread, should be different one.
      SyncFuture future1 = cache.getIfPresentOrNew().reset(1, false);
      assertNotNull(future1);
      assertNotSame(future0, future1);
      cache.offer(future1);
      // Should override.
      cache.offer(future0);
      SyncFuture future3 = cache.getIfPresentOrNew();
      assertEquals(future3, future0);
      final SyncFuture[] future4 = new SyncFuture[1];
      // From a different thread
      CompletableFuture.runAsync(() -> future4[0] = cache.getIfPresentOrNew().reset(4, false))
        .get();
      assertNotNull(future4[0]);
      assertNotSame(future3, future4[0]);
      // Clean up
      cache.offer(future3);
      cache.offer(future4[0]);
    } finally {
      cache.clear();
    }
  }

  @Test
  public void testFallsBackToNewSyncFutureWhenCacheThrows() throws Exception {
    SyncFutureCache cache = new SyncFutureCache(HBaseConfiguration.create());

    final Cache<Thread, SyncFuture> delegate = CacheBuilder.newBuilder().build();
    Cache<Thread, SyncFuture> throwing = new ForwardingCache<Thread, SyncFuture>() {
      @Override
      protected Cache<Thread, SyncFuture> delegate() {
        return delegate;
      }

      @Override
      public ConcurrentMap<Thread, SyncFuture> asMap() {
        throw new NullPointerException("boom");
      }
    };

    Field field = SyncFutureCache.class.getDeclaredField("syncFutureCache");
    field.setAccessible(true);
    field.set(cache, throwing);

    assertNotNull(cache.getIfPresentOrNew());
  }
}
