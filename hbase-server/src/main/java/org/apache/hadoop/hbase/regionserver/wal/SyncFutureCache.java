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

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;

/**
 * A cache of {@link SyncFuture}s.  This class supports two methods
 * {@link SyncFutureCache#getIfPresentOrNew()} and {@link SyncFutureCache#offer(SyncFuture)}.
 *
 * Usage pattern:
 *   SyncFuture sf = syncFutureCache.getIfPresentOrNew();
 *   sf.reset(...);
 *   // Use the sync future
 *   finally: syncFutureCache.offer(sf);
 *
 * Offering the sync future back to the cache makes it eligible for reuse within the same thread
 * context. Cache keyed by the accessing thread instance and automatically invalidated if it remains
 * unused for {@link SyncFutureCache#SYNC_FUTURE_INVALIDATION_TIMEOUT_MINS} minutes.
 */
@InterfaceAudience.Private
public final class SyncFutureCache {

  private static final long SYNC_FUTURE_INVALIDATION_TIMEOUT_MINS = 2;

  private final Cache<Thread, SyncFuture> syncFutureCache;

  public SyncFutureCache(final Configuration conf) {
    final int handlerCount = conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
        HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT);
    syncFutureCache = CacheBuilder.newBuilder().initialCapacity(handlerCount)
        .expireAfterWrite(SYNC_FUTURE_INVALIDATION_TIMEOUT_MINS, TimeUnit.MINUTES).build();
  }

  public SyncFuture getIfPresentOrNew() {
    // Invalidate the entry if a mapping exists. We do not want it to be reused at the same time.
    SyncFuture future = syncFutureCache.asMap().remove(Thread.currentThread());
    return (future == null) ? new SyncFuture() : future;
  }

  /**
   * Offers the sync future back to the cache for reuse.
   */
  public void offer(SyncFuture syncFuture) {
    // It is ok to overwrite an existing mapping.
    syncFutureCache.asMap().put(syncFuture.getThread(), syncFuture);
  }

  public void clear() {
    if (syncFutureCache != null) {
      syncFutureCache.invalidateAll();
    }
  }
}