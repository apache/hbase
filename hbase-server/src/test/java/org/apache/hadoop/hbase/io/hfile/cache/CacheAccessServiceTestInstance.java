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
package org.apache.hadoop.hbase.io.hfile.cache;

import java.util.Objects;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Test helper that keeps a concrete {@link BlockCache} together with the {@link CacheAccessService}
 * facade backed by that cache.
 * <p>
 * This is useful for tests that need to exercise production code through
 * {@link CacheAccessService}, while still inspecting implementation-specific state through the
 * concrete cache instance.
 * </p>
 * @param <T> concrete block cache type
 */
@InterfaceAudience.Private
public final class CacheAccessServiceTestInstance<T extends BlockCache> {

  private final T blockCache;
  private final CacheAccessService cacheAccessService;

  CacheAccessServiceTestInstance(T blockCache) {
    this.blockCache = Objects.requireNonNull(blockCache, "blockCache must not be null");
    this.cacheAccessService = CacheAccessServices.fromBlockCache(blockCache);
  }

  /**
   * Returns the concrete block cache instance.
   * <p>
   * Tests should use this only when they need implementation-specific inspection or assertions.
   * Normal cache access should go through {@link #service()}.
   * </p>
   * @return concrete block cache
   */
  public T blockCache() {
    return blockCache;
  }

  /**
   * Returns the {@link CacheAccessService} facade backed by the concrete block cache.
   * @return cache access service
   */
  public CacheAccessService service() {
    return cacheAccessService;
  }
}
