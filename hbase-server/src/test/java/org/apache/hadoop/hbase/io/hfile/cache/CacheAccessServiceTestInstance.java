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
 * Test helper that keeps a concrete cache implementation together with its {@link CacheEngine} and
 * {@link CacheAccessService} views.
 * <p>
 * The concrete cache may be either a legacy {@link BlockCache} or a native {@link CacheEngine}.
 * Tests should normally exercise cache operations through {@link CacheAccessService}, while using
 * the concrete cache or cache engine only for implementation-specific assertions.
 * </p>
 * @param <T> concrete cache implementation type
 */
@InterfaceAudience.Private
public final class CacheAccessServiceTestInstance<T> {

  private final T cache;
  private final BlockCache blockCache;
  private final CacheEngine cacheEngine;
  private final CacheAccessService cacheAccessService;

  CacheAccessServiceTestInstance(T cache) {
    this.cache = Objects.requireNonNull(cache, "cache must not be null");

    if (cache instanceof CacheEngine) {
      this.blockCache = null;
      this.cacheEngine = (CacheEngine) cache;
      this.cacheAccessService = createCacheAccessService(cacheEngine);
    } else if (cache instanceof BlockCache) {
      this.blockCache = (BlockCache) cache;
      this.cacheEngine = CacheEngines.fromBlockCache(blockCache);
      this.cacheAccessService = CacheAccessServices.fromBlockCache(blockCache);
    } else {
      throw new IllegalArgumentException(
        "Unsupported cache implementation: " + cache.getClass().getName());
    }
  }

  /**
   * Creates a single-tier cache access service backed by the specified cache engine.
   * @param cacheEngine cache engine
   * @return cache access service
   */
  private static CacheAccessService createCacheAccessService(CacheEngine cacheEngine) {
    Objects.requireNonNull(cacheEngine, "cacheEngine must not be null");

    CacheTopology topology = new SingleTierTopology(cacheEngine.getName(), cacheEngine);
    return new TopologyBackedCacheAccessService(topology,
      new DefaultHBaseCachePlacementAdmissionPolicy());
  }

  /**
   * Returns the concrete cache implementation supplied to this instance.
   * @return concrete cache implementation
   */
  public T cache() {
    return cache;
  }

  /**
   * Returns the legacy block cache when this instance was constructed from one.
   * @return legacy block cache, or {@code null} for a native cache engine
   */
  public BlockCache blockCache() {
    return blockCache;
  }

  /**
   * Returns the cache engine.
   * <p>
   * For a native cache engine this returns the original engine. For a legacy {@link BlockCache},
   * this returns its cache-engine adapter.
   * </p>
   * @return cache engine
   */
  public CacheEngine cacheEngine() {
    return cacheEngine;
  }

  /**
   * Returns the cache access service.
   * @return cache access service
   */
  public CacheAccessService service() {
    return cacheAccessService;
  }
}
