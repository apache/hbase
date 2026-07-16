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
 * Factory helpers for {@link CacheEngine} instances.
 * <p>
 * These helpers are intended to keep transitional wiring concise while existing cache
 * implementations are still based on {@link BlockCache}. Once built-in caches implement
 * {@link CacheEngine} directly, callers can construct or obtain those engines without going through
 * a legacy adapter.
 * </p>
 */
@InterfaceAudience.Private
public final class CacheEngines {

  private CacheEngines() {
  }

  /**
   * Wraps an existing {@link BlockCache} as a {@link CacheEngine}.
   * @param blockCache block cache to adapt
   * @return cache engine backed by the supplied block cache
   */
  public static CacheEngine fromBlockCache(BlockCache blockCache) {
    Objects.requireNonNull(blockCache, "blockCache must not be null");
    return new BlockCacheBackedCacheEngine(blockCache);
  }
}
