/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class InclusiveCombinedBlockCache extends CombinedBlockCache implements BlockCache {
  public InclusiveCombinedBlockCache(FirstLevelBlockCache l1, BlockCache l2) {
    super(l1,l2);
  }

  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching,
                            boolean repeat, boolean updateCacheMetrics) {
    // On all external cache set ups the lru should have the l2 cache set as the victimHandler
    // Because of that all requests that miss inside of the lru block cache will be
    // tried in the l2 block cache.
    return l1Cache.getBlock(cacheKey, caching, repeat, updateCacheMetrics);
  }

  /**
   *
   * @param cacheKey The block's cache key.
   * @param buf The block contents wrapped in a ByteBuffer.
   * @param inMemory Whether block should be treated as in-memory. This parameter is only useful for
   *                 the L1 lru cache.
   * @param cacheDataInL1 This is totally ignored.
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory,
                         final boolean cacheDataInL1) {
    // This is the inclusive part of the combined block cache.
    // Every block is placed into both block caches.
    l1Cache.cacheBlock(cacheKey, buf, inMemory, true);

    // This assumes that insertion into the L2 block cache is either async or very fast.
    l2Cache.cacheBlock(cacheKey, buf, inMemory, true);
  }
}
