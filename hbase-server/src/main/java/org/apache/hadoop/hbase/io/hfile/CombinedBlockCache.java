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

import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * CombinedBlockCache is an abstraction layer that combines
 * {@link FirstLevelBlockCache} and {@link BucketCache}. The smaller lruCache is used
 * to cache bloom blocks and index blocks.  The larger Cache is used to
 * cache data blocks. {@link #getBlock(BlockCacheKey, boolean, boolean, boolean)} reads
 * first from the smaller l1Cache before looking for the block in the l2Cache.  Blocks evicted
 * from l1Cache are put into the bucket cache.
 * Metrics are the combined size and hits and misses of both caches.
 */
@InterfaceAudience.Private
public class CombinedBlockCache extends CompositeBlockCache implements ResizableBlockCache {

  public CombinedBlockCache(FirstLevelBlockCache l1Cache, BlockCache l2Cache) {
    super(l1Cache, l2Cache);
  }

  @Override
  public void setMaxSize(long size) {
    ((FirstLevelBlockCache) l1Cache).setMaxSize(size);
  }

  @VisibleForTesting
  public int getRpcRefCount(BlockCacheKey cacheKey) {
    return (this.l2Cache instanceof BucketCache)
        ? ((BucketCache) this.l2Cache).getRpcRefCount(cacheKey)
        : 0;
  }

  public FirstLevelBlockCache getFirstLevelCache() {
    return (FirstLevelBlockCache) l1Cache;
  }
}
