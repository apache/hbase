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

import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Listener for cache-engine eviction events.
 * <p>
 * The listener is intended for topology-level handling of capacity-driven evictions. In particular,
 * an exclusive tiered topology may use an eviction from L1 as a request to demote the block to L2.
 * </p>
 * <p>
 * The supplied block remains valid for the duration of the callback. An implementation that needs
 * to retain the block after the callback returns must establish its own reference.
 * </p>
 */
@InterfaceAudience.Private
public interface CacheEvictionListener {

  /**
   * Handles a block evicted by a cache engine because of cache pressure.
   * @param sourceEngine engine that evicted the block
   * @param cacheKey     key identifying the evicted block
   * @param block        evicted block
   */
  void onEviction(CacheEngine sourceEngine, BlockCacheKey cacheKey, Cacheable block);
}
