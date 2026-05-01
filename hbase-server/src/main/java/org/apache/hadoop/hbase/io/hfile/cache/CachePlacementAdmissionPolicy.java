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
 * Policy interface for cache admission, tier placement, representation, and promotion.
 * <p>
 * This policy decides what should happen for a cache operation. It does not execute cache
 * operations directly. Execution belongs to {@link CacheTopology} and storage belongs to
 * {@link CacheEngine}.
 * </p>
 * <p>
 * The policy may consult {@link CacheTopologyView} to inspect available tiers and engine capacity,
 * but it must not mutate cache state.
 * </p>
 */
@InterfaceAudience.Private
public interface CachePlacementAdmissionPolicy {

  /**
   * Decides whether a block should be admitted into the cache.
   * @param cacheKey     block cache key
   * @param block        block to cache
   * @param context      write context describing insertion source and hints
   * @param priority     admission priority hint
   * @param topologyView read-only topology view
   * @return admission decision
   */
  AdmissionDecision shouldAdmit(BlockCacheKey cacheKey, Cacheable block, CacheWriteContext context,
    AdmissionPriority priority, CacheTopologyView topologyView);

  /**
   * Selects the target tier for an admitted block.
   * @param cacheKey     block cache key
   * @param block        block to cache
   * @param context      write context describing insertion source and hints
   * @param topologyView read-only topology view
   * @return tier decision
   */
  TierDecision selectTier(BlockCacheKey cacheKey, Cacheable block, CacheWriteContext context,
    CacheTopologyView topologyView);

  /**
   * Selects the cached representation for an admitted block.
   * <p>
   * The initial compatibility policy should preserve the representation currently produced by
   * existing HBase code paths.
   * </p>
   * @param cacheKey     block cache key
   * @param block        block to cache
   * @param context      write context describing insertion source and hints
   * @param topologyView read-only topology view
   * @return representation decision
   */
  RepresentationDecision selectRepresentation(BlockCacheKey cacheKey, Cacheable block,
    CacheWriteContext context, CacheTopologyView topologyView);

  /**
   * Decides whether a cache hit should trigger promotion to another tier.
   * @param cacheKey     block cache key
   * @param block        cached block
   * @param sourceTier   tier where the block was found
   * @param context      read request context
   * @param topologyView read-only topology view
   * @return promotion decision
   */
  PromotionDecision shouldPromote(BlockCacheKey cacheKey, Cacheable block, CacheTier sourceTier,
    CacheRequestContext context, CacheTopologyView topologyView);
}
