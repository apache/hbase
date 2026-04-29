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

import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Describes orchestration of one or more cache engines.
 * <p>
 * {@code CacheTopology} is responsible for the structural relationship between cache engines, for
 * example a single-engine cache, a tiered exclusive L1/L2 cache, or a tiered inclusive L1/L2 cache.
 * </p>
 * <p>
 * This abstraction does not own storage. Storage belongs to {@link CacheEngine}. This abstraction
 * also does not decide admission or write placement. Admission, placement, representation, and
 * promotion decisions belong to the policy layer.
 * </p>
 * <p>
 * Responsibilities of a cache topology include:
 * </p>
 * <ul>
 * <li>exposing participating cache engines</li>
 * <li>mapping engines to logical tiers such as L1 and L2</li>
 * <li>providing aggregate cache statistics</li>
 * <li>performing topology-specific promotion when requested by policy</li>
 * <li>optionally performing topology-specific demotion</li>
 * <li>coordinating shutdown of participating engines</li>
 * </ul>
 * <p>
 * Non-responsibilities include:
 * </p>
 * <ul>
 * <li>block storage</li>
 * <li>local eviction algorithms</li>
 * <li>cache admission control</li>
 * <li>write routing / target tier selection</li>
 * <li>HFile read/write path integration</li>
 * </ul>
 */
@InterfaceAudience.Private
public interface CacheTopology {

  /**
   * Returns a human-readable topology name.
   * <p>
   * The name is intended for logging, metrics, diagnostics, and configuration reporting. It should
   * be stable for the lifetime of this topology instance.
   * </p>
   * @return topology name
   */
  String getName();

  /**
   * Returns the topology type.
   * <p>
   * The type identifies the topology family, such as single-engine, tiered exclusive, or tiered
   * inclusive.
   * </p>
   * @return topology type
   */
  CacheTopologyType getType();

  /**
   * Returns the cache engines participating in this topology.
   * <p>
   * The returned list is primarily intended for diagnostics, metrics, and topology inspection.
   * Callers should not use it to bypass topology and policy logic for normal cache operations.
   * </p>
   * @return participating cache engines
   */
  List<CacheEngine> getEngines();

  /**
   * Returns the logical cache tiers defined by this topology.
   * <p>
   * This method describes the structure of the topology in terms of {@link CacheTier} identifiers
   * (for example, {@code SINGLE}, {@code L1}, {@code L2}). The returned list defines which tiers
   * are present and can be used for lookup, placement, and promotion decisions.
   * </p>
   * <p>
   * The ordering of tiers is significant and should reflect lookup priority (for example,
   * {@code L1} before {@code L2}).
   * </p>
   * <p>
   * This method must be explicitly implemented by each topology. Callers must not infer tier
   * structure from {@link #getEngines()} or other properties.
   * </p>
   * @return ordered list of tiers defined by this topology
   */
  List<CacheTier> getTiers();

  /**
   * Returns the cache engine associated with the given logical tier, if one exists.
   * <p>
   * For example, a tiered topology may expose an L1 and L2 engine. A single-engine topology may
   * return an engine for {@link CacheTier#SINGLE} and an empty result for L1/L2.
   * </p>
   * @param tier logical cache tier
   * @return cache engine for the tier, or empty if this topology does not define that tier
   */
  Optional<CacheEngine> getEngine(CacheTier tier);

  /**
   * Returns aggregate topology-level cache statistics.
   * <p>
   * For a single-engine topology, this may simply return the underlying engine statistics. For a
   * multi-engine topology, this should represent an aggregate view suitable for compatibility with
   * existing HBase block cache metrics.
   * </p>
   * @return aggregate cache statistics
   */
  CacheStats getStats();

  /**
   * Promotes a cached block from one engine to another.
   * <p>
   * This method performs the topology-specific mechanics of promotion. The decision whether a block
   * should be promoted belongs to the placement/admission policy layer. For example, a policy may
   * decide that an index block found in L2 should be promoted to L1, and then call this method to
   * perform the promotion.
   * </p>
   * <p>
   * Implementations may either copy or move the block depending on topology semantics. For example:
   * </p>
   * <ul>
   * <li>inclusive may copy the block into the target tier while retaining it in source</li>
   * <li>exclusive may move the block into the target tier and remove it from source</li>
   * </ul>
   * @param cacheKey     block cache key
   * @param block        cached block to promote
   * @param sourceEngine engine where the block was found
   * @param targetEngine engine where the block should be promoted
   * @return {@code true} if promotion was performed, {@code false} otherwise
   */
  boolean promote(BlockCacheKey cacheKey, Cacheable block, CacheEngine sourceEngine,
    CacheEngine targetEngine);

  /**
   * Demotes a cached block from one engine to another.
   * <p>
   * Demotion is optional because not all topologies or cache engines can support it efficiently. In
   * many implementations, eviction does not expose the evicted block in a form that can be cheaply
   * demoted. The default implementation therefore performs no action.
   * </p>
   * <p>
   * The decision whether demotion should happen belongs to the policy layer or to a
   * topology-specific eviction callback mechanism. This method only provides a standard hook for
   * topologies that support demotion.
   * </p>
   * @param cacheKey     block cache key
   * @param block        cached block to demote
   * @param sourceEngine engine from which the block is being demoted
   * @param targetEngine engine where the block should be demoted
   * @return {@code true} if demotion was performed, {@code false} otherwise
   */
  default boolean demote(BlockCacheKey cacheKey, Cacheable block, CacheEngine sourceEngine,
    CacheEngine targetEngine) {
    return false;
  }

  /**
   * Shuts down this topology and any cache engines owned by it.
   * <p>
   * If the topology does not own the life cycle of its engines, the implementation should document
   * that behavior. The default expectation is that shutting down a topology shuts down
   * participating engines.
   * </p>
   */
  void shutdown();

  /**
   * Returns a read-only view of this topology.
   * <p>
   * The returned view is intended for policy implementations, diagnostics, and metrics. It should
   * expose topology and engine state without allowing callers to mutate cache contents or bypass
   * topology behavior.
   * </p>
   * @return read-only topology view
   */
  CacheTopologyView getView();
}
