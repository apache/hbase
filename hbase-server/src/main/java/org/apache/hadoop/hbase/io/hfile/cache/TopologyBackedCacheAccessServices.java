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
 * Factory helpers for topology-backed {@link CacheAccessService} instances.
 * <p>
 * These helpers are intended for transitional wiring while existing cache implementations still
 * expose the legacy {@link BlockCache} API. The supplied block caches are adapted to
 * {@link CacheEngine} using {@link BlockCacheBackedCacheEngine}, assembled into a
 * {@link TieredExclusiveTopology}, and exposed through {@link TopologyBackedCacheAccessService}.
 * </p>
 * <p>
 * This class does not change production cache wiring by itself. It only provides a reusable
 * construction path for tests and later migration steps that need a CombinedBlockCache-compatible
 * topology-backed service.
 * </p>
 */
@InterfaceAudience.Private
public final class TopologyBackedCacheAccessServices {

  private TopologyBackedCacheAccessServices() {
  }

  /**
   * Creates a topology-backed cache access service from existing L1 and L2 block caches.
   * <p>
   * The resulting service uses {@link TieredExclusiveTopology}, which models the current
   * CombinedBlockCache-compatible L1/L2 behavior where promotion can move a block from one tier to
   * another.
   * </p>
   * @param name   human-readable topology/service name
   * @param l1     L1 block cache
   * @param l2     L2 block cache
   * @param policy placement and admission policy
   * @return topology-backed cache access service
   */
  public static TopologyBackedCacheAccessService fromTieredExclusiveBlockCaches(String name,
    BlockCache l1, BlockCache l2, CachePlacementAdmissionPolicy policy) {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(l1, "l1 must not be null");
    Objects.requireNonNull(l2, "l2 must not be null");
    Objects.requireNonNull(policy, "policy must not be null");

    CacheEngine l1Engine = CacheEngines.fromBlockCache(l1);
    CacheEngine l2Engine = CacheEngines.fromBlockCache(l2);
    CacheTopology topology = new TieredExclusiveTopology(name, l1Engine, l2Engine);
    return new TopologyBackedCacheAccessService(topology, policy);
  }
}
