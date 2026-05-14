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
 * Utility methods for creating {@link CacheAccessService} instances.
 * <p>
 * This class keeps service construction centralized without introducing a full factory or plugin
 * loader in the initial {@code CacheAccessService} ticket. The first supported construction modes
 * are a legacy {@link BlockCache}-backed service, a topology-backed service, and a disabled no-op
 * service.
 * </p>
 * <p>
 * A later integration step can move construction into {@code BlockCacheFactory} once HBase runtime
 * wiring starts returning {@link CacheAccessService} instead of, or alongside, raw
 * {@link BlockCache}.
 * </p>
 */
@InterfaceAudience.Private
public final class CacheAccessServices {

  private CacheAccessServices() {
  }

  /**
   * Creates a {@link CacheAccessService} backed by an existing {@link BlockCache}.
   * <p>
   * This is the default compatibility path during migration from {@code BlockCache} to
   * {@code CacheAccessService}. The returned service delegates to the supplied block cache and
   * should preserve existing behavior.
   * </p>
   * @param blockCache block cache to wrap
   * @return cache access service backed by {@code blockCache}
   */
  public static CacheAccessService fromBlockCache(BlockCache blockCache) {
    return new BlockCacheBackedCacheAccessService(
      Objects.requireNonNull(blockCache, "blockCache must not be null"));
  }

  /**
   * Creates a {@link CacheAccessService} backed by a {@link CacheTopology} and placement/admission
   * policy.
   * <p>
   * The returned service uses the topology to resolve participating tiers and engines, uses the
   * policy to make admission, placement, representation, and promotion decisions, and executes
   * storage operations through {@link CacheEngine}.
   * </p>
   * @param topology cache topology
   * @param policy   placement and admission policy
   * @return topology-backed cache access service
   */
  public static CacheAccessService fromTopology(CacheTopology topology,
    CachePlacementAdmissionPolicy policy) {
    return new TopologyBackedCacheAccessService(
      Objects.requireNonNull(topology, "topology must not be null"),
      Objects.requireNonNull(policy, "policy must not be null"));
  }

  /**
   * Creates a disabled no-op {@link CacheAccessService}.
   * <p>
   * The returned service never stores blocks and always reports zero capacity and occupancy. It is
   * useful for callers that prefer a non-null service object even when block cache is disabled.
   * </p>
   * @return disabled no-op cache access service
   */
  public static CacheAccessService disabled() {
    return new NoOpCacheAccessService();
  }
}
