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
import java.util.stream.Collectors;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Default immutable-style read-only view over a {@link CacheTopology}.
 *
 * <p>This view delegates to the topology and exposes only read-only engine views.</p>
 */
@InterfaceAudience.Private
final class CacheTopologyView  {

  private final CacheTopology topology;

  /** 
   * Constructs a CacheTopologyView for the given CacheTopology. 
   */
  CacheTopologyView(CacheTopology topology) {
    this.topology = topology;
  }

  /** 
   * Delegating getters for topology properties and read-only engine views. 
   */
  
  public String getName() {
    return topology.getName();
  }
  /**
   * Returns the cache topology type, which can be SINGLE, L1_L2, or CUSTOM.
   * @return the cache topology type
   */
  public CacheTopologyType getType() {
    return topology.getType();
  }

  /**
   * Returns the list of cache tiers in this topology. For a single-tier topology, it returns a list
   * containing only CacheTier.SINGLE. For a multi-tier topology, it returns a list containing
   * CacheTier.L1 and CacheTier.L2.
   * @return the list of cache tiers in this topology
   */
  public List<CacheTier> getTiers() {
    return topology.getEngines().size() == 1
      ? List.of(CacheTier.SINGLE)
      : List.of(CacheTier.L1, CacheTier.L2);
  }

  /** 
   * Returns an Optional containing a CacheEngineView for the specified cache tier if it exists in
   * the topology, or an empty Optional if the tier is not present.
   * @param tier the cache tier for which to retrieve the engine view
   * @return an Optional containing the CacheEngineView for the specified tier, or empty if not present
   */
  public Optional<CacheEngineView> getEngine(CacheTier tier) {
    return topology.getEngine(tier).map(CacheEngineView::new);
  }

  /**
   * Returns a list of CacheEngineView objects representing all cache engines in this topology.
   * @return a list of CacheEngineView objects for all engines in this topology
   */
  public List<CacheEngineView> getEngines() {
    return topology.getEngines().stream().map(CacheEngineView::new)
      .collect(Collectors.toList());
  }
}