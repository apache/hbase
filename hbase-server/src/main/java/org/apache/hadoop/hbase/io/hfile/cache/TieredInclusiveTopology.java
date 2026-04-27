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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Tiered inclusive cache topology.
 *
 * <p>In an inclusive topology, a block promoted from L2 to L1 may remain in L2. Promotion is
 * therefore modeled as a copy rather than a move.</p>
 *
 * <p>This class is introduced as a topology foundation. Production wiring and policy-driven
 * routing are handled in later migration phases.</p>
 */
@InterfaceAudience.Private
public class TieredInclusiveTopology implements CacheTopology {

  private final String name;
  private final CacheEngine l1;
  private final CacheEngine l2;
  private final CacheTopologyView view;

  public TieredInclusiveTopology(String name, CacheEngine l1, CacheEngine l2) {
    this.name = name;
    this.l1 = l1;
    this.l2 = l2;
    this.view = new CacheTopologyView(this);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public CacheTopologyType getType() {
    return CacheTopologyType.TIERED_INCLUSIVE;
  }

  @Override
  public List<CacheEngine> getEngines() {
    return Arrays.asList(l1, l2);
  }

  @Override
  public Optional<CacheEngine> getEngine(CacheTier tier) {
    switch (tier) {
      case L1:
        return Optional.of(l1);
      case L2:
        return Optional.of(l2);
      default:
        return Optional.empty();
    }
  }

  @Override
  public CacheTopologyView getView() {
    return view;
  }

  @Override
  public CacheStats getStats() {
    // TODO: replace with aggregate topology stats in follow-up metrics ticket.
    return l1.getStats();
  }

  @Override
  public boolean promote(BlockCacheKey cacheKey, Cacheable block, CacheEngine sourceEngine,
    CacheEngine targetEngine) {
    if (targetEngine == null || block == null) {
      return false;
    }

    targetEngine.cacheBlock(cacheKey, block);
    return true;
  }

  @Override
  public void shutdown() {
    l1.shutdown();
    l2.shutdown();
  }
}