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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Single-engine cache topology.
 * <p>
 * This topology wraps a single {@link CacheEngine}. It is primarily useful as a baseline topology
 * and as a simple bridge for cache configurations that do not use L1/L2 tiering.
 * </p>
 */
@InterfaceAudience.Private
public class SingleEngineTopology implements CacheTopology {

  private final String name;
  private final CacheEngine engine;
  private final CacheTopologyView view;

  public SingleEngineTopology(String name, CacheEngine engine) {
    this.name = name;
    this.engine = engine;
    this.view = new CacheTopologyView(this);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public CacheTopologyType getType() {
    return CacheTopologyType.SINGLE;
  }

  @Override
  public List<CacheEngine> getEngines() {
    return Collections.singletonList(engine);
  }

  @Override
  public Optional<CacheEngine> getEngine(CacheTier tier) {
    return tier == CacheTier.SINGLE ? Optional.of(engine) : Optional.empty();
  }

  @Override
  public CacheTopologyView getView() {
    return view;
  }

  @Override
  public CacheStats getStats() {
    return engine.getStats();
  }

  @Override
  public boolean promote(BlockCacheKey cacheKey, Cacheable block, CacheEngine sourceEngine,
    CacheEngine targetEngine) {
    return false;
  }

  @Override
  public void shutdown() {
    engine.shutdown();
  }

  @Override
  public List<CacheTier> getTiers() {
    return Collections.singletonList(CacheTier.SINGLE);
  }
}
