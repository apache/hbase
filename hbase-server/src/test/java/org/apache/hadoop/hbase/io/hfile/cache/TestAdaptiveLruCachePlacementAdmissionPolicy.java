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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestAdaptiveLruCachePlacementAdmissionPolicy {

  private static final String HEAVY_EVICTION_COUNT_LIMIT =
    "hbase.lru.cache.heavy.eviction.count.limit";

  private static final String HEAVY_EVICTION_MB_SIZE_LIMIT =
    "hbase.lru.cache.heavy.eviction.mb.size.limit";

  private static final String HEAVY_EVICTION_OVERHEAD_COEFFICIENT =
    "hbase.lru.cache.heavy.eviction.overhead.coefficient";

  private static final long MB = 1024L * 1024L;

  private static final long EVALUATION_PERIOD_MS = 10_000L;

  private ManualEnvironmentEdge environmentEdge;

  private long currentTime;

  /**
   * Installs a manually controlled clock for AdaptiveLRU evaluation windows.
   */
  @BeforeEach
  void setUp() {
    currentTime = 1_000L;
    environmentEdge = new ManualEnvironmentEdge();
    environmentEdge.setValue(currentTime);
    EnvironmentEdgeManager.injectEdge(environmentEdge);
  }

  /**
   * Restores the normal environment clock after each test.
   */
  @AfterEach
  void tearDown() {
    EnvironmentEdgeManager.reset();
  }

  /**
   * Verifies that AdaptiveLRU initially admits all data blocks.
   */
  @Test
  void testInitialDataBlockAdmissionIsOneHundredPercent() {
    AdaptiveLruCachePlacementAdmissionPolicy policy = createPolicy(0, 200L, 0.01f);

    assertEquals(100, policy.getCacheDataBlockPercent());
  }

  /**
   * Verifies that sustained eviction pressure reduces data-block admission.
   */
  @Test
  void testHeavyEvictionPressureReducesDataBlockAdmission() {
    AdaptiveLruCachePlacementAdmissionPolicy policy = createPolicy(0, 200L, 0.01f);

    applyPressure(policy, CacheTier.SINGLE, singleTierView(), 3_200L);

    assertEquals(85, policy.getCacheDataBlockPercent());
  }

  /**
   * Verifies that the configured heavy-eviction count delays adaptive admission changes.
   */
  @Test
  void testHeavyEvictionCountLimitDelaysAdaptation() {
    AdaptiveLruCachePlacementAdmissionPolicy policy = createPolicy(1, 200L, 0.01f);
    CacheTopologyView topologyView = singleTierView();

    applyPressure(policy, CacheTier.SINGLE, topologyView, 3_200L);
    assertEquals(100, policy.getCacheDataBlockPercent());

    applyPressure(policy, CacheTier.SINGLE, topologyView, 3_200L);
    assertEquals(85, policy.getCacheDataBlockPercent());
  }

  /**
   * Verifies that admission increases again when eviction pressure decreases.
   */
  @Test
  void testAdmissionRecoversWhenEvictionPressureDrops() {
    AdaptiveLruCachePlacementAdmissionPolicy policy = createPolicy(0, 200L, 0.01f);
    CacheTopologyView topologyView = singleTierView();

    applyPressure(policy, CacheTier.SINGLE, topologyView, 3_200L);
    assertEquals(85, policy.getCacheDataBlockPercent());

    applyPressure(policy, CacheTier.SINGLE, topologyView, 20L);
    assertEquals(95, policy.getCacheDataBlockPercent());

    applyPressure(policy, CacheTier.SINGLE, topologyView, 0L);
    assertEquals(100, policy.getCacheDataBlockPercent());
  }

  /**
   * Verifies that pressure from L2 does not affect AdaptiveLRU admission.
   */
  @Test
  void testSecondLevelEvictionPressureIsIgnored() {
    AdaptiveLruCachePlacementAdmissionPolicy policy = createPolicy(0, 200L, 0.01f);

    applyPressure(policy, CacheTier.L2, tieredExclusiveView(), 3_200L);

    assertEquals(100, policy.getCacheDataBlockPercent());
  }

  /**
   * Verifies the deterministic offset-based data-block admission filter for a single-tier cache.
   */
  @Test
  void testSingleTierDataBlockAdmissionUsesBlockOffset() {
    AdaptiveLruCachePlacementAdmissionPolicy policy = createPolicy(0, 200L, 0.01f);
    CacheTopologyView topologyView = singleTierView();
    Cacheable dataBlock = block(BlockType.DATA);
    CacheWriteContext context = mock(CacheWriteContext.class);

    applyPressure(policy, CacheTier.SINGLE, topologyView, 3_200L);
    assertEquals(85, policy.getCacheDataBlockPercent());

    AdmissionDecision admitted = policy.shouldAdmit(new BlockCacheKey("file", 84L), dataBlock,
      context, AdmissionPriority.NORMAL, topologyView);

    AdmissionDecision rejected = policy.shouldAdmit(new BlockCacheKey("file", 85L), dataBlock,
      context, AdmissionPriority.NORMAL, topologyView);

    assertTrue(admitted.isAdmitted());
    assertFalse(rejected.isAdmitted());
  }

  /**
   * Verifies that AdaptiveLRU does not filter metadata blocks.
   */
  @Test
  void testMetadataBlocksAreNotAdaptivelyRejected() {
    AdaptiveLruCachePlacementAdmissionPolicy policy = createPolicy(0, 200L, 0.01f);
    CacheTopologyView topologyView = singleTierView();
    Cacheable indexBlock = block(BlockType.ROOT_INDEX);
    CacheWriteContext context = mock(CacheWriteContext.class);

    applyPressure(policy, CacheTier.SINGLE, topologyView, 3_200L);

    AdmissionDecision decision = policy.shouldAdmit(new BlockCacheKey("file", 99L), indexBlock,
      context, AdmissionPriority.NORMAL, topologyView);

    assertTrue(decision.isAdmitted());
  }

  /**
   * Verifies that inclusive topology removes only L1 when AdaptiveLRU rejects a data block there.
   */
  @Test
  void testInclusiveTopologyRetainsL2WhenDataBlockIsRejectedFromL1() {
    AdaptiveLruCachePlacementAdmissionPolicy policy = createPolicy(0, 200L, 0.01f);
    CacheTopologyView topologyView = tieredInclusiveView();
    Cacheable dataBlock = block(BlockType.DATA);
    CacheWriteContext context = mock(CacheWriteContext.class);

    applyPressure(policy, CacheTier.L1, topologyView, 3_200L);
    assertEquals(85, policy.getCacheDataBlockPercent());

    BlockCacheKey cacheKey = new BlockCacheKey("file", 99L);

    AdmissionDecision admission =
      policy.shouldAdmit(cacheKey, dataBlock, context, AdmissionPriority.NORMAL, topologyView);
    TierDecision placement = policy.selectTier(cacheKey, dataBlock, context, topologyView);

    assertTrue(admission.isAdmitted());
    assertEquals(List.of(CacheTier.L2), placement.getTiers());
  }

  /**
   * Verifies that exclusive topology continues to place data blocks in L2 even when AdaptiveLRU
   * would reject them from L1.
   */
  @Test
  void testExclusiveTopologyKeepsDefaultDataPlacementInL2() {
    AdaptiveLruCachePlacementAdmissionPolicy policy = createPolicy(0, 200L, 0.01f);
    CacheTopologyView topologyView = tieredExclusiveView();
    Cacheable dataBlock = block(BlockType.DATA);
    CacheWriteContext context = mock(CacheWriteContext.class);

    applyPressure(policy, CacheTier.L1, topologyView, 3_200L);
    assertEquals(85, policy.getCacheDataBlockPercent());

    BlockCacheKey cacheKey = new BlockCacheKey("file", 99L);

    AdmissionDecision admission =
      policy.shouldAdmit(cacheKey, dataBlock, context, AdmissionPriority.NORMAL, topologyView);
    TierDecision placement = policy.selectTier(cacheKey, dataBlock, context, topologyView);

    assertTrue(admission.isAdmitted());
    assertEquals(List.of(CacheTier.L2), placement.getTiers());
  }

  /**
   * Creates an AdaptiveLRU policy with explicitly controlled adaptation parameters.
   * @param heavyEvictionCountLimit          number of heavy periods tolerated before adaptation
   * @param heavyEvictionMbSizeLimit         target eviction volume in megabytes
   * @param heavyEvictionOverheadCoefficient coefficient controlling admission adjustment speed
   * @return configured AdaptiveLRU policy
   */
  private AdaptiveLruCachePlacementAdmissionPolicy createPolicy(int heavyEvictionCountLimit,
    long heavyEvictionMbSizeLimit, float heavyEvictionOverheadCoefficient) {
    Configuration conf = new Configuration(false);
    conf.setInt(HEAVY_EVICTION_COUNT_LIMIT, heavyEvictionCountLimit);
    conf.setLong(HEAVY_EVICTION_MB_SIZE_LIMIT, heavyEvictionMbSizeLimit);
    conf.setFloat(HEAVY_EVICTION_OVERHEAD_COEFFICIENT, heavyEvictionOverheadCoefficient);
    return new AdaptiveLruCachePlacementAdmissionPolicy(conf);
  }

  /**
   * Advances one evaluation period and reports the specified eviction pressure.
   * @param policy       policy receiving the pressure event
   * @param sourceTier   tier reporting pressure
   * @param topologyView topology view associated with the policy
   * @param pressureMb   cache pressure in megabytes
   */
  private void applyPressure(AdaptiveLruCachePlacementAdmissionPolicy policy, CacheTier sourceTier,
    CacheTopologyView topologyView, long pressureMb) {
    currentTime += EVALUATION_PERIOD_MS;
    environmentEdge.setValue(currentTime);
    policy.onEvictionPressure(sourceTier, pressureMb * MB, topologyView);
  }

  /**
   * Creates a mocked single-tier topology view.
   * @return single-tier topology view
   */
  private CacheTopologyView singleTierView() {
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    when(topologyView.getType()).thenReturn(CacheTopologyType.SINGLE_TIER);
    return topologyView;
  }

  /**
   * Creates a mocked tiered-inclusive topology view.
   * @return tiered-inclusive topology view
   */
  private CacheTopologyView tieredInclusiveView() {
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    when(topologyView.getType()).thenReturn(CacheTopologyType.TIERED_INCLUSIVE);
    return topologyView;
  }

  /**
   * Creates a mocked tiered-exclusive topology view with both tiers available.
   * @return tiered-exclusive topology view
   */
  private CacheTopologyView tieredExclusiveView() {
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CacheEngineView l1 = mock(CacheEngineView.class);
    CacheEngineView l2 = mock(CacheEngineView.class);

    when(topologyView.getType()).thenReturn(CacheTopologyType.TIERED_EXCLUSIVE);
    when(topologyView.getEngine(CacheTier.L1)).thenReturn(Optional.of(l1));
    when(topologyView.getEngine(CacheTier.L2)).thenReturn(Optional.of(l2));
    return topologyView;
  }

  /**
   * Creates a cacheable block with the specified type.
   * @param blockType block type
   * @return mocked cacheable block
   */
  private Cacheable block(BlockType blockType) {
    Cacheable block = mock(Cacheable.class);
    when(block.getBlockType()).thenReturn(blockType);
    return block;
  }
}
