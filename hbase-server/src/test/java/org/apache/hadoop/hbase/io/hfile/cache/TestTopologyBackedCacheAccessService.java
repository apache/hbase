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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TopologyBackedCacheAccessService}.
 */
@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestTopologyBackedCacheAccessService {
  private static final String HFILE_NAME = "file";

  private static final long BLOCK_OFFSET = 1L;

  private static final long RANGE_START_OFFSET = 10L;

  private static final long RANGE_END_OFFSET = 100L;

  /**
   * Verifies that lookup checks topology tiers in order and returns the first cached block found.
   */
  @Test
  void testGetBlockChecksTiersInOrder() {
    BlockCacheKey key = new BlockCacheKey("file", BLOCK_OFFSET);
    Cacheable block = mock(Cacheable.class);
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine l1 = mock(CacheEngine.class);
    CacheEngine l2 = mock(CacheEngine.class);
    CacheRequestContext context = requestContext();

    when(topology.getName()).thenReturn("tiered");
    when(topology.getView()).thenReturn(topologyView);
    when(topology.getTiers()).thenReturn(List.of(CacheTier.L1, CacheTier.L2));
    when(topology.getEngine(CacheTier.L1)).thenReturn(Optional.of(l1));
    when(topology.getEngine(CacheTier.L2)).thenReturn(Optional.of(l2));
    when(l1.getBlock(key, true, false, true, BlockType.DATA)).thenReturn(null);
    when(l2.getBlock(key, true, false, true, BlockType.DATA)).thenReturn(block);
    when(policy.shouldPromote(key, block, CacheTier.L2, context, topologyView))
      .thenReturn(PromotionDecision.none());

    CacheAccessService service = new TopologyBackedCacheAccessService(topology, policy);

    assertSame(block, service.getBlock(key, context));

    verify(l1).getBlock(key, true, false, true, BlockType.DATA);
    verify(l2).getBlock(key, true, false, true, BlockType.DATA);
    verify(policy).shouldPromote(key, block, CacheTier.L2, context, topologyView);
  }

  /**
   * Verifies that a hit can trigger topology-level promotion when requested by policy.
   */
  @Test
  void testGetBlockPromotesOnPolicyDecision() {
    BlockCacheKey key = new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET);
    Cacheable block = mock(Cacheable.class);
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine l1 = mock(CacheEngine.class);
    CacheEngine l2 = mock(CacheEngine.class);
    CacheRequestContext context = requestContext();

    when(topology.getView()).thenReturn(topologyView);
    when(topology.getTiers()).thenReturn(List.of(CacheTier.L1, CacheTier.L2));
    when(topology.getEngine(CacheTier.L1)).thenReturn(Optional.of(l1));
    when(topology.getEngine(CacheTier.L2)).thenReturn(Optional.of(l2));
    when(l1.getBlock(key, true, false, true, BlockType.DATA)).thenReturn(null);
    when(l2.getBlock(key, true, false, true, BlockType.DATA)).thenReturn(block);
    when(policy.shouldPromote(key, block, CacheTier.L2, context, topologyView))
      .thenReturn(PromotionDecision.promoteTo(CacheTier.L1, false));
    when(topology.promote(key, block, l2, l1)).thenReturn(true);

    CacheAccessService service = new TopologyBackedCacheAccessService(topology, policy);

    assertSame(block, service.getBlock(key, context));

    verify(policy).shouldPromote(key, block, CacheTier.L2, context, topologyView);
    verify(topology).promote(key, block, l2, l1);
  }

  /**
   * Verifies that cache insertion is skipped when admission policy rejects the block.
   */
  @Test
  void testCacheBlockSkipsInsertionWhenRejected() {
    BlockCacheKey key = new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET);
    Cacheable block = mock(Cacheable.class);
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine engine = mock(CacheEngine.class);
    CacheWriteContext context = writeContext();

    when(topology.getView()).thenReturn(topologyView);
    when(topology.getEngine(CacheTier.SINGLE)).thenReturn(Optional.of(engine));
    when(policy.shouldAdmit(key, block, context, AdmissionPriority.NORMAL, topologyView))
      .thenReturn(AdmissionDecision.reject("rejected"));

    CacheAccessService service = new TopologyBackedCacheAccessService(topology, policy);

    service.cacheBlock(key, block, context);

    verify(policy).shouldAdmit(key, block, context, AdmissionPriority.NORMAL, topologyView);
    verify(policy, never()).selectTier(key, block, context, topologyView);
    verify(engine, never()).cacheBlock(key, block, true, true);
  }

  /**
   * Verifies that admitted blocks are inserted into policy-selected tiers.
   */
  @Test
  void testCacheBlockInsertsIntoSelectedTier() {
    BlockCacheKey key = new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET);
    Cacheable block = mock(Cacheable.class);
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine engine = mock(CacheEngine.class);
    CacheWriteContext context = writeContext();

    when(topology.getView()).thenReturn(topologyView);
    when(topology.getEngine(CacheTier.SINGLE)).thenReturn(Optional.of(engine));
    when(policy.shouldAdmit(key, block, context, AdmissionPriority.NORMAL, topologyView))
      .thenReturn(AdmissionDecision.admit());
    when(policy.selectRepresentation(key, block, context, topologyView))
      .thenReturn(RepresentationDecision.CURRENT_HBASE_DEFAULT);
    when(policy.selectTier(key, block, context, topologyView))
      .thenReturn(TierDecision.single(CacheTier.SINGLE));

    CacheAccessService service = new TopologyBackedCacheAccessService(topology, policy);

    service.cacheBlock(key, block, context);

    verify(policy).shouldAdmit(key, block, context, AdmissionPriority.NORMAL, topologyView);
    verify(policy, never()).selectRepresentation(key, block, context, topologyView);
    verify(policy).selectTier(key, block, context, topologyView);
    verify(engine).cacheBlock(key, block, true, true);
  }

  /**
   * Verifies that insertion can target multiple tiers.
   */
  @Test
  void testCacheBlockInsertsIntoMultipleSelectedTiers() {
    BlockCacheKey key = new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET);
    Cacheable block = mock(Cacheable.class);
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine l1 = mock(CacheEngine.class);
    CacheEngine l2 = mock(CacheEngine.class);
    CacheWriteContext context = writeContext();

    when(topology.getView()).thenReturn(topologyView);
    when(topology.getEngine(CacheTier.L1)).thenReturn(Optional.of(l1));
    when(topology.getEngine(CacheTier.L2)).thenReturn(Optional.of(l2));
    when(policy.shouldAdmit(key, block, context, AdmissionPriority.NORMAL, topologyView))
      .thenReturn(AdmissionDecision.admit());
    when(policy.selectRepresentation(key, block, context, topologyView))
      .thenReturn(RepresentationDecision.CURRENT_HBASE_DEFAULT);
    when(policy.selectTier(key, block, context, topologyView))
      .thenReturn(TierDecision.multiple(List.of(CacheTier.L1, CacheTier.L2)));

    CacheAccessService service = new TopologyBackedCacheAccessService(topology, policy);

    service.cacheBlock(key, block, context);

    verify(l1).cacheBlock(key, block, true, true);
    verify(l2).cacheBlock(key, block, true, true);
  }

  /**
   * Verifies that eviction is propagated to all participating engines.
   */
  @Test
  void testEvictBlockEvictsFromAllEngines() {
    BlockCacheKey key = new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET);
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine l1 = mock(CacheEngine.class);
    CacheEngine l2 = mock(CacheEngine.class);

    when(topology.getView()).thenReturn(topologyView);
    when(topology.getEngines()).thenReturn(List.of(l1, l2));
    when(l1.evictBlock(key)).thenReturn(false);
    when(l2.evictBlock(key)).thenReturn(true);

    CacheAccessService service = new TopologyBackedCacheAccessService(topology, policy);

    assertTrue(service.evictBlock(key));

    verify(l1).evictBlock(key);
    verify(l2).evictBlock(key);
  }

  /**
   * Verifies that file-level eviction sums results from all engines.
   */
  @Test
  void testEvictBlocksByHfileNameSumsEngineResults() {
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine l1 = mock(CacheEngine.class);
    CacheEngine l2 = mock(CacheEngine.class);

    when(topology.getView()).thenReturn(topologyView);
    when(topology.getEngines()).thenReturn(List.of(l1, l2));
    when(l1.evictBlocksByHfileName(HFILE_NAME)).thenReturn(2);
    when(l2.evictBlocksByHfileName(HFILE_NAME)).thenReturn(3);

    CacheAccessService service = new TopologyBackedCacheAccessService(topology, policy);

    assertEquals(5, service.evictBlocksByHfileName(HFILE_NAME));

    verify(l1).evictBlocksByHfileName(HFILE_NAME);
    verify(l2).evictBlocksByHfileName(HFILE_NAME);
  }

  /**
   * Verifies that aggregate sizing methods sum engine values.
   */
  @Test
  void testSizingMethodsAggregateEngines() {
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine l1 = mock(CacheEngine.class);
    CacheEngine l2 = mock(CacheEngine.class);

    when(topology.getView()).thenReturn(topologyView);
    when(topology.getEngines()).thenReturn(List.of(l1, l2));
    when(l1.getMaxSize()).thenReturn(100L);
    when(l2.getMaxSize()).thenReturn(200L);
    when(l1.getFreeSize()).thenReturn(10L);
    when(l2.getFreeSize()).thenReturn(20L);
    when(l1.size()).thenReturn(90L);
    when(l2.size()).thenReturn(180L);
    when(l1.getCurrentDataSize()).thenReturn(80L);
    when(l2.getCurrentDataSize()).thenReturn(160L);
    when(l1.getBlockCount()).thenReturn(8L);
    when(l2.getBlockCount()).thenReturn(16L);
    when(l1.getDataBlockCount()).thenReturn(6L);
    when(l2.getDataBlockCount()).thenReturn(12L);

    CacheAccessService service = new TopologyBackedCacheAccessService(topology, policy);

    assertEquals(300L, service.getMaxSize());
    assertEquals(30L, service.getFreeSize());
    assertEquals(270L, service.size());
    assertEquals(240L, service.getCurrentDataSize());
    assertEquals(24L, service.getBlockCount());
    assertEquals(18L, service.getDataBlockCount());
  }

  /**
   * Verifies that service-level stats are returned from the topology.
   */
  @Test
  void testGetStatsDelegatesToTopology() {
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheStats stats = new CacheStats("topology");

    when(topology.getView()).thenReturn(topologyView);
    when(topology.getStats()).thenReturn(stats);

    CacheAccessService service = new TopologyBackedCacheAccessService(topology, policy);

    assertSame(stats, service.getStats());
  }

  /**
   * Verifies optional helper methods aggregate or search across engines.
   */
  @Test
  void testOptionalHelpersUseParticipatingEngines() {
    BlockCacheKey key = new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET);
    HFileBlock block = mock(HFileBlock.class);
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine l1 = mock(CacheEngine.class);
    CacheEngine l2 = mock(CacheEngine.class);

    when(topology.getView()).thenReturn(topologyView);
    when(topology.getEngines()).thenReturn(List.of(l1, l2));
    when(l1.blockFitsIntoTheCache(block)).thenReturn(Optional.of(false));
    when(l2.blockFitsIntoTheCache(block)).thenReturn(Optional.of(true));
    when(l1.isAlreadyCached(key)).thenReturn(Optional.of(false));
    when(l2.isAlreadyCached(key)).thenReturn(Optional.of(true));
    when(l1.getBlockSize(key)).thenReturn(Optional.empty());
    when(l2.getBlockSize(key)).thenReturn(Optional.of(123));

    CacheAccessService service = new TopologyBackedCacheAccessService(topology, policy);

    assertEquals(Optional.of(true), service.blockFitsIntoTheCache(block));
    assertEquals(Optional.of(true), service.isAlreadyCached(key));
    assertEquals(Optional.of(123), service.getBlockSize(key));
  }

  /**
   * Verifies cache-enabled and initialization behavior across participating engines.
   */
  @Test
  void testEnablementAndInitializationUseEngines() {
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine l1 = mock(CacheEngine.class);
    CacheEngine l2 = mock(CacheEngine.class);

    when(topology.getView()).thenReturn(topologyView);
    when(topology.getEngines()).thenReturn(List.of(l1, l2));
    when(l1.isCacheEnabled()).thenReturn(false);
    when(l2.isCacheEnabled()).thenReturn(true);
    when(l1.waitForCacheInitialization(500L)).thenReturn(true);
    when(l2.waitForCacheInitialization(500L)).thenReturn(false);

    CacheAccessService service = new TopologyBackedCacheAccessService(topology, policy);

    assertTrue(service.isCacheEnabled());
    assertFalse(service.waitForCacheInitialization(500L));
  }

  /**
   * Verifies that configuration changes are propagated to all engines and shutdown uses topology.
   */
  @Test
  void testConfigurationChangeAndShutdown() {
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine l1 = mock(CacheEngine.class);
    CacheEngine l2 = mock(CacheEngine.class);
    Configuration conf = new Configuration(false);

    when(topology.getView()).thenReturn(topologyView);
    when(topology.getEngines()).thenReturn(List.of(l1, l2));

    CacheAccessService service = new TopologyBackedCacheAccessService(topology, policy);

    service.onConfigurationChange(conf);
    service.shutdown();

    verify(l1).onConfigurationChange(conf);
    verify(l2).onConfigurationChange(conf);
    verify(topology).shutdown();
  }

  /**
   * Verifies that range-based HFile eviction is propagated to all participating engines and sums
   * their results.
   */
  @Test
  void testEvictBlocksRangeByHfileNameSumsEngineResults() {
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine l1 = mock(CacheEngine.class);
    CacheEngine l2 = mock(CacheEngine.class);

    when(topology.getView()).thenReturn(topologyView);
    when(topology.getEngines()).thenReturn(List.of(l1, l2));
    when(l1.evictBlocksRangeByHfileName(HFILE_NAME, RANGE_START_OFFSET, RANGE_END_OFFSET))
      .thenReturn(2);
    when(l2.evictBlocksRangeByHfileName(HFILE_NAME, RANGE_START_OFFSET, RANGE_END_OFFSET))
      .thenReturn(3);

    CacheAccessService service = new TopologyBackedCacheAccessService(topology, policy);

    assertEquals(5,
      service.evictBlocksRangeByHfileName(HFILE_NAME, RANGE_START_OFFSET, RANGE_END_OFFSET));

    verify(l1).evictBlocksRangeByHfileName(HFILE_NAME, RANGE_START_OFFSET, RANGE_END_OFFSET);
    verify(l2).evictBlocksRangeByHfileName(HFILE_NAME, RANGE_START_OFFSET, RANGE_END_OFFSET);
  }

  /**
   * Verifies that region-level eviction is propagated to all participating engines and sums their
   * results.
   */
  @Test
  void testEvictBlocksByRegionNameSumsEngineResults() {
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    CacheEngine l1 = mock(CacheEngine.class);
    CacheEngine l2 = mock(CacheEngine.class);

    when(topology.getView()).thenReturn(topologyView);
    when(topology.getEngines()).thenReturn(List.of(l1, l2));
    when(l1.evictBlocksByRegionName("region")).thenReturn(4);
    when(l2.evictBlocksByRegionName("region")).thenReturn(5);

    CacheAccessService service = new TopologyBackedCacheAccessService(topology, policy);

    assertEquals(9, service.evictBlocksByRegionName("region"));

    verify(l1).evictBlocksByRegionName("region");
    verify(l2).evictBlocksByRegionName("region");
  }

  /**
   * Verifies that factory helper creates a topology-backed service.
   */
  @Test
  void testFactoryCreatesTopologyBackedService() {
    CacheTopology topology = mock(CacheTopology.class);
    CacheTopologyView topologyView = mock(CacheTopologyView.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);

    when(topology.getView()).thenReturn(topologyView);

    CacheAccessService service = CacheAccessServices.fromTopology(topology, policy);

    assertInstanceOf(TopologyBackedCacheAccessService.class, service);
    assertSame(topology, ((TopologyBackedCacheAccessService) service).getTopology());
    assertSame(policy, ((TopologyBackedCacheAccessService) service).getPolicy());
  }

  private static CacheRequestContext requestContext() {
    return CacheRequestContext.newBuilder().setCaching(true).setRepeat(false)
      .setUpdateCacheMetrics(true).setBlockType(BlockType.DATA).build();
  }

  private static CacheWriteContext writeContext() {
    return CacheWriteContext.newBuilder().setInMemory(true).setWaitWhenCache(true)
      .setSource(CacheWriteSource.READ_MISS).build();
  }
}
