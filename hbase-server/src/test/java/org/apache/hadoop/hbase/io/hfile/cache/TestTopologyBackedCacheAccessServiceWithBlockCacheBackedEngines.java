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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestTopologyBackedCacheAccessServiceWithBlockCacheBackedEngines {

  @Test
  void testL1HitDoesNotCheckL2() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    when(l1.getBlock(key, true, false, true)).thenReturn(block);

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    assertSame(block, service.getBlock(key, requestContext()));

    verify(l1).getBlock(key, true, false, true);
    verify(l2, never()).getBlock(any(), any(Boolean.class), any(Boolean.class), any(Boolean.class));
  }

  @Test
  void testL2HitPromotesToL1AndEvictsFromL2() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    when(l1.getBlock(key, true, false, true)).thenReturn(null);
    when(l2.getBlock(key, true, false, true)).thenReturn(block);

    TopologyBackedCacheAccessService service = service(l1, l2, promoteToL1Policy());

    assertSame(block, service.getBlock(key, requestContext()));

    verify(l1).getBlock(key, true, false, true);
    verify(l2).getBlock(key, true, false, true);
    verify(l1).cacheBlock(key, block);
    verify(l2).evictBlock(key);
  }

  @Test
  void testMissReturnsNull() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);

    when(l1.getBlock(key, true, false, true)).thenReturn(null);
    when(l2.getBlock(key, true, false, true)).thenReturn(null);

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    assertNull(service.getBlock(key, requestContext()));

    verify(l1).getBlock(key, true, false, true);
    verify(l2).getBlock(key, true, false, true);
  }

  @Test
  void testCacheBlockToL1() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    TopologyBackedCacheAccessService service = service(l1, l2, admitToTiersPolicy(CacheTier.L1));

    service.cacheBlock(key, block, writeContext());

    verify(l1).cacheBlock(key, block, false, false);
    verify(l2, never()).cacheBlock(any(), any(), any(Boolean.class), any(Boolean.class));
  }

  @Test
  void testCacheBlockToL2() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    TopologyBackedCacheAccessService service = service(l1, l2, admitToTiersPolicy(CacheTier.L2));

    service.cacheBlock(key, block, writeContext());

    verify(l2).cacheBlock(key, block, false, false);
    verify(l1, never()).cacheBlock(any(), any(), any(Boolean.class), any(Boolean.class));
  }

  @Test
  void testCacheBlockToBothTiers() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    TopologyBackedCacheAccessService service =
      service(l1, l2, admitToTiersPolicy(CacheTier.L1, CacheTier.L2));

    service.cacheBlock(key, block, writeContext());

    verify(l1).cacheBlock(key, block, false, false);
    verify(l2).cacheBlock(key, block, false, false);
  }

  @Test
  void testRejectedBlockIsNotCached() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    TopologyBackedCacheAccessService service = service(l1, l2, rejectPolicy());

    service.cacheBlock(key, block, writeContext());

    verify(l1, never()).cacheBlock(any(), any(), any(Boolean.class), any(Boolean.class));
    verify(l2, never()).cacheBlock(any(), any(), any(Boolean.class), any(Boolean.class));
  }

  @Test
  void testEvictBlockEvictsFromBothTiers() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    service.evictBlock(key);

    verify(l1).evictBlock(key);
    verify(l2).evictBlock(key);
  }

  @Test
  void testShutdownShutsDownBothTiers() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);

    TopologyBackedCacheAccessService service = service(l1, l2, noPromotionPolicy());

    service.shutdown();

    verify(l1).shutdown();
    verify(l2).shutdown();
  }

  private static TopologyBackedCacheAccessService service(BlockCache l1, BlockCache l2,
    CachePlacementAdmissionPolicy policy) {
    CacheEngine l1Engine = CacheEngines.fromBlockCache(l1);
    CacheEngine l2Engine = CacheEngines.fromBlockCache(l2);
    CacheTopology topology = new TieredExclusiveTopology("test", l1Engine, l2Engine);
    return new TopologyBackedCacheAccessService(topology, policy);
  }

  private static CacheRequestContext requestContext() {
    return CacheRequestContext.newBuilder().withCaching(true).withRepeat(false)
      .withUpdateCacheMetrics(true).build();
  }

  private static CacheWriteContext writeContext() {
    return CacheWriteContext.newBuilder().withInMemory(false).withWaitWhenCache(false).build();
  }

  private static CachePlacementAdmissionPolicy noPromotionPolicy() {
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    when(policy.shouldPromote(any(), any(), any(), any(), any()))
      .thenReturn(PromotionDecision.none());
    return policy;
  }

  private static CachePlacementAdmissionPolicy promoteToL1Policy() {
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    when(policy.shouldPromote(any(), any(), eq(CacheTier.L2), any(), any()))
      .thenReturn(PromotionDecision.promoteTo(CacheTier.L1, false));
    when(policy.shouldPromote(any(), any(), eq(CacheTier.L1), any(), any()))
      .thenReturn(PromotionDecision.none());
    return policy;
  }

  private static CachePlacementAdmissionPolicy admitToTiersPolicy(CacheTier... tiers) {
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    when(policy.shouldAdmit(any(), any(), any(), any(), any()))
      .thenReturn(AdmissionDecision.admit());
    when(policy.selectTier(any(), any(), any(), any()))
      .thenReturn(TierDecision.multiple(Arrays.asList(tiers)));
    return policy;
  }

  private static CachePlacementAdmissionPolicy rejectPolicy() {
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);
    when(policy.shouldAdmit(any(), any(), any(), any(), any()))
      .thenReturn(AdmissionDecision.reject("test rejection"));
    return policy;
  }
}
