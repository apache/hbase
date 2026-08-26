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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CombinedBlockCache;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestCacheAccessServices {

  @Test
  void testFromBlockCacheCreatesBlockCacheBackedServiceForRegularBlockCache() {
    BlockCache blockCache = mock(BlockCache.class);
    CacheAccessService service = CacheAccessServices.fromBlockCache(blockCache);
    assertTrue(service instanceof TopologyBackedCacheAccessService);
  }

  @Test
  void testFromBlockCacheCreatesTopologyBackedServiceForCombinedBlockCache() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    CombinedBlockCache combinedBlockCache = mock(CombinedBlockCache.class);

    when(combinedBlockCache.getBlockCaches()).thenReturn(new BlockCache[] { l1, l2 });

    CacheAccessService service = CacheAccessServices.fromBlockCache(combinedBlockCache);

    assertTrue(service instanceof TopologyBackedCacheAccessService);

    TopologyBackedCacheAccessService topologyBackedService =
      (TopologyBackedCacheAccessService) service;

    assertTrue(topologyBackedService.getTopology() instanceof TieredExclusiveTopology);
    assertSame(CacheTopologyType.TIERED_EXCLUSIVE, topologyBackedService.getTopology().getType());

    Optional<CacheEngine> l1Engine = topologyBackedService.getTopology().getEngine(CacheTier.L1);
    Optional<CacheEngine> l2Engine = topologyBackedService.getTopology().getEngine(CacheTier.L2);

    assertTrue(l1Engine.isPresent());
    assertTrue(l2Engine.isPresent());
    assertTrue(l1Engine.get() instanceof BlockCacheBackedCacheEngine);
    assertTrue(l2Engine.get() instanceof BlockCacheBackedCacheEngine);
    assertSame(l1, ((BlockCacheBackedCacheEngine) l1Engine.get()).getBlockCache());
    assertSame(l2, ((BlockCacheBackedCacheEngine) l2Engine.get()).getBlockCache());
  }

  @Test
  void testFromBlockCacheRejectsNull() {
    assertThrows(NullPointerException.class, () -> CacheAccessServices.fromBlockCache(null));
  }

  @Test
  void testDisabledReturnsNoOpService() {
    assertTrue(CacheAccessServices.disabled() instanceof NoOpCacheAccessService);
  }
}
