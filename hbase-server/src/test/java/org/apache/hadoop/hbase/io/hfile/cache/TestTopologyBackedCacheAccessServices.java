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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Optional;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestTopologyBackedCacheAccessServices {

  @Test
  void testFromTieredExclusiveBlockCachesCreatesExpectedService() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);

    TopologyBackedCacheAccessService service =
      TopologyBackedCacheAccessServices.fromTieredExclusiveBlockCaches("combined", l1, l2, policy);

    assertEquals("combined", service.getName());
    assertSame(policy, service.getPolicy());
    assertTrue(service.getTopology() instanceof TieredExclusiveTopology);
    assertEquals(CacheTopologyType.TIERED_EXCLUSIVE, service.getTopology().getType());

    Optional<CacheEngine> l1Engine = service.getTopology().getEngine(CacheTier.L1);
    Optional<CacheEngine> l2Engine = service.getTopology().getEngine(CacheTier.L2);

    assertTrue(l1Engine.isPresent());
    assertTrue(l2Engine.isPresent());
    assertTrue(l1Engine.get() instanceof BlockCacheBackedCacheEngine);
    assertTrue(l2Engine.get() instanceof BlockCacheBackedCacheEngine);
    assertSame(l1, ((BlockCacheBackedCacheEngine) l1Engine.get()).getBlockCache());
    assertSame(l2, ((BlockCacheBackedCacheEngine) l2Engine.get()).getBlockCache());
  }

  @Test
  void testFromTieredExclusiveBlockCachesRejectsNullArguments() {
    BlockCache l1 = mock(BlockCache.class);
    BlockCache l2 = mock(BlockCache.class);
    CachePlacementAdmissionPolicy policy = mock(CachePlacementAdmissionPolicy.class);

    assertThrows(NullPointerException.class,
      () -> TopologyBackedCacheAccessServices.fromTieredExclusiveBlockCaches(null, l1, l2, policy));
    assertThrows(NullPointerException.class, () -> TopologyBackedCacheAccessServices
      .fromTieredExclusiveBlockCaches("combined", null, l2, policy));
    assertThrows(NullPointerException.class, () -> TopologyBackedCacheAccessServices
      .fromTieredExclusiveBlockCaches("combined", l1, null, policy));
    assertThrows(NullPointerException.class, () -> TopologyBackedCacheAccessServices
      .fromTieredExclusiveBlockCaches("combined", l1, l2, null));
  }
}
