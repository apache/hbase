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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestIndexOnlyCachePlacementAdmissionPolicy {

  private static final BlockCacheKey CACHE_KEY = new BlockCacheKey("test", 0);

  private final IndexOnlyCachePlacementAdmissionPolicy policy =
    IndexOnlyCachePlacementAdmissionPolicy.INSTANCE;
  private final CacheWriteContext context = mock(CacheWriteContext.class);
  private final CacheTopologyView topologyView = mock(CacheTopologyView.class);

  /**
   * Verifies that the policy preserves the legacy IndexOnlyLruBlockCache admission behavior:
   * data-category blocks are rejected and every non-data block is admitted.
   */
  @Test
  public void testAdmissionByBlockType() {
    for (BlockType blockType : BlockType.values()) {
      Cacheable block = mock(Cacheable.class);
      when(block.getBlockType()).thenReturn(blockType);

      AdmissionDecision decision =
        policy.shouldAdmit(CACHE_KEY, block, context, AdmissionPriority.NORMAL, topologyView);

      boolean expected = blockType.getCategory() != BlockType.BlockCategory.DATA;
      assertEquals(expected, decision.isAdmitted(),
        "Unexpected admission decision for block type " + blockType);
    }
  }

  /**
   * Verifies that a block without a block type is not admitted.
   */
  @Test
  public void testRejectsBlockWithoutType() {
    Cacheable block = mock(Cacheable.class);
    when(block.getBlockType()).thenReturn(null);

    AdmissionDecision decision =
      policy.shouldAdmit(CACHE_KEY, block, context, AdmissionPriority.NORMAL, topologyView);

    assertFalse(decision.isAdmitted());
  }
}
