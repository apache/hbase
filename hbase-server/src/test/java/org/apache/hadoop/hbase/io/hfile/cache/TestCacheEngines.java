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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.ResizableBlockCache;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
class TestCacheEngines {

  @Test
  void testFromBlockCacheReturnsBlockCacheBackedCacheEngine() {
    BlockCache blockCache = mock(BlockCache.class);
    CacheEngine engine = CacheEngines.fromBlockCache(blockCache);
    assertTrue(engine instanceof BlockCacheBackedCacheEngine);
    assertSame(blockCache, ((BlockCacheBackedCacheEngine) engine).getBlockCache());
  }

  @Test
  void testFromBlockCacheRejectsNull() {
    assertThrows(NullPointerException.class, () -> CacheEngines.fromBlockCache(null));
  }

  /**
   * Verifies that adapting a resizable legacy block cache preserves its dynamic sizing capability.
   */
  @Test
  public void testResizableBlockCachePreservesResizeCapability() {
    ResizableBlockCache blockCache = mock(ResizableBlockCache.class);
    CacheEngine engine = CacheEngines.fromBlockCache(blockCache);
    assertInstanceOf(ResizableCacheEngine.class, engine);
    ResizableCacheEngine resizableEngine = (ResizableCacheEngine) engine;
    resizableEngine.setMaxSize(12345L);
    verify(blockCache).setMaxSize(12345L);
  }

  /**
   * Verifies that adapting a non-resizable legacy block cache does not expose dynamic sizing.
   */
  @Test
  public void testNonResizableBlockCacheDoesNotExposeResizeCapability() {
    BlockCache blockCache = mock(BlockCache.class);
    CacheEngine engine = CacheEngines.fromBlockCache(blockCache);
    assertFalse(engine instanceof ResizableCacheEngine);
  }
}
