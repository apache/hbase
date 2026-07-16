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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestBlockCacheBackedCacheEngine {

  @Test
  void testRejectsNullBlockCache() {
    assertThrows(NullPointerException.class, () -> new BlockCacheBackedCacheEngine(null));
  }

  @Test
  void testReturnsWrappedBlockCache() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheBackedCacheEngine engine = new BlockCacheBackedCacheEngine(blockCache);

    assertSame(blockCache, engine.getBlockCache());
  }

  @Test
  void testGetNameUsesWrappedClassName() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheBackedCacheEngine engine = new BlockCacheBackedCacheEngine(blockCache);

    assertEquals(blockCache.getClass().getSimpleName(), engine.getName());
  }

  @Test
  void testCacheBlockDelegates() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheBackedCacheEngine engine = new BlockCacheBackedCacheEngine(blockCache);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    engine.cacheBlock(key, block);
    engine.cacheBlock(key, block, true);
    engine.cacheBlock(key, block, true, true);

    verify(blockCache).cacheBlock(key, block);
    verify(blockCache).cacheBlock(key, block, true);
    verify(blockCache).cacheBlock(key, block, true, true);
  }

  @Test
  void testGetBlockDelegates() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheBackedCacheEngine engine = new BlockCacheBackedCacheEngine(blockCache);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    when(blockCache.getBlock(key, true, false, true)).thenReturn(block);
    when(blockCache.getBlock(key, true, false, true, BlockType.DATA)).thenReturn(block);

    assertSame(block, engine.getBlock(key, true, false, true));
    assertSame(block, engine.getBlock(key, true, false, true, BlockType.DATA));
  }

  @Test
  void testEvictionDelegates() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheBackedCacheEngine engine = new BlockCacheBackedCacheEngine(blockCache);
    BlockCacheKey key = new BlockCacheKey("file", 1L);

    when(blockCache.evictBlock(key)).thenReturn(true);
    when(blockCache.evictBlocksByHfileName("file")).thenReturn(2);
    when(blockCache.evictBlocksRangeByHfileName("file", 10L, 20L)).thenReturn(3);

    assertTrue(engine.evictBlock(key));
    assertEquals(2, engine.evictBlocksByHfileName("file"));
    assertEquals(3, engine.evictBlocksRangeByHfileName("file", 10L, 20L));
  }

  @Test
  void testStatsAndSizingDelegates() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheBackedCacheEngine engine = new BlockCacheBackedCacheEngine(blockCache);
    CacheStats stats = mock(CacheStats.class);

    when(blockCache.getStats()).thenReturn(stats);
    when(blockCache.getMaxSize()).thenReturn(100L);
    when(blockCache.getFreeSize()).thenReturn(20L);
    when(blockCache.size()).thenReturn(80L);
    when(blockCache.getCurrentDataSize()).thenReturn(70L);
    when(blockCache.getBlockCount()).thenReturn(7L);
    when(blockCache.getDataBlockCount()).thenReturn(5L);

    assertSame(stats, engine.getStats());
    assertEquals(100L, engine.getMaxSize());
    assertEquals(20L, engine.getFreeSize());
    assertEquals(80L, engine.size());
    assertEquals(70L, engine.getCurrentDataSize());
    assertEquals(7L, engine.getBlockCount());
    assertEquals(5L, engine.getDataBlockCount());
  }

  @Test
  void testOptionalCapabilityDelegates() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheBackedCacheEngine engine = new BlockCacheBackedCacheEngine(blockCache);
    HFileBlock block = mock(HFileBlock.class);
    BlockCacheKey key = new BlockCacheKey("file", 1L);

    when(blockCache.blockFitsIntoTheCache(block)).thenReturn(Optional.of(true));
    when(blockCache.isAlreadyCached(key)).thenReturn(Optional.of(false));
    when(blockCache.getBlockSize(key)).thenReturn(Optional.of(123));

    assertEquals(Optional.of(true), engine.blockFitsIntoTheCache(block));
    assertEquals(Optional.of(false), engine.isAlreadyCached(key));
    assertEquals(Optional.of(123), engine.getBlockSize(key));
  }

  @Test
  void testLifecycleDelegates() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheBackedCacheEngine engine = new BlockCacheBackedCacheEngine(blockCache);
    Configuration conf = new Configuration(false);
    Path fileName = new Path("/hbase/test-file");

    when(blockCache.isCacheEnabled()).thenReturn(true);
    when(blockCache.waitForCacheInitialization(1000L)).thenReturn(true);

    assertTrue(engine.isCacheEnabled());
    assertTrue(engine.waitForCacheInitialization(1000L));

    engine.onConfigurationChange(conf);
    engine.notifyFileCachingCompleted(fileName, 10, 8, 1024L);
    engine.shutdown();

    verify(blockCache).onConfigurationChange(conf);
    verify(blockCache).notifyFileCachingCompleted(fileName, 10, 8, 1024L);
    verify(blockCache).shutdown();
  }

  @Test
  void testFactoryCreatesBlockCacheBackedEngine() {
    BlockCache blockCache = mock(BlockCache.class);
    CacheEngine engine = CacheEngines.fromBlockCache(blockCache);

    assertTrue(engine instanceof BlockCacheBackedCacheEngine);
    assertSame(blockCache, ((BlockCacheBackedCacheEngine) engine).getBlockCache());
  }

  @Test
  void testFactoryRejectsNullBlockCache() {
    assertThrows(NullPointerException.class, () -> CacheEngines.fromBlockCache(null));
  }

  @Test
  void testNullArgumentsRejected() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheBackedCacheEngine engine = new BlockCacheBackedCacheEngine(blockCache);
    BlockCacheKey key = new BlockCacheKey("file", 1L);
    Cacheable block = mock(Cacheable.class);

    assertThrows(NullPointerException.class, () -> engine.cacheBlock(null, block));
    assertThrows(NullPointerException.class, () -> engine.cacheBlock(key, null));
    assertThrows(NullPointerException.class, () -> engine.getBlock(null, true, false, true));
    assertThrows(NullPointerException.class, () -> engine.evictBlock(null));
    assertThrows(NullPointerException.class, () -> engine.evictBlocksByHfileName(null));
    assertThrows(NullPointerException.class,
      () -> engine.evictBlocksRangeByHfileName(null, 0L, 1L));
    assertThrows(NullPointerException.class, () -> engine.evictBlocksByRegionName(null));
    assertThrows(NullPointerException.class, () -> engine.blockFitsIntoTheCache(null));
    assertThrows(NullPointerException.class, () -> engine.isAlreadyCached(null));
    assertThrows(NullPointerException.class, () -> engine.getBlockSize(null));
    assertThrows(NullPointerException.class, () -> engine.onConfigurationChange(null));
    assertThrows(NullPointerException.class,
      () -> engine.notifyFileCachingCompleted(null, 1, 1, 1L));
  }

  @Test
  void testDisabledCacheStateDelegates() {
    BlockCache blockCache = mock(BlockCache.class);
    BlockCacheBackedCacheEngine engine = new BlockCacheBackedCacheEngine(blockCache);

    when(blockCache.isCacheEnabled()).thenReturn(false);

    assertFalse(engine.isCacheEnabled());
  }
}
