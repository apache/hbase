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

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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

/**
 * Tests for {@link BlockCacheBackedCacheAccessService} and related service helpers.
 */
@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestBlockCacheBackedCacheAccessService {

  private static final String HFILE_NAME = "file";

  private static final long BLOCK_OFFSET = 1L;

  private static final long RANGE_START_OFFSET = 1L;

  private static final long RANGE_END_OFFSET = 10L;

  /**
   * Verifies that context-based lookup delegates to the block-type aware legacy lookup method.
   */
  @Test
  void testGetBlockWithBlockTypeDelegatesToBlockCache() {
    BlockCache blockCache = mock(BlockCache.class);
    CacheAccessService service = new BlockCacheBackedCacheAccessService(blockCache);
    BlockCacheKey key = new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET);
    Cacheable block = mock(Cacheable.class);

    when(blockCache.getBlock(key, true, true, false, BlockType.DATA)).thenReturn(block);

    CacheRequestContext context = CacheRequestContext.newBuilder().setCaching(true).setRepeat(true)
      .setUpdateCacheMetrics(false).setBlockType(BlockType.DATA).build();

    assertSame(block, service.getBlock(key, context));
    verify(blockCache).getBlock(key, true, true, false, BlockType.DATA);
  }

  /**
   * Verifies that context-based lookup delegates to the legacy lookup method without block type.
   */
  @Test
  void testGetBlockWithoutBlockTypeDelegatesToBlockCache() {
    BlockCache blockCache = mock(BlockCache.class);
    CacheAccessService service = new BlockCacheBackedCacheAccessService(blockCache);
    BlockCacheKey key = new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET);
    Cacheable block = mock(Cacheable.class);

    when(blockCache.getBlock(key, true, false, true)).thenReturn(block);

    CacheRequestContext context = CacheRequestContext.newBuilder().setCaching(true).setRepeat(false)
      .setUpdateCacheMetrics(true).build();

    assertSame(block, service.getBlock(key, context));
    verify(blockCache).getBlock(key, true, false, true);
  }

  /**
   * Verifies that context-based insertion delegates in-memory and wait flags correctly.
   */
  @Test
  void testCacheBlockDelegatesToBlockCache() {
    BlockCache blockCache = mock(BlockCache.class);
    CacheAccessService service = new BlockCacheBackedCacheAccessService(blockCache);
    BlockCacheKey key = new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET);
    Cacheable block = mock(Cacheable.class);

    CacheWriteContext context = CacheWriteContext.newBuilder().setInMemory(true)
      .setWaitWhenCache(true).setSource(CacheWriteSource.READ_MISS).build();

    service.cacheBlock(key, block, context);

    verify(blockCache).cacheBlock(key, block, true, true);
  }

  /**
   * Verifies that invalidation methods delegate to the wrapped block cache.
   */
  @Test
  void testEvictionDelegatesToBlockCache() {
    BlockCache blockCache = mock(BlockCache.class);
    CacheAccessService service = new BlockCacheBackedCacheAccessService(blockCache);
    BlockCacheKey key = new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET);

    when(blockCache.evictBlock(key)).thenReturn(true);
    when(blockCache.evictBlocksByHfileName(HFILE_NAME)).thenReturn(3);
    when(blockCache.evictBlocksRangeByHfileName(HFILE_NAME, RANGE_START_OFFSET, RANGE_END_OFFSET))
      .thenReturn(2);

    assertTrue(service.evictBlock(key));
    assertEquals(3, service.evictBlocksByHfileName(HFILE_NAME));
    assertEquals(2,
      service.evictBlocksRangeByHfileName(HFILE_NAME, RANGE_START_OFFSET, RANGE_END_OFFSET));

    verify(blockCache).evictBlock(key);
    verify(blockCache).evictBlocksByHfileName(HFILE_NAME);
    verify(blockCache).evictBlocksRangeByHfileName(HFILE_NAME, RANGE_START_OFFSET,
      RANGE_END_OFFSET);
  }

  /**
   * Verifies that stats, sizing, lifecycle, and optional helpers delegate to the wrapped cache.
   */
  @Test
  void testStatsSizingLifecycleAndHelpersDelegateToBlockCache() {
    BlockCache blockCache = mock(BlockCache.class);
    CacheAccessService service = new BlockCacheBackedCacheAccessService(blockCache);
    CacheStats stats = new CacheStats("test");
    HFileBlock hfileBlock = mock(HFileBlock.class);
    BlockCacheKey key = new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET);
    Configuration conf = new Configuration(false);

    when(blockCache.getStats()).thenReturn(stats);
    when(blockCache.getMaxSize()).thenReturn(100L);
    when(blockCache.getFreeSize()).thenReturn(40L);
    when(blockCache.size()).thenReturn(60L);
    when(blockCache.getCurrentDataSize()).thenReturn(50L);
    when(blockCache.getBlockCount()).thenReturn(10L);
    when(blockCache.getDataBlockCount()).thenReturn(8L);
    when(blockCache.blockFitsIntoTheCache(hfileBlock)).thenReturn(Optional.of(true));
    when(blockCache.isAlreadyCached(key)).thenReturn(Optional.of(false));
    when(blockCache.getBlockSize(key)).thenReturn(Optional.of(123));
    when(blockCache.isCacheEnabled()).thenReturn(true);
    when(blockCache.waitForCacheInitialization(500L)).thenReturn(true);

    assertSame(stats, service.getStats());
    assertEquals(100L, service.getMaxSize());
    assertEquals(40L, service.getFreeSize());
    assertEquals(60L, service.size());
    assertEquals(50L, service.getCurrentDataSize());
    assertEquals(10L, service.getBlockCount());
    assertEquals(8L, service.getDataBlockCount());
    assertEquals(Optional.of(true), service.blockFitsIntoTheCache(hfileBlock));
    assertEquals(Optional.of(false), service.isAlreadyCached(key));
    assertEquals(Optional.of(123), service.getBlockSize(key));
    assertTrue(service.isCacheEnabled());
    assertTrue(service.waitForCacheInitialization(500L));

    service.onConfigurationChange(conf);
    service.shutdown();

    verify(blockCache).onConfigurationChange(conf);
    verify(blockCache).shutdown();
  }

  /**
   * Verifies factory helper methods.
   */
  @Test
  void testCacheAccessServicesFactoryMethods() {
    BlockCache blockCache = mock(BlockCache.class);
    CacheAccessService service = CacheAccessServices.fromBlockCache(blockCache);

    assertInstanceOf(BlockCacheBackedCacheAccessService.class, service);
    assertSame(blockCache, ((BlockCacheBackedCacheAccessService) service).getBlockCache());
    assertInstanceOf(NoOpCacheAccessService.class, CacheAccessServices.disabled());
  }

  /**
   * Verifies disabled-cache behavior.
   */
  @Test
  void testNoOpCacheAccessService() {
    CacheAccessService service = new NoOpCacheAccessService(new CacheStats("noop"));

    assertEquals("NoOpCacheAccessService", service.getName());
    assertNull(service.getBlock(mock(BlockCacheKey.class), mock(CacheRequestContext.class)));
    service.cacheBlock(mock(BlockCacheKey.class), mock(Cacheable.class),
      mock(CacheWriteContext.class));
    assertFalse(service.evictBlock(mock(BlockCacheKey.class)));
    assertEquals(0, service.evictBlocksByHfileName(HFILE_NAME));
    assertEquals(0L, service.getMaxSize());
    assertEquals(0L, service.getFreeSize());
    assertEquals(0L, service.size());
    assertEquals(0L, service.getCurrentDataSize());
    assertEquals(0L, service.getBlockCount());
    assertEquals(0L, service.getDataBlockCount());
    assertFalse(service.isCacheEnabled());
    assertFalse(service.waitForCacheInitialization(1L));
    service.onConfigurationChange(new Configuration(false));
    service.shutdown();
  }

  @Test
  void testNotifyFileCachingCompletedDelegatesToBlockCache() {
    BlockCache blockCache = mock(BlockCache.class);
    CacheAccessService service = new BlockCacheBackedCacheAccessService(blockCache);
    Path fileName = new Path("/hbase/table/region/family/file");
    int totalBlockCount = 10;
    int dataBlockCount = 8;
    long size = 1024L;

    service.notifyFileCachingCompleted(fileName, totalBlockCount, dataBlockCount, size);

    verify(blockCache).notifyFileCachingCompleted(fileName, totalBlockCount, dataBlockCount, size);
  }

  @Test
  void testNotifyFileCachingCompletedRejectsNullPath() {
    BlockCache blockCache = mock(BlockCache.class);
    CacheAccessService service = new BlockCacheBackedCacheAccessService(blockCache);

    assertThrows(NullPointerException.class,
      () -> service.notifyFileCachingCompleted(null, 10, 8, 1024L));
  }
}
