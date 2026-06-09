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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link NoOpCacheAccessService} and related service helpers.
 */
@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)

public class TestNoOpCacheAccessService {

  private static final String HFILE_NAME = "file";
  private static final String REGION_NAME = "region";
  private static final long BLOCK_OFFSET = 1L;
  private static final long RANGE_START_OFFSET = 1L;
  private static final long RANGE_END_OFFSET = 10L;

  @Test
  void testNoOpCacheAccessService() {
    CacheAccessService service = new NoOpCacheAccessService(new CacheStats("noop"));
    BlockCacheKey key = new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET);
    CacheRequestContext requestContext = CacheRequestContext.newBuilder().withCaching(true)
      .withRepeat(false).withUpdateCacheMetrics(true).build();
    CacheWriteContext writeContext = CacheWriteContext.newBuilder().withInMemory(true)
      .withWaitWhenCache(true).withSource(CacheWriteSource.READ_MISS).build();
    Cacheable block = mock(Cacheable.class);
    Configuration conf = new Configuration(false);

    assertEquals("NoOpCacheAccessService", service.getName());
    assertNull(service.getBlock(key, requestContext));

    service.cacheBlock(key, block, writeContext);

    assertFalse(service.evictBlock(key));
    assertEquals(0, service.evictBlocksByHfileName(HFILE_NAME));
    assertEquals(0,
      service.evictBlocksRangeByHfileName(HFILE_NAME, RANGE_START_OFFSET, RANGE_END_OFFSET));
    assertEquals(0, service.evictBlocksByRegionName(REGION_NAME));

    assertEquals(0L, service.getMaxSize());
    assertEquals(0L, service.getFreeSize());
    assertEquals(0L, service.size());
    assertEquals(0L, service.getCurrentDataSize());
    assertEquals(0L, service.getBlockCount());
    assertEquals(0L, service.getDataBlockCount());

    assertEquals(Optional.empty(), service.blockFitsIntoTheCache(mock(HFileBlock.class)));
    assertEquals(Optional.empty(), service.isAlreadyCached(key));
    assertEquals(Optional.empty(), service.getBlockSize(key));

    assertFalse(service.isCacheEnabled());
    assertFalse(service.waitForCacheInitialization(1L));

    service.onConfigurationChange(conf);
    service.shutdown();
  }

  @Test
  void testNoOpCacheAccessServiceRejectsNullInputs() {
    CacheAccessService service = new NoOpCacheAccessService(new CacheStats("noop"));
    Cacheable block = mock(Cacheable.class);
    CacheRequestContext requestContext = CacheRequestContext.newBuilder().build();
    CacheWriteContext writeContext = CacheWriteContext.newBuilder().build();

    assertThrows(NullPointerException.class, () -> new NoOpCacheAccessService(null));
    assertThrows(NullPointerException.class, () -> service.getBlock(null, requestContext));
    assertThrows(NullPointerException.class,
      () -> service.getBlock(new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET), null));
    assertThrows(NullPointerException.class, () -> service.cacheBlock(null, block, writeContext));
    assertThrows(NullPointerException.class,
      () -> service.cacheBlock(new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET), null, writeContext));
    assertThrows(NullPointerException.class,
      () -> service.cacheBlock(new BlockCacheKey(HFILE_NAME, BLOCK_OFFSET), block, null));
    assertThrows(NullPointerException.class, () -> service.evictBlock(null));
    assertThrows(NullPointerException.class, () -> service.evictBlocksByHfileName(null));
    assertThrows(NullPointerException.class,
      () -> service.evictBlocksRangeByHfileName(null, RANGE_START_OFFSET, RANGE_END_OFFSET));
    assertThrows(NullPointerException.class, () -> service.evictBlocksByRegionName(null));
    assertThrows(NullPointerException.class, () -> service.blockFitsIntoTheCache(null));
    assertThrows(NullPointerException.class, () -> service.isAlreadyCached(null));
    assertThrows(NullPointerException.class, () -> service.getBlockSize(null));
    assertThrows(NullPointerException.class, () -> service.onConfigurationChange(null));
  }
}
