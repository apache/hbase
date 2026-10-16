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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LruCacheEngine}.
 */
@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestLruCacheEngine {

  private static final String LRU_MIN_FACTOR_CONFIG_NAME = "hbase.lru.blockcache.min.factor";

  /**
   * Verifies that an eviction pass with no excess occupancy reports zero cache pressure.
   */
  @Test
  void testEvictionPassReportsZeroPressureWhenBelowTargetSize() {
    LruCacheEngine engine = new LruCacheEngine(1024L * 1024L, 64L * 1024L, false);
    CacheEvictionListener listener = mock(CacheEvictionListener.class);

    try {
      engine.setEvictionListener(listener);

      engine.evict();

      verify(listener).onEvictionPressure(engine, 0L);
    } finally {
      engine.shutdown();
    }
  }

  /**
   * Verifies that an eviction pass reports the number of bytes the engine determines should be
   * freed to reach its configured minimum target size.
   */
  @Test
  void testEvictionPassReportsRequestedPressure() {
    Configuration conf = new Configuration(false);
    conf.setFloat(LRU_MIN_FACTOR_CONFIG_NAME, 0.5f);

    LruCacheEngine engine = new LruCacheEngine(16L * 1024L * 1024L, 64L * 1024L, false, conf);
    CacheEvictionListener listener = mock(CacheEvictionListener.class);

    try {
      long currentSize = engine.getCurrentSize();
      long newMaxSize = Math.max(1L, currentSize / 2L);
      long expectedBytesToFree = currentSize - (long) Math.floor(newMaxSize * 0.5f);

      assertTrue(currentSize > 0);
      assertTrue(expectedBytesToFree > 0);

      engine.setEvictionListener(listener);
      engine.setMaxSize(newMaxSize);

      verify(listener).onEvictionPressure(engine, expectedBytesToFree);
    } finally {
      engine.shutdown();
    }
  }
}
