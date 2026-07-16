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

import org.apache.hadoop.hbase.io.hfile.BlockCache;
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
}
