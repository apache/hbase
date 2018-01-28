/**
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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({IOTests.class, SmallTests.class})
public class TestLruCachedBlock {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLruCachedBlock.class);

  LruCachedBlock block;
  LruCachedBlock blockEqual;
  LruCachedBlock blockNotEqual;

  @Before
  public void setUp() throws Exception {
    BlockCacheKey cacheKey = new BlockCacheKey("name", 0);
    BlockCacheKey otherKey = new BlockCacheKey("name2", 1);

    Cacheable cacheable = Mockito.mock(Cacheable.class);
    Cacheable otheCacheable = Mockito.mock(Cacheable.class);

    block = new LruCachedBlock(cacheKey, cacheable, 0);
    blockEqual = new LruCachedBlock(otherKey, otheCacheable, 0);
    blockNotEqual = new LruCachedBlock(cacheKey, cacheable, 1);
  }

  @Test
  public void testEquality() {
    assertEquals(block.hashCode(), blockEqual.hashCode());
    assertNotEquals(block.hashCode(), blockNotEqual.hashCode());

    assertEquals(block, blockEqual);
    assertNotEquals(block, blockNotEqual);
  }
}
