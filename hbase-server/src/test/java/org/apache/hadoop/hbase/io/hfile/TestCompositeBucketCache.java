/**
 *
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestCompositeBucketCache extends TestCompositeBlockCache {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompositeBucketCache.class);

  @Test
  public void testMultiThreadGetAndEvictBlock() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean(BlockCacheFactory.BUCKET_CACHE_COMPOSITE_KEY, true);
    conf.set(CompositeBucketCache.IOENGINE_L1, "offheap");
    conf.set(CompositeBucketCache.IOENGINE_L2, "offheap");
    conf.setInt(CompositeBucketCache.CACHESIZE_L1, 32);
    conf.setInt(CompositeBucketCache.CACHESIZE_L2, 32);
    BlockCache blockCache = BlockCacheFactory.createBlockCache(conf);
    Assert.assertTrue(blockCache instanceof CompositeBucketCache);
    TestLruBlockCache.testMultiThreadGetAndEvictBlockInternal(blockCache);
  }

}
