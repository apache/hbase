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
package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, SmallTests.class })
public class TestCacheStats {
  @Test
  public void testEvicted() {
    CacheStats cacheStats = new CacheStats("testEvicted");
    // ages in seconds
    long[] ages = { 5, 7 };

    cacheStats.evicted(System.nanoTime() - ages[0] * BlockCacheUtil.NANOS_PER_SECOND, false);
    Assert.assertEquals(5, cacheStats.getAgeAtEvictionSnapshot().getMean(), 0);

    cacheStats.evicted(System.nanoTime() - ages[1] * BlockCacheUtil.NANOS_PER_SECOND, false);
    Assert.assertEquals(6, cacheStats.getAgeAtEvictionSnapshot().getMean(), 0);
  }
}
