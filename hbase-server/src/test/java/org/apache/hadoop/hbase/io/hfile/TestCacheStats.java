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

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestCacheStats {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCacheStats.class);

  @Test
  public void testPeriodicMetrics() throws Exception {
    CacheStats cacheStats = new CacheStats("test", 5, 1, TimeUnit.SECONDS);
    cacheStats.hit(false, false, BlockType.DATA);
    cacheStats.hit(false, false, BlockType.DATA);
    cacheStats.hit(false, false, BlockType.DATA);
    cacheStats.miss(false, false, BlockType.DATA);
    // first period should have a 75% hit, 25% miss
    Thread.sleep(1001);
    cacheStats.hit(false, false, BlockType.DATA);
    cacheStats.hit(false, false, BlockType.DATA);
    cacheStats.miss(false, false, BlockType.DATA);
    cacheStats.miss(false, false, BlockType.DATA);
    Thread.sleep(1001);
    cacheStats.hit(false, false, BlockType.DATA);
    cacheStats.miss(false, false, BlockType.DATA);
    cacheStats.miss(false, false, BlockType.DATA);
    cacheStats.miss(false, false, BlockType.DATA);
    Thread.sleep(1001);
    cacheStats.hit(false, false, BlockType.DATA);
    cacheStats.hit(false, false, BlockType.DATA);
    cacheStats.hit(false, false, BlockType.DATA);
    cacheStats.hit(false, false, BlockType.DATA);
    Thread.sleep(1001);
    cacheStats.miss(false, false, BlockType.DATA);
    cacheStats.miss(false, false, BlockType.DATA);
    cacheStats.miss(false, false, BlockType.DATA);
    cacheStats.miss(false, false, BlockType.DATA);
    Thread.sleep(1001);
    cacheStats.getMetricsRollerScheduler().shutdownNow();
    long[] hitCounts = cacheStats.getHitCounts();
    long[] requestCounts = cacheStats.getRequestCounts();
    assertEquals(5, hitCounts.length);
    assertEquals(5, requestCounts.length);
    assertEquals(3, hitCounts[0]);
    assertEquals(2, hitCounts[1]);
    assertEquals(1, hitCounts[2]);
    assertEquals(4, hitCounts[3]);
    assertEquals(0, hitCounts[4]);
    assertEquals(10, cacheStats.getHitCount());
    assertEquals(0.5, cacheStats.getHitRatioPastNPeriods(), 0.01);
  }
}
