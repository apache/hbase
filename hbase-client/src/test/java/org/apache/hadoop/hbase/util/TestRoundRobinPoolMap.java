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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.PoolMap.PoolType;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestRoundRobinPoolMap extends PoolMapTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRoundRobinPoolMap.class);

  @Override
  protected PoolType getPoolType() {
    return PoolType.RoundRobin;
  }

  @Test
  public void testSingleThreadedClient() throws InterruptedException, ExecutionException {
    Random rand = ThreadLocalRandom.current();
    String randomKey = String.valueOf(rand.nextInt());
    String randomValue = String.valueOf(rand.nextInt());
    // As long as the pool is not full, we'll get null back.
    // This forces the user to create new values that can be used to populate
    // the pool.
    runThread(randomKey, randomValue, null);
    assertEquals(1, poolMap.size(randomKey));
  }

  @Test
  public void testMultiThreadedClients() throws InterruptedException, ExecutionException {
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < POOL_SIZE; i++) {
      String randomKey = String.valueOf(rand.nextInt());
      String randomValue = String.valueOf(rand.nextInt());
      // As long as the pool is not full, we'll get null back
      runThread(randomKey, randomValue, null);
      // As long as we use distinct keys, each pool will have one value
      assertEquals(1, poolMap.size(randomKey));
    }
    poolMap.clear();
    String randomKey = String.valueOf(rand.nextInt());
    for (int i = 0; i < POOL_SIZE - 1; i++) {
      String randomValue = String.valueOf(rand.nextInt());
      // As long as the pool is not full, we'll get null back
      runThread(randomKey, randomValue, null);
      // since we use the same key, the pool size should grow
      assertEquals(i + 1, poolMap.size(randomKey));
    }
    // at the end of the day, there should be as many values as we put
    assertEquals(POOL_SIZE - 1, poolMap.size(randomKey));
  }

  @Test
  public void testPoolCap() throws InterruptedException, ExecutionException {
    Random rand = ThreadLocalRandom.current();
    String randomKey = String.valueOf(rand.nextInt());
    List<String> randomValues = new ArrayList<>();
    for (int i = 0; i < POOL_SIZE * 2; i++) {
      String randomValue = String.valueOf(rand.nextInt());
      randomValues.add(randomValue);
      if (i < POOL_SIZE - 1) {
        // As long as the pool is not full, we'll get null back
        runThread(randomKey, randomValue, null);
      } else {
        // when the pool becomes full, we expect the value we get back to be
        // what we put earlier, in round-robin order
        runThread(randomKey, randomValue, randomValues.get((i - POOL_SIZE + 1) % POOL_SIZE));
      }
    }
    assertEquals(POOL_SIZE, poolMap.size(randomKey));
  }
}
