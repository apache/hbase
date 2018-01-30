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
public class TestThreadLocalPoolMap extends PoolMapTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestThreadLocalPoolMap.class);

  @Override
  protected PoolType getPoolType() {
    return PoolType.ThreadLocal;
  }

  @Test
  public void testSingleThreadedClient() throws InterruptedException, ExecutionException {
    Random rand = ThreadLocalRandom.current();
    String randomKey = String.valueOf(rand.nextInt());
    String randomValue = String.valueOf(rand.nextInt());
    // As long as the pool is not full, we should get back what we put
    runThread(randomKey, randomValue, randomValue);
    assertEquals(1, poolMap.size(randomKey));
  }

  @Test
  public void testMultiThreadedClients() throws InterruptedException, ExecutionException {
    Random rand = ThreadLocalRandom.current();
    // As long as the pool is not full, we should get back what we put
    for (int i = 0; i < POOL_SIZE; i++) {
      String randomKey = String.valueOf(rand.nextInt());
      String randomValue = String.valueOf(rand.nextInt());
      runThread(randomKey, randomValue, randomValue);
      assertEquals(1, poolMap.size(randomKey));
    }
    String randomKey = String.valueOf(rand.nextInt());
    for (int i = 0; i < POOL_SIZE; i++) {
      String randomValue = String.valueOf(rand.nextInt());
      runThread(randomKey, randomValue, randomValue);
      assertEquals(i + 1, poolMap.size(randomKey));
    }
  }

  @Test
  public void testPoolCap() throws InterruptedException, ExecutionException {
    Random rand = ThreadLocalRandom.current();
    String randomKey = String.valueOf(rand.nextInt());
    for (int i = 0; i < POOL_SIZE * 2; i++) {
      String randomValue = String.valueOf(rand.nextInt());
      // as of HBASE-4150, pool limit is no longer used with ThreadLocalPool
      runThread(randomKey, randomValue, randomValue);
    }
    assertEquals(POOL_SIZE * 2, poolMap.size(randomKey));
  }
}
