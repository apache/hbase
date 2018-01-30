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
public class TestReusablePoolMap extends PoolMapTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReusablePoolMap.class);

  @Override
  protected PoolType getPoolType() {
    return PoolType.Reusable;
  }

  @Test
  public void testSingleThreadedClient() throws InterruptedException, ExecutionException {
    Random rand = ThreadLocalRandom.current();
    String randomKey = String.valueOf(rand.nextInt());
    String randomValue = String.valueOf(rand.nextInt());
    // As long as we poll values we put, the pool size should remain zero
    runThread(randomKey, randomValue, randomValue);
    assertEquals(0, poolMap.size(randomKey));
  }

  @Test
  public void testMultiThreadedClients() throws InterruptedException, ExecutionException {
    Random rand = ThreadLocalRandom.current();
    // As long as we poll values we put, the pool size should remain zero
    for (int i = 0; i < POOL_SIZE; i++) {
      String randomKey = String.valueOf(rand.nextInt());
      String randomValue = String.valueOf(rand.nextInt());
      runThread(randomKey, randomValue, randomValue);
      assertEquals(0, poolMap.size(randomKey));
    }
    poolMap.clear();
    String randomKey = String.valueOf(rand.nextInt());
    for (int i = 0; i < POOL_SIZE - 1; i++) {
      String randomValue = String.valueOf(rand.nextInt());
      runThread(randomKey, randomValue, randomValue);
      assertEquals(0, poolMap.size(randomKey));
    }
    assertEquals(0, poolMap.size(randomKey));
  }

  @Test
  public void testPoolCap() throws InterruptedException, ExecutionException {
    Random rand = ThreadLocalRandom.current();
    // As long as we poll values we put, the pool size should remain zero
    String randomKey = String.valueOf(rand.nextInt());
    List<String> randomValues = new ArrayList<>();
    for (int i = 0; i < POOL_SIZE * 2; i++) {
      String randomValue = String.valueOf(rand.nextInt());
      randomValues.add(randomValue);
      runThread(randomKey, randomValue, randomValue);
    }
    assertEquals(0, poolMap.size(randomKey));
  }
}
