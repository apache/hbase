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
  public void testSingleThreadedClient() throws InterruptedException {
    String key = "key";
    String value = "value";
    // As long as the pool is not full, we'll get null back
    // This forces the user to create new values that can be used to populate the pool.
    runThread(key, value, null);
    assertEquals(1, poolMap.size(key));
    assertEquals(1, poolMap.size());
  }

  @Test
  public void testMultiThreadedClients() throws InterruptedException {
    for (int i = 0; i < KEY_COUNT; i++) {
      String key = Integer.toString(i);
      String value = Integer.toString(2 * i);
      // As long as the pool is not full, we'll get null back
      // This forces the user to create new values that can be used to populate the pool.
      runThread(key, value, null);
      assertEquals(1, poolMap.size(key));
    }

    assertEquals(KEY_COUNT, poolMap.size());
    poolMap.clear();

    String key = "key";
    for (int i = 0; i < POOL_SIZE - 1; i++) {
      String value = Integer.toString(i);
      // As long as the pool is not full, we'll get null back
      runThread(key, value, null);
      // since we use the same key, the pool size should grow
      assertEquals(i + 1, poolMap.size(key));
    }
    // at the end of the day, there should be as many values as we put
    assertEquals(POOL_SIZE - 1, poolMap.size(key));
    assertEquals(1, poolMap.size());
  }

  @Test
  public void testPoolCap() throws InterruptedException {
    String key = "key";
    // filling pool
    for (int i = 0; i < POOL_SIZE; i++) {
      String value = Integer.toString(i);
      String expected = (i < POOL_SIZE - 1) ? null : "0";
      runThread(key, value, expected);
    }

    assertEquals(POOL_SIZE, poolMap.size(key));
    assertEquals(1, poolMap.size());

    // pool is filled, get() should return elements round robin order
    // starting from 1, because the first get was called by runThread()
    for (int i = 1; i < 2 * POOL_SIZE; i++) {
      String expected = Integer.toString(i % POOL_SIZE);
      assertEquals(expected, poolMap.get(key));
    }
  }
}
