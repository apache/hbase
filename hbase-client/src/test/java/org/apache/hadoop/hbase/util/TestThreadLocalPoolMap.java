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
  public void testSingleThreadedClient() throws InterruptedException {
    Random rand = ThreadLocalRandom.current();
    String key = "key";
    String value = "value";
    runThread(key, () -> value, value);
    assertEquals(1, poolMap.values().size());
  }

  @Test
  public void testMultiThreadedClients() throws InterruptedException {
    for (int i = 0; i < KEY_COUNT; i++) {
      String key = Integer.toString(i);
      String value = Integer.toString(2 * i);
      runThread(key, () -> value, value);
    }

    assertEquals(KEY_COUNT, poolMap.values().size());
    poolMap.clear();

    String key = "key";
    for (int i = 0; i < POOL_SIZE; i++) {
      String value = Integer.toString(i);
      runThread(key, () -> value, value);
    }

    assertEquals(POOL_SIZE, poolMap.values().size());
  }

  @Test
  public void testPoolCap() throws InterruptedException {
    String key = "key";
    for (int i = 0; i < POOL_SIZE * 2; i++) {
      String value = Integer.toString(i);
      runThread(key, () -> value, value);
    }
    /* we never discard values automatically, even if the thread exited */
    assertEquals(POOL_SIZE * 2, poolMap.values().size());
  }
}
