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
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
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
  public void testGetOrCreate() throws IOException {
    String key = "key";
    String value = "value";
    String result = poolMap.getOrCreate(key, () -> value);

    assertEquals(value, result);
    assertEquals(1, poolMap.values().size());
  }

  @Test
  public void testMultipleKeys() throws IOException {
    for (int i = 0; i < KEY_COUNT; i++) {
      String key = Integer.toString(i);
      String value = Integer.toString(2 * i);
      String result = poolMap.getOrCreate(key, () -> value);

      assertEquals(value, result);
    }

    assertEquals(KEY_COUNT, poolMap.values().size());
  }

  @Test
  public void testMultipleValues() throws IOException {
    String key = "key";

    for (int i = 0; i < POOL_SIZE; i++) {
      String value = Integer.toString(i);
      String result = poolMap.getOrCreate(key, () -> value);

      assertEquals(value, result);
    }

    assertEquals(POOL_SIZE, poolMap.values().size());
  }

  @Test
  public void testRoundRobin() throws IOException {
    String key = "key";

    for (int i = 0; i < POOL_SIZE; i++) {
      String value = Integer.toString(i);
      poolMap.getOrCreate(key, () -> value);
    }

    assertEquals(POOL_SIZE, poolMap.values().size());

    /* pool is filled, get() should return elements round robin order */
    for (int i = 0; i < 2 * POOL_SIZE; i++) {
      String expected = Integer.toString(i % POOL_SIZE);
      assertEquals(expected, poolMap.getOrCreate(key, () -> {
        throw new IOException("must not call me");
      }));
    }

    assertEquals(POOL_SIZE, poolMap.values().size());
  }

  @Test
  public void testMultiThreadedRoundRobin() throws ExecutionException, InterruptedException {
    String key = "key";
    AtomicInteger id = new AtomicInteger();
    List<String> results = Collections.synchronizedList(new ArrayList<>());

    Runnable runnable = () -> {
      try {
        for (int i = 0; i < POOL_SIZE; i++) {
          String value = Integer.toString(id.getAndIncrement());
          String result = poolMap.getOrCreate(key, () -> value);
          results.add(result);

          Thread.yield();
        }
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    };

    CompletableFuture<Void> future1 = CompletableFuture.runAsync(runnable);
    CompletableFuture<Void> future2 = CompletableFuture.runAsync(runnable);

    /* test for successful completion */
    future1.get();
    future2.get();

    assertEquals(POOL_SIZE, poolMap.values().size());

    /* check every elements occur twice */
    Collections.sort(results);
    Iterator<String> iterator = results.iterator();

    for (int i = 0; i < POOL_SIZE; i++) {
      String next1 = iterator.next();
      String next2 = iterator.next();
      assertEquals(next1, next2);
    }

    assertFalse(iterator.hasNext());
  }
}
