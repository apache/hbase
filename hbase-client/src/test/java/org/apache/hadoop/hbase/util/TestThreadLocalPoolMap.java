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

import java.io.IOException;
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
public class TestThreadLocalPoolMap extends PoolMapTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestThreadLocalPoolMap.class);

  @Override
  protected PoolType getPoolType() {
    return PoolType.ThreadLocal;
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
  public void testFull() throws IOException {
    String key = "key";
    String value = "value";

    String result = poolMap.getOrCreate(key, () -> value);
    assertEquals(value, result);

    String result2 = poolMap.getOrCreate(key, () -> {
      throw new IOException("must not call me");
    });

    assertEquals(value, result2);
    assertEquals(1, poolMap.values().size());
  }

  @Test
  public void testLocality() throws ExecutionException, InterruptedException {
    String key = "key";
    AtomicInteger id = new AtomicInteger();

    Runnable runnable = () -> {
      try {
        String myId = Integer.toString(id.getAndIncrement());

        for (int i = 0; i < 3; i++) {
          String result = poolMap.getOrCreate(key, () -> myId);
          assertEquals(myId, result);

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

    assertEquals(2, poolMap.values().size());
  }
}
