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
package org.apache.hadoop.hbase.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(SmallTests.TAG)
public class TestFastStringPool {

  private static final Logger LOG = LoggerFactory.getLogger(TestFastStringPool.class);

  @Test
  public void testMultiThread() throws InterruptedException {
    FastStringPool pool = new FastStringPool();
    List<String> list1 = new ArrayList<>();
    List<String> list2 = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      list1.add("list1-" + i);
      list2.add("list2-" + i);
    }
    Map<String, String> interned1 = new HashMap<>();
    Map<String, String> interned2 = new HashMap<>();
    AtomicBoolean failed = new AtomicBoolean(false);
    int numThreads = 10;
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      threads.add(new Thread(() -> {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        for (int j = 0; j < 1000000; j++) {
          List<String> list;
          Map<String, String> interned;
          if (rand.nextBoolean()) {
            list = list1;
            interned = interned1;
          } else {
            list = list2;
            interned = interned2;
          }
          // create a new reference
          String k = new String(list.get(rand.nextInt(list.size())));
          String v = pool.intern(k);
          synchronized (interned) {
            String prev = interned.get(k);
            if (prev != null) {
              // should always return the same reference
              if (prev != v) {
                failed.set(true);
                String msg = "reference not equal, intern failed on string " + k;
                LOG.error(msg);
                throw new AssertionError(msg);
              }
            } else {
              interned.put(k, v);
            }
          }
        }
      }));
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    LOG.info("interned1 size {}, interned2 size {}, pool size {}", interned1.size(),
      interned2.size(), pool.size());
    assertEquals(interned1.size() + interned2.size(), pool.size());
    interned1.clear();
    list1.clear();
    LOG.info("clear interned1");
    // wait for at most 30 times
    for (int i = 0; i < 30; i++) {
      // invoke gc manually
      LOG.info("trigger GC");
      System.gc();
      Thread.sleep(1000);
      // should have cleaned up all the references for list1
      if (interned2.size() == pool.size()) {
        return;
      }
    }
    fail("should only have list2 strings in pool, expected pool size " + interned2.size()
      + ", but got " + pool.size());
  }
}
