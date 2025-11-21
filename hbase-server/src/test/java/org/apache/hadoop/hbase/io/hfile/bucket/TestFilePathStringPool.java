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
package org.apache.hadoop.hbase.io.hfile.bucket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for {@link FilePathStringPool}
 */
@Category({ SmallTests.class })
public class TestFilePathStringPool {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFilePathStringPool.class);

  private FilePathStringPool pool;

  @Before
  public void setUp() {
    pool = FilePathStringPool.getInstance();
    pool.clear();
  }

  @Test
  public void testSingletonPattern() {
    FilePathStringPool instance1 = FilePathStringPool.getInstance();
    FilePathStringPool instance2 = FilePathStringPool.getInstance();
    assertNotNull(instance1);
    assertNotNull(instance2);
    assertEquals(instance1, instance2);
  }

  @Test
  public void testBasicEncodeDecodeRoundTrip() {
    String testString = "/hbase/data/default/test-table/region1/cf1/file1.hfile";
    int id = pool.encode(testString);
    String decoded = pool.decode(id);
    assertEquals(testString, decoded);
  }

  @Test
  public void testEncodeReturnsSameIdForSameString() {
    String testString = "/hbase/data/file.hfile";
    int id1 = pool.encode(testString);
    int id2 = pool.encode(testString);
    assertEquals(id1, id2);
    assertEquals(1, pool.size());
  }

  @Test
  public void testEncodeDifferentStringsGetDifferentIds() {
    String string1 = "/path/to/file1.hfile";
    String string2 = "/path/to/file2.hfile";
    int id1 = pool.encode(string1);
    int id2 = pool.encode(string2);
    assertNotEquals(id1, id2);
    assertEquals(2, pool.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEncodeNullStringThrowsException() {
    pool.encode(null);
  }

  @Test
  public void testDecodeNonExistentIdReturnsNull() {
    String decoded = pool.decode(999999);
    assertNull(decoded);
  }

  @Test
  public void testContainsWithId() {
    String testString = "/hbase/file.hfile";
    int id = pool.encode(testString);
    assertTrue(pool.contains(id));
    assertFalse(pool.contains(id + 1));
  }

  @Test
  public void testContainsWithString() {
    String testString = "/hbase/file.hfile";
    pool.encode(testString);
    assertTrue(pool.contains(testString));
    assertFalse(pool.contains("/hbase/other-file.hfile"));
  }

  @Test
  public void testRemoveExistingString() {
    String testString = "/hbase/file.hfile";
    int id = pool.encode(testString);
    assertEquals(1, pool.size());
    assertTrue(pool.contains(testString));
    boolean removed = pool.remove(testString);
    assertTrue(removed);
    assertEquals(0, pool.size());
    assertFalse(pool.contains(testString));
    assertFalse(pool.contains(id));
    assertNull(pool.decode(id));
  }

  @Test
  public void testRemoveNonExistentStringReturnsFalse() {
    boolean removed = pool.remove("/non/existent/file.hfile");
    assertFalse(removed);
  }

  @Test
  public void testRemoveNullStringReturnsFalse() {
    boolean removed = pool.remove(null);
    assertFalse(removed);
  }

  @Test
  public void testClear() {
    pool.encode("/file1.hfile");
    pool.encode("/file2.hfile");
    pool.encode("/file3.hfile");
    assertEquals(3, pool.size());
    pool.clear();
    assertEquals(0, pool.size());
  }

  @Test
  public void testSizeTracking() {
    assertEquals(0, pool.size());
    pool.encode("/file1.hfile");
    assertEquals(1, pool.size());
    pool.encode("/file2.hfile");
    assertEquals(2, pool.size());
    // Encoding same string should not increase size
    pool.encode("/file1.hfile");
    assertEquals(2, pool.size());
    pool.remove("/file1.hfile");
    assertEquals(1, pool.size());
    pool.clear();
    assertEquals(0, pool.size());
  }

  @Test
  public void testGetPoolStats() {
    String stats = pool.getPoolStats();
    assertEquals("No strings encoded", stats);
    pool.encode("/hbase/data/table1/file1.hfile");
    pool.encode("/hbase/data/table2/file2.hfile");
    stats = pool.getPoolStats();
    assertNotNull(stats);
    assertTrue(stats.contains("2 unique strings"));
    assertTrue(stats.contains("avg length:"));
  }

  @Test
  public void testConcurrentEncoding() throws InterruptedException {
    int numThreads = 10;
    int stringsPerThread = 100;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    ConcurrentHashMap<String, Integer> results = new ConcurrentHashMap<>();
    AtomicInteger errorCount = new AtomicInteger(0);

    for (int t = 0; t < numThreads; t++) {
      final int threadId = t;
      executor.submit(() -> {
        try {
          for (int i = 0; i < stringsPerThread; i++) {
            String string = "/thread" + threadId + "/file" + i + ".hfile";
            int id = pool.encode(string);
            results.put(string, id);
          }
        } catch (Exception e) {
          errorCount.incrementAndGet();
        } finally {
          doneLatch.countDown();
        }
      });
    }

    assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
    executor.shutdown();

    assertEquals(0, errorCount.get());
    assertEquals(numThreads * stringsPerThread, pool.size());
    assertEquals(numThreads * stringsPerThread, results.size());

    // Verify all strings can be decoded correctly
    for (Map.Entry<String, Integer> entry : results.entrySet()) {
      String decoded = pool.decode(entry.getValue());
      assertEquals(entry.getKey(), decoded);
    }
  }

  @Test
  public void testConcurrentEncodingSameStrings() throws InterruptedException {
    int numThreads = 20;
    String sharedString = "/shared/file.hfile";
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    Set<Integer> ids = ConcurrentHashMap.newKeySet();
    AtomicInteger errorCount = new AtomicInteger(0);

    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
        try {
          int id = pool.encode(sharedString);
          ids.add(id);
        } catch (Exception e) {
          errorCount.incrementAndGet();
        } finally {
          doneLatch.countDown();
        }
      });
    }

    doneLatch.await(10, TimeUnit.SECONDS);
    executor.shutdown();

    assertEquals(0, errorCount.get());
    // All threads should get the same ID
    assertEquals(1, ids.size());
    assertEquals(1, pool.size());
  }

  @Test
  public void testConcurrentRemoval() throws InterruptedException {
    // Pre-populate with strings
    List<String> strings = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String string = "/file" + i + ".hfile";
      strings.add(string);
      pool.encode(string);
    }
    assertEquals(100, pool.size());

    int numThreads = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    AtomicInteger successfulRemovals = new AtomicInteger(0);

    for (int t = 0; t < numThreads; t++) {
      final int threadId = t;
      executor.submit(() -> {
        try {
          for (int i = threadId * 10; i < (threadId + 1) * 10; i++) {
            if (pool.remove(strings.get(i))) {
              successfulRemovals.incrementAndGet();
            }
          }
        } catch (Exception e) {
          // Ignore
        } finally {
          doneLatch.countDown();
        }
      });
    }

    doneLatch.await(10, TimeUnit.SECONDS);
    executor.shutdown();

    assertEquals(100, successfulRemovals.get());
    assertEquals(0, pool.size());
  }

  @Test
  public void testBidirectionalMappingConsistency() {
    // Verify that both mappings stay consistent
    List<String> strings = new ArrayList<>();
    List<Integer> ids = new ArrayList<>();

    for (int i = 0; i < 50; i++) {
      String string = "/region" + (i % 5) + "/file" + i + ".hfile";
      strings.add(string);
      ids.add(pool.encode(string));
    }

    // Verify forward mapping (string -> id)
    for (int i = 0; i < strings.size(); i++) {
      int expectedId = ids.get(i);
      int actualId = pool.encode(strings.get(i));
      assertEquals(expectedId, actualId);
    }

    // Verify reverse mapping (id -> string)
    for (int i = 0; i < ids.size(); i++) {
      String expectedString = strings.get(i);
      String actualString = pool.decode(ids.get(i));
      assertEquals(expectedString, actualString);
    }
  }
}
