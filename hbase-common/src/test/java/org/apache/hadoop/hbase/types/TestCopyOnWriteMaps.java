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

package org.apache.hadoop.hbase.types;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

@Category(SmallTests.class)
public class TestCopyOnWriteMaps {

  private static final int MAX_RAND = 10 * 1000 * 1000;
  private ConcurrentNavigableMap<Long, Long> m;
  private ConcurrentSkipListMap<Long, Long> csm;

  @Before
  public void setUp() {
    m = new CopyOnWriteArrayMap<>();
    csm = new ConcurrentSkipListMap<>();

    for (  long i = 0 ; i < 10000; i++ ) {
      long o = ThreadLocalRandom.current().nextLong(MAX_RAND);
      m.put(i, o);
      csm.put(i,o);
    }
    long o = ThreadLocalRandom.current().nextLong(MAX_RAND);
    m.put(0L, o);
    csm.put(0L,o);
  }

  @Test
  public void testSize() throws Exception {
    assertEquals("Size should always be equal", m.size(), csm.size());
  }

  @Test
  public void testIsEmpty() throws Exception {
    m.clear();
    assertTrue(m.isEmpty());
    m.put(100L, 100L);
    assertFalse(m.isEmpty());
    m.remove(100L);
    assertTrue(m.isEmpty());
  }

  @Test
  public void testFindOnEmpty() throws Exception {
    m.clear();
    assertTrue(m.isEmpty());
    assertNull(m.get(100L));
    assertFalse(m.containsKey(100L));
    assertEquals(0, m.tailMap(100L).entrySet().size());
  }


  @Test
  public void testLowerKey() throws Exception {

    assertEquals(csm.lowerKey(400L), m.lowerKey(400L));
    assertEquals(csm.lowerKey(-1L), m.lowerKey(-1L));

    for ( int i =0 ; i < 100; i ++) {
      Long key = ThreadLocalRandom.current().nextLong();
      assertEquals(csm.lowerKey(key), m.lowerKey(key));
    }
  }

  @Test
  public void testFloorEntry() throws Exception {
    for ( int i =0 ; i < 100; i ++) {
      Long key = ThreadLocalRandom.current().nextLong();
      assertEquals(csm.floorEntry(key), m.floorEntry(key));
    }
  }

  @Test
  public void testFloorKey() throws Exception {
    for ( int i =0 ; i < 100; i ++) {
      Long key = ThreadLocalRandom.current().nextLong();
      assertEquals(csm.floorKey(key), m.floorKey(key));
    }
  }

  @Test
  public void testCeilingKey() throws Exception {

    assertEquals(csm.ceilingKey(4000L), m.ceilingKey(4000L));
    assertEquals(csm.ceilingKey(400L), m.ceilingKey(400L));
    assertEquals(csm.ceilingKey(-1L), m.ceilingKey(-1L));

    for ( int i =0 ; i < 100; i ++) {
      Long key = ThreadLocalRandom.current().nextLong();
      assertEquals(csm.ceilingKey(key), m.ceilingKey(key));
    }
  }

  @Test
  public void testHigherKey() throws Exception {

    assertEquals(csm.higherKey(4000L), m.higherKey(4000L));
    assertEquals(csm.higherKey(400L), m.higherKey(400L));
    assertEquals(csm.higherKey(-1L), m.higherKey(-1L));

    for ( int i =0 ; i < 100; i ++) {
      Long key = ThreadLocalRandom.current().nextLong();
      assertEquals(csm.higherKey(key), m.higherKey(key));
    }
  }

  @Test
  public void testRemove() throws Exception {
    for (Map.Entry<Long, Long> e:csm.entrySet()) {
      assertEquals(csm.remove(e.getKey()), m.remove(e.getKey()));
      assertEquals(null, m.remove(e.getKey()));
    }
  }

  @Test
  public void testReplace() throws Exception {
    for (Map.Entry<Long, Long> e:csm.entrySet()) {
      Long newValue = ThreadLocalRandom.current().nextLong();
      assertEquals(csm.replace(e.getKey(), newValue), m.replace(e.getKey(), newValue));
    }
    assertEquals(null, m.replace(MAX_RAND + 100L, ThreadLocalRandom.current().nextLong()));
  }

  @Test
  public void testReplace1() throws Exception {
    for (Map.Entry<Long, Long> e: csm.entrySet()) {
      Long newValue = ThreadLocalRandom.current().nextLong();
      assertEquals(csm.replace(e.getKey(), e.getValue() + 1, newValue),
          m.replace(e.getKey(), e.getValue() + 1, newValue));
      assertEquals(csm.replace(e.getKey(), e.getValue(), newValue),
          m.replace(e.getKey(), e.getValue(), newValue));
      assertEquals(newValue, m.get(e.getKey()));
      assertEquals(csm.get(e.getKey()), m.get(e.getKey()));
    }
    assertEquals(null, m.replace(MAX_RAND + 100L, ThreadLocalRandom.current().nextLong()));
  }

  @Test
  public void testMultiAdd() throws InterruptedException {

    Thread[] threads = new Thread[10];
    for ( int i =0 ; i<threads.length; i++) {
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          for ( int j = 0; j < 5000; j++) {
            m.put(ThreadLocalRandom.current().nextLong(),
                ThreadLocalRandom.current().nextLong());
          }
        }
      });
    }

    for (Thread thread1 : threads) {
      thread1.start();
    }

    for (Thread thread2 : threads) {
      thread2.join();
    }
  }

  @Test
  public void testFirstKey() throws Exception {
    assertEquals(csm.firstKey(), m.firstKey());
  }

  @Test
  public void testLastKey() throws Exception {
    assertEquals(csm.lastKey(), m.lastKey());
  }

  @Test
  public void testFirstEntry() throws Exception {
    assertEquals(csm.firstEntry().getKey(), m.firstEntry().getKey());
    assertEquals(csm.firstEntry().getValue(), m.firstEntry().getValue());
    assertEquals(csm.firstEntry(), m.firstEntry());
  }

  @Test
  public void testLastEntry() throws Exception {
    assertEquals(csm.lastEntry().getKey(), m.lastEntry().getKey());
    assertEquals(csm.lastEntry().getValue(), m.lastEntry().getValue());
    assertEquals(csm.lastEntry(), m.lastEntry());
  }

  @Test
  public void testKeys() throws Exception {
    for (Long key:csm.keySet()) {
      //assertTrue(m.containsKey(key));
      assertNotNull(m.get(key));
      assertNotNull(m.remove(key));
      assertNull(m.get(key));
    }
  }

  @Test
  public void testValues() throws Exception {
    for (Long value:m.values()) {
      assertTrue(csm.values().contains(value));
      assertTrue(m.containsValue(value));
    }
  }

  @Test
  public void testTailMap() throws Exception {
    Map<Long, Long> fromCsm = csm.tailMap(50L);
    Map<Long, Long> fromM = m.tailMap(50L);
    assertEquals(fromCsm, fromM);
    for (Long value:m.keySet()) {
      assertEquals(csm.tailMap(value), m.tailMap(value));
    }

    for (  long i = 0 ; i < 100; i++ ) {
      long o = ThreadLocalRandom.current().nextLong(MAX_RAND);
      assertEquals(csm.tailMap(o), m.tailMap(o));
    }
  }

  @Test
  public void testTailMapExclusive() throws Exception {
    m.clear();
    m.put(100L, 100L);
    m.put(101L, 101L);
    m.put(101L, 101L);
    m.put(103L, 103L);
    m.put(99L, 99L);
    m.put(102L, 102L);

    long n = 100L;
    CopyOnWriteArrayMap<Long,Long> tm99 = (CopyOnWriteArrayMap<Long, Long>) m.tailMap(99L, false);
    for (Map.Entry<Long,Long> e:tm99.entrySet()) {
      assertEquals(new Long(n), e.getKey());
      assertEquals(new Long(n), e.getValue());
      n++;
    }
  }

  @Test
  public void testTailMapInclusive() throws Exception {
    m.clear();
    m.put(100L, 100L);
    m.put(101L, 101L);
    m.put(101L, 101L);
    m.put(103L, 103L);
    m.put(99L, 99L);
    m.put(102L, 102L);

    long n = 102;
    CopyOnWriteArrayMap<Long,Long> tm102 = (CopyOnWriteArrayMap<Long, Long>) m.tailMap(102L, true);
    for (Map.Entry<Long,Long> e:tm102.entrySet()) {
      assertEquals(new Long(n), e.getKey());
      assertEquals(new Long(n), e.getValue());
      n++;
    }
    n = 99;
    CopyOnWriteArrayMap<Long,Long> tm98 = (CopyOnWriteArrayMap<Long, Long>) m.tailMap(98L, true);
    for (Map.Entry<Long,Long> e:tm98.entrySet()) {
      assertEquals(new Long(n), e.getKey());
      assertEquals(new Long(n), e.getValue());
      n++;
    }
  }

  @Test
  public void testPut() throws Exception {
    m.clear();
    m.put(100L, 100L);
    m.put(101L, 101L);
    m.put(101L, 101L);
    m.put(103L, 103L);
    m.put(99L, 99L);
    m.put(102L, 102L);
    long n = 99;

    for (Map.Entry<Long, Long> e:m.entrySet()) {
      assertEquals(new Long(n), e.getKey());
      assertEquals(new Long(n), e.getValue());
      n++;
    }
    assertEquals(5, m.size());
    assertEquals(false, m.isEmpty());
  }
}