/*
 *
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category({MiscTests.class, SmallTests.class})
public class TestConcatenatedLists {
  @Test
  public void testUnsupportedOps() {
    // If adding support, add tests.
    ConcatenatedLists<Long> c = new ConcatenatedLists<Long>();
    c.addSublist(Arrays.asList(0L, 1L));
    try {
      c.add(2L);
      fail("Should throw");
    } catch (UnsupportedOperationException ex) {
    }
    try {
      c.addAll(Arrays.asList(2L, 3L));
      fail("Should throw");
    } catch (UnsupportedOperationException ex) {
    }
    try {
      c.remove(0L);
      fail("Should throw");
    } catch (UnsupportedOperationException ex) {
    }
    try {
      c.removeAll(Arrays.asList(0L, 1L));
      fail("Should throw");
    } catch (UnsupportedOperationException ex) {
    }
    try {
      c.clear();
      fail("Should throw");
    } catch (UnsupportedOperationException ex) {
    }
    try {
      c.retainAll(Arrays.asList(0L, 1L));
      fail("Should throw");
    } catch (UnsupportedOperationException ex) {
    }
    Iterator<Long> iter = c.iterator();
    iter.next();
    try {
      iter.remove();
      fail("Should throw");
    } catch (UnsupportedOperationException ex) {
    }
  }

  @Test
  public void testEmpty() {
    verify(new ConcatenatedLists<Long>(), -1);
  }

  @Test
  public void testOneOne() {
    ConcatenatedLists<Long> c = new ConcatenatedLists<Long>();
    c.addSublist(Arrays.asList(0L));
    verify(c, 0);
  }

  @Test
  public void testOneMany() {
    ConcatenatedLists<Long> c = new ConcatenatedLists<Long>();
    c.addSublist(Arrays.asList(0L, 1L, 2L));
    verify(c, 2);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testManyOne() {
    ConcatenatedLists<Long> c = new ConcatenatedLists<Long>();
    c.addSublist(Arrays.asList(0L));
    c.addAllSublists(Arrays.asList(Arrays.asList(1L), Arrays.asList(2L)));
    verify(c, 2);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testManyMany() {
    ConcatenatedLists<Long> c = new ConcatenatedLists<Long>();
    c.addAllSublists(Arrays.asList(Arrays.asList(0L, 1L)));
    c.addSublist(Arrays.asList(2L, 3L, 4L));
    c.addAllSublists(Arrays.asList(Arrays.asList(5L), Arrays.asList(6L, 7L)));
    verify(c, 7);
  }

  private void verify(ConcatenatedLists<Long> c, int last) {
    assertEquals((last == -1), c.isEmpty());
    assertEquals(last + 1, c.size());
    assertTrue(c.containsAll(c));
    Long[] array = c.toArray(new Long[c.size()]);
    List<Long> all = new ArrayList<Long>();
    Iterator<Long> iter = c.iterator();
    for (Long i = 0L; i <= last; ++i) {
      assertTrue(iter.hasNext());
      assertEquals(i, iter.next());
      assertEquals(i, array[i.intValue()]);
      assertTrue(c.contains(i));
      assertTrue(c.containsAll(Arrays.asList(i)));
      all.add(i);
    }
    assertTrue(c.containsAll(all));
    assertFalse(iter.hasNext());
    try {
      iter.next();
      fail("Should have thrown");
    } catch (NoSuchElementException nsee) {
    }
  }
}
