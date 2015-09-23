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

import static org.junit.Assert.*;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.google.common.collect.Lists;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestSortedList {

  static class StringComparator implements Comparator<String> {
    @Override
    public int compare(String o1, String o2) {
      return o1.compareTo(o2);
    }
  }

  @Test
  public void testSorting() throws Exception {
    SortedList<String> list = new SortedList<String>(new StringComparator());
    list.add("c");
    list.add("d");
    list.add("a");
    list.add("b");

    assertEquals(4, list.size());
    assertArrayEquals(new String[]{"a", "b", "c", "d"}, list.toArray(new String[4]));

    list.add("c");
    assertEquals(5, list.size());
    assertArrayEquals(new String[]{"a", "b", "c", "c", "d"}, list.toArray(new String[5]));

    // Test that removal from head or middle maintains sort
    list.remove("b");
    assertEquals(4, list.size());
    assertArrayEquals(new String[]{"a", "c", "c", "d"}, list.toArray(new String[4]));
    list.remove("c");
    assertEquals(3, list.size());
    assertArrayEquals(new String[]{"a", "c", "d"}, list.toArray(new String[3]));
    list.remove("a");
    assertEquals(2, list.size());
    assertArrayEquals(new String[]{"c", "d"}, list.toArray(new String[2]));
  }

  @Test
  public void testReadOnlyIterators() throws Exception {
    SortedList<String> list = new SortedList<String>(
        Lists.newArrayList("a", "b", "c", "d", "e"), new StringComparator());

    Iterator<String> i = list.iterator();
    i.next();
    try {
      i.remove();
      fail("Iterator should have thrown an exception");
    } catch (UnsupportedOperationException e) {
      // ok
    }

    ListIterator<String> li = list.listIterator();
    li.next();
    try {
      li.add("a");
      fail("Iterator should have thrown an exception");
    } catch (UnsupportedOperationException e) {
      // ok
    }
    try {
      li.set("b");
      fail("Iterator should have thrown an exception");
    } catch (UnsupportedOperationException e) {
      // ok
    }
    try {
      li.remove();
      fail("Iterator should have thrown an exception");
    } catch (UnsupportedOperationException e) {
      // ok
    }
  }

  @Test
  public void testIteratorIsolation() throws Exception {
    SortedList<String> list = new SortedList<String>(
        Lists.newArrayList("a", "b", "c", "d", "e"), new StringComparator());

    // isolation of remove()
    Iterator<String> iter = list.iterator();
    list.remove("c");
    boolean found = false;
    while (iter.hasNext() && !found) {
      found = "c".equals(iter.next());
    }
    assertTrue(found);

    iter = list.iterator();
    found = false;
    while (iter.hasNext() && !found) {
      found = "c".equals(iter.next());
    }
    assertFalse(found);

    // isolation of add()
    iter = list.iterator();
    list.add("f");
    found = false;
    while (iter.hasNext() && !found) {
      String next = iter.next();
      found = "f".equals(next);
    }
    assertFalse(found);

    // isolation of addAll()
    iter = list.iterator();
    list.addAll(Lists.newArrayList("g", "h", "i"));
    found = false;
    while (iter.hasNext() && !found) {
      String next = iter.next();
      found = "g".equals(next) || "h".equals(next) || "i".equals(next);
    }
    assertFalse(found);

    // isolation of clear()
    iter = list.iterator();
    list.clear();
    assertEquals(0, list.size());
    int size = 0;
    while (iter.hasNext()) {
      iter.next();
      size++;
    }
    assertTrue(size > 0);
  }

  @Test
  public void testRandomAccessIsolation() throws Exception {
    SortedList<String> list = new SortedList<String>(
        Lists.newArrayList("a", "b", "c"), new StringComparator());
    List<String> innerList = list.get();
    assertEquals("a", innerList.get(0));
    assertEquals("b", innerList.get(1));
    list.clear();
    assertEquals("c", innerList.get(2));
  }
}

