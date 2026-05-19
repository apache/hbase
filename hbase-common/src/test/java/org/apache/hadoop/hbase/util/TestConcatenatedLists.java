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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestConcatenatedLists {

  @Test
  public void testUnsupportedOps() {
    // If adding support, add tests.
    ConcatenatedLists<Long> c = new ConcatenatedLists<>();
    c.addSublist(Arrays.asList(0L, 1L));
    assertThrows(UnsupportedOperationException.class, () -> c.add(2L));
    assertThrows(UnsupportedOperationException.class, () -> c.addAll(Arrays.asList(2L, 3L)));
    assertThrows(UnsupportedOperationException.class, () -> c.remove(0L));
    assertThrows(UnsupportedOperationException.class, () -> c.removeAll(Arrays.asList(0L, 1L)));
    assertThrows(UnsupportedOperationException.class, () -> c.clear());
    assertThrows(UnsupportedOperationException.class, () -> c.retainAll(Arrays.asList(0L, 2L)));

    Iterator<Long> iter = c.iterator();
    iter.next();
    assertThrows(UnsupportedOperationException.class, () -> iter.remove());
  }

  @Test
  public void testEmpty() {
    verify(new ConcatenatedLists<>(), -1);
  }

  @Test
  public void testOneOne() {
    ConcatenatedLists<Long> c = new ConcatenatedLists<>();
    c.addSublist(Arrays.asList(0L));
    verify(c, 0);
  }

  @Test
  public void testOneMany() {
    ConcatenatedLists<Long> c = new ConcatenatedLists<>();
    c.addSublist(Arrays.asList(0L, 1L, 2L));
    verify(c, 2);
  }

  @Test
  public void testManyOne() {
    ConcatenatedLists<Long> c = new ConcatenatedLists<>();
    c.addSublist(Arrays.asList(0L));
    c.addAllSublists(Arrays.asList(Arrays.asList(1L), Arrays.asList(2L)));
    verify(c, 2);
  }

  @Test
  public void testManyMany() {
    ConcatenatedLists<Long> c = new ConcatenatedLists<>();
    c.addAllSublists(Arrays.asList(Arrays.asList(0L, 1L)));
    c.addSublist(Arrays.asList(2L, 3L, 4L));
    c.addAllSublists(Arrays.asList(Arrays.asList(5L), Arrays.asList(6L, 7L)));
    verify(c, 7);
  }

  @SuppressWarnings("ModifyingCollectionWithItself")
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "DMI_VACUOUS_SELF_COLLECTION_CALL",
      justification = "Intended vacuous containsAll call on 'c'")
  private void verify(ConcatenatedLists<Long> c, int last) {
    assertEquals((last == -1), c.isEmpty());
    assertEquals(last + 1, c.size());
    // This check is O(n^2), test with care
    assertTrue(c.containsAll(c));
    Long[] array = c.toArray(new Long[c.size()]);
    List<Long> all = new ArrayList<>();
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
    assertThrows(NoSuchElementException.class, () -> iter.next());
  }
}
