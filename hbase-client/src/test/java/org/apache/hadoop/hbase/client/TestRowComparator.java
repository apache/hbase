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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ClientTests.class, SmallTests.class})
public class TestRowComparator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRowComparator.class);

  private static final List<byte[]> DEFAULT_ROWS = IntStream.range(1, 9)
    .mapToObj(String::valueOf).map(Bytes::toBytes).collect(Collectors.toList());

  @Test
  public void testPut() {
    test(row -> new Put(row));
  }

  @Test
  public void testDelete() {
    test(row -> new Delete(row));
  }

  @Test
  public void testAppend() {
    test(row -> new Append(row));
  }

  @Test
  public void testIncrement() {
    test(row -> new Increment(row));
  }

  @Test
  public void testGet() {
    test(row -> new Get(row));
  }

  private static <T extends Row> void test(Function<byte[], T> f) {
    List<T> rows = new ArrayList<T>(DEFAULT_ROWS.stream()
      .map(f).collect(Collectors.toList()));
    do {
      Collections.shuffle(rows);
    } while (needShuffle(rows));
    Collections.sort(rows, Row.COMPARATOR);
    assertSort(rows);
  }

  private static boolean needShuffle(List<? extends Row> rows) {
    assertFalse(rows.isEmpty());
    assertEquals(DEFAULT_ROWS.size(), rows.size());
    for (int i = 0; i != DEFAULT_ROWS.size(); ++i) {
      if (!Bytes.equals(DEFAULT_ROWS.get(i), rows.get(i).getRow())) {
        return false;
      }
    }
    return true;
  }

  private static void assertSort(List<? extends Row> rows) {
    assertFalse(rows.isEmpty());
    assertEquals(DEFAULT_ROWS.size(), rows.size());
    for (int i = 0; i != DEFAULT_ROWS.size(); ++i) {
      assertTrue(Bytes.equals(DEFAULT_ROWS.get(i), rows.get(i).getRow()));
    }
  }
}
