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
package org.apache.hadoop.hbase.metrics.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(SmallTests.class)
public class TestRefCountingMap {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRefCountingMap.class);

  private RefCountingMap<String, String> map;

  @Before
  public void setUp() {
    map = new RefCountingMap<>();
  }

  @Test
  public void testPutGet() {
    map.put("foo", () -> "foovalue");

    String v = map.get("foo");
    assertNotNull(v);
    assertEquals("foovalue", v);
  }

  @Test
  public void testPutMulti() {
    String v1 = map.put("foo", () -> "foovalue");
    String v2 =  map.put("foo", () -> "foovalue2");
    String v3 = map.put("foo", () -> "foovalue3");

    String v = map.get("foo");
    assertEquals("foovalue", v);
    assertEquals(v, v1);
    assertEquals(v, v2);
    assertEquals(v, v3);
  }

  @Test
  public void testPutRemove() {
    map.put("foo", () -> "foovalue");
    String v = map.remove("foo");
    assertNull(v);
    v = map.get("foo");
    assertNull(v);
  }

  @Test
  public void testPutRemoveMulti() {
    map.put("foo", () -> "foovalue");
    map.put("foo", () -> "foovalue2");
    map.put("foo", () -> "foovalue3");

    // remove 1
    String v = map.remove("foo");
    assertEquals("foovalue", v);

    // remove 2
    v = map.remove("foo");
    assertEquals("foovalue", v);

    // remove 3
    v = map.remove("foo");
    assertNull(v);
    v = map.get("foo");
    assertNull(v);
  }

  @Test
  public void testSize() {
    assertEquals(0, map.size());

    // put a key
    map.put("foo", () -> "foovalue");
    assertEquals(1, map.size());

    // put a different key
    map.put("bar", () -> "foovalue2");
    assertEquals(2, map.size());

    // put the same key again
    map.put("bar", () -> "foovalue3");
    assertEquals(2, map.size()); // map should be same size
  }

  @Test
  public void testClear() {
    map.put("foo", () -> "foovalue");
    map.put("bar", () -> "foovalue2");
    map.put("baz", () -> "foovalue3");

    map.clear();

    assertEquals(0, map.size());
  }


  @Test
  public void testKeySet() {
    map.put("foo", () -> "foovalue");
    map.put("bar", () -> "foovalue2");
    map.put("baz", () -> "foovalue3");

    Set<String> keys = map.keySet();
    assertEquals(3, keys.size());

    Lists.newArrayList("foo", "bar", "baz").stream().forEach(v -> assertTrue(keys.contains(v)));
  }

  @Test
  public void testValues() {
    map.put("foo", () -> "foovalue");
    map.put("foo", () -> "foovalue2");
    map.put("bar", () -> "foovalue3");
    map.put("baz", () -> "foovalue4");

    Collection<String> values = map.values();
    assertEquals(3, values.size());

    Lists.newArrayList("foovalue", "foovalue3", "foovalue4").stream()
            .forEach(v -> assertTrue(values.contains(v)));
  }
}
