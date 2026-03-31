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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestStrings {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStrings.class);

  @Test
  public void testAppendKeyValue() {
    assertEquals("foo, bar=baz",
      Strings.appendKeyValue(new StringBuilder("foo"), "bar", "baz").toString());
    assertEquals("bar->baz",
      Strings.appendKeyValue(new StringBuilder(), "bar", "baz", "->", "| ").toString());
    assertEquals("foo, bar=baz",
      Strings.appendKeyValue(new StringBuilder("foo"), "bar", "baz", "=", ", ").toString());
    assertEquals("foo| bar->baz",
      Strings.appendKeyValue(new StringBuilder("foo"), "bar", "baz", "->", "| ").toString());
  }

  @Test
  public void testDomainNamePointerToHostName() {
    assertNull(Strings.domainNamePointerToHostName(null));
    assertEquals("foo", Strings.domainNamePointerToHostName("foo"));
    assertEquals("foo.com", Strings.domainNamePointerToHostName("foo.com"));
    assertEquals("foo.bar.com", Strings.domainNamePointerToHostName("foo.bar.com"));
    assertEquals("foo.bar.com", Strings.domainNamePointerToHostName("foo.bar.com."));
  }

  @Test
  public void testPadFront() {
    assertEquals("ddfoo", Strings.padFront("foo", 'd', 5));
    assertThrows(IllegalArgumentException.class, () -> Strings.padFront("foo", 'd', 1));
  }

  @Test
  public void testParseURIQueries() throws Exception {
    Map<String,
      String> queries = Strings.parseURIQueries(new URI("hbase+rpc://server01:123?a=1&b=2&a=3&"
        + URLEncoder.encode("& ?", StandardCharsets.UTF_8.name()) + "=&"
        + URLEncoder.encode("===", StandardCharsets.UTF_8.name())));
    assertEquals("1", queries.get("a"));
    assertEquals("2", queries.get("b"));
    assertEquals("", queries.get("& ?"));
    assertEquals("", queries.get("==="));
    assertEquals(4, queries.size());

    assertTrue(Strings.parseURIQueries(new URI("hbase+zk://zk1:2181/")).isEmpty());
    assertTrue(Strings.parseURIQueries(new URI("hbase+zk://zk1:2181/?")).isEmpty());
    assertTrue(Strings.parseURIQueries(new URI("hbase+zk://zk1:2181/?#anchor")).isEmpty());
  }

  @Test
  public void testApplyURIQueriesToConf() throws Exception {
    Configuration conf = new Configuration();
    Strings.applyURIQueriesToConf(new URI("hbase+zk://aaa:2181/root?a=1&b=2&c"), conf);
    assertEquals("1", conf.get("a"));
    assertEquals("2", conf.get("b"));
    assertEquals("", conf.get("c"));
  }
}
