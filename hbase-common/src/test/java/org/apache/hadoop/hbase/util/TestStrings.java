/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category({SmallTests.class})
public class TestStrings {

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
          HBaseClassTestRule.forClass(TestStrings.class);

  @Test
  public void testAppendKeyValue() {
    Assert.assertEquals("foo, bar=baz", Strings.appendKeyValue(
            new StringBuilder("foo"), "bar", "baz").toString());
    Assert.assertEquals("bar->baz", Strings.appendKeyValue(
            new StringBuilder(), "bar", "baz", "->", "| ").toString());
    Assert.assertEquals("foo, bar=baz", Strings.appendKeyValue(
            new StringBuilder("foo"), "bar", "baz", "=", ", ").toString());
    Assert.assertEquals("foo| bar->baz", Strings.appendKeyValue(
            new StringBuilder("foo"), "bar", "baz", "->", "| ").toString());
  }

  @Test
  public void testDomainNamePointerToHostName() {
    Assert.assertNull(Strings.domainNamePointerToHostName(null));
    Assert.assertEquals("foo",
            Strings.domainNamePointerToHostName("foo"));
    Assert.assertEquals("foo.com",
            Strings.domainNamePointerToHostName("foo.com"));
    Assert.assertEquals("foo.bar.com",
            Strings.domainNamePointerToHostName("foo.bar.com"));
    Assert.assertEquals("foo.bar.com",
            Strings.domainNamePointerToHostName("foo.bar.com."));
  }

  @Test
  public void testPadFront() {
    Assert.assertEquals("ddfoo", Strings.padFront("foo", 'd', 5));

    thrown.expect(IllegalArgumentException.class);
    Strings.padFront("foo", 'd', 1);
  }
}
