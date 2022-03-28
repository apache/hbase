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
package org.apache.hadoop.hbase.net;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestAddress {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAddress.class);

  @Test
  public void testGetHostWithoutDomain() {
    assertEquals("a:123",
        Address.fromParts("a.b.c", 123).toStringWithoutDomain());
    assertEquals("1:123",
        Address.fromParts("1.b.c", 123).toStringWithoutDomain());
    assertEquals("123.456.789.1:123",
        Address.fromParts("123.456.789.1", 123).toStringWithoutDomain());
    assertEquals("[2001:db8::1]:80",
        Address.fromParts("[2001:db8::1]:", 80).toStringWithoutDomain());
    assertEquals("[2001:db8::1]:80",
        Address.fromParts("[2001:db8::1]:", 80).toStringWithoutDomain());
  }
}
