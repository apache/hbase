/*
 * Copyright The Apache Software Foundation
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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category({SmallTests.class})
public class TestClasses {

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
          HBaseClassTestRule.forClass(TestClasses.class);

  @Test
  public void testExtendedForName() throws ClassNotFoundException {
    assertEquals(int.class, Classes.extendedForName("int"));
    assertEquals(long.class, Classes.extendedForName("long"));
    assertEquals(char.class, Classes.extendedForName("char"));
    assertEquals(byte.class, Classes.extendedForName("byte"));
    assertEquals(short.class, Classes.extendedForName("short"));
    assertEquals(float.class, Classes.extendedForName("float"));
    assertEquals(double.class, Classes.extendedForName("double"));
    assertEquals(boolean.class, Classes.extendedForName("boolean"));

    thrown.expect(ClassNotFoundException.class);
    Classes.extendedForName("foo");
  }

  @Test
  public void testStringify() {
    assertEquals("", Classes.stringify(new Class[0]));
    assertEquals("NULL", Classes.stringify(null));
    assertEquals("java.lang.String,java.lang.Integer",
            Classes.stringify(new Class[]{String.class, Integer.class}));
  }
}
