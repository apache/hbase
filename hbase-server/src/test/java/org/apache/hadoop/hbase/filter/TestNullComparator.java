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
package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ FilterTests.class, SmallTests.class })
public class TestNullComparator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestNullComparator.class);

  @Test
  public void testNullValue() {
    // given
    byte[] value = null;
    NullComparator comparator = new NullComparator();

    // when
    int comp1 = comparator.compareTo(value);
    int comp2 = comparator.compareTo(value, 5, 15);

    // then
    Assert.assertEquals(0, comp1);
    Assert.assertEquals(0, comp2);
  }

  @Test
  public void testNonNullValue() {
    // given
    byte[] value = new byte[] { 0, 1, 2, 3, 4, 5 };
    NullComparator comparator = new NullComparator();

    // when
    int comp1 = comparator.compareTo(value);
    int comp2 = comparator.compareTo(value, 1, 3);

    // then
    Assert.assertEquals(1, comp1);
    Assert.assertEquals(1, comp2);
  }

  @Test
  public void testEmptyValue() {
    // given
    byte[] value = new byte[] { 0 };
    NullComparator comparator = new NullComparator();

    // when
    int comp1 = comparator.compareTo(value);
    int comp2 = comparator.compareTo(value, 1, 3);

    // then
    Assert.assertEquals(1, comp1);
    Assert.assertEquals(1, comp2);
  }

}
