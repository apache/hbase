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
package org.apache.hadoop.hbase.filter;

import java.math.BigDecimal;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ FilterTests.class, SmallTests.class })
public class TestBigDecimalComparator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBigDecimalComparator.class);

  @Test
  public void testObjectEquals() {
    BigDecimal bd = new BigDecimal(Double.MIN_VALUE);
    // Check that equals returns true for identical objects
    final BigDecimalComparator bdc = new BigDecimalComparator(bd);
    Assert.assertTrue(bdc.equals(bdc));
    Assert.assertEquals(bdc.hashCode(), bdc.hashCode());

    // Check that equals returns true for the same object
    final BigDecimalComparator bdc1 = new BigDecimalComparator(bd);
    final BigDecimalComparator bdc2 = new BigDecimalComparator(bd);
    Assert.assertTrue(bdc1.equals(bdc2));
    Assert.assertEquals(bdc1.hashCode(), bdc2.hashCode());

    // Check that equals returns false for different objects
    final BigDecimalComparator bdc3 = new BigDecimalComparator(bd);
    final BigDecimalComparator bdc4 = new BigDecimalComparator(new BigDecimal(Long.MIN_VALUE));
    Assert.assertFalse(bdc3.equals(bdc4));
    Assert.assertNotEquals(bdc3.hashCode(), bdc4.hashCode());

    // Check that equals returns false for a different type
    final BigDecimalComparator bdc5 = new BigDecimalComparator(bd);
    Assert.assertFalse(bdc5.equals(0));
  }

  @Test
  public void testEqualsValue() {
    // given
    BigDecimal bd1 = new BigDecimal(Double.MAX_VALUE);
    BigDecimal bd2 = new BigDecimal(Double.MIN_VALUE);
    byte[] value1 = Bytes.toBytes(bd1);
    byte[] value2 = Bytes.toBytes(bd2);
    BigDecimalComparator comparator1 = new BigDecimalComparator(bd1);
    BigDecimalComparator comparator2 = new BigDecimalComparator(bd2);

    // when
    int comp1 = comparator1.compareTo(value1);
    int comp2 = comparator2.compareTo(value2);

    // then
    Assert.assertEquals(0, comp1);
    Assert.assertEquals(0, comp2);
  }

  @Test
  public void testGreaterThanValue() {
    // given
    byte[] val1 = Bytes.toBytes(new BigDecimal("1000000000000000000000000000000.9999999999999999"));
    byte[] val2 = Bytes.toBytes(new BigDecimal(0));
    byte[] val3 = Bytes.toBytes(new BigDecimal(Double.MIN_VALUE));
    BigDecimal bd = new BigDecimal(Double.MAX_VALUE);
    BigDecimalComparator comparator = new BigDecimalComparator(bd);

    // when
    int comp1 = comparator.compareTo(val1);
    int comp2 = comparator.compareTo(val2);
    int comp3 = comparator.compareTo(val3);

    // then
    Assert.assertEquals(1, comp1);
    Assert.assertEquals(1, comp2);
    Assert.assertEquals(1, comp3);
  }

  @Test
  public void testLessThanValue() {
    // given
    byte[] val1 = Bytes.toBytes(new BigDecimal("-1000000000000000000000000000000"));
    byte[] val2 = Bytes.toBytes(new BigDecimal(0));
    byte[] val3 = Bytes.toBytes(new BigDecimal(1));
    BigDecimal bd = new BigDecimal("-1000000000000000000000000000000.0000000000000001");
    BigDecimalComparator comparator = new BigDecimalComparator(bd);

    // when
    int comp1 = comparator.compareTo(val1);
    int comp2 = comparator.compareTo(val2);
    int comp3 = comparator.compareTo(val3);

    // then
    Assert.assertEquals(-1, comp1);
    Assert.assertEquals(-1, comp2);
    Assert.assertEquals(-1, comp3);
  }

}
