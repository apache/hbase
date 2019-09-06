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
package org.apache.hadoop.hbase.hbtop.field;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(SmallTests.class)
public class FieldValueTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(FieldValueTest.class);

  @Test
  public void testParseAndAsSomethingMethod() {
    // String
    FieldValue stringFieldValue = new FieldValue("aaa", FieldValueType.STRING);
    assertThat(stringFieldValue.asString(), is("aaa"));

    try {
      new FieldValue(1, FieldValueType.STRING);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    // Integer
    FieldValue integerFieldValue = new FieldValue(100, FieldValueType.INTEGER);
    assertThat(integerFieldValue.asInt(), is(100));

    integerFieldValue = new FieldValue("100", FieldValueType.INTEGER);
    assertThat(integerFieldValue.asInt(), is(100));

    try {
      new FieldValue("aaa", FieldValueType.INTEGER);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    // Long
    FieldValue longFieldValue = new FieldValue(100L, FieldValueType.LONG);
    assertThat(longFieldValue.asLong(), is(100L));

    longFieldValue = new FieldValue("100", FieldValueType.LONG);
    assertThat(longFieldValue.asLong(), is(100L));

    try {
      new FieldValue("aaa", FieldValueType.LONG);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    try {
      new FieldValue(100, FieldValueType.LONG);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    // Float
    FieldValue floatFieldValue = new FieldValue(1.0f, FieldValueType.FLOAT);
    assertThat(floatFieldValue.asFloat(), is(1.0f));

    floatFieldValue = new FieldValue("1", FieldValueType.FLOAT);
    assertThat(floatFieldValue.asFloat(), is(1.0f));

    try {
      new FieldValue("aaa", FieldValueType.FLOAT);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    try {
      new FieldValue(1, FieldValueType.FLOAT);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    // Size
    FieldValue sizeFieldValue =
      new FieldValue(new Size(100, Size.Unit.MEGABYTE), FieldValueType.SIZE);
    assertThat(sizeFieldValue.asString(), is("100.0MB"));
    assertThat(sizeFieldValue.asSize(), is(new Size(100, Size.Unit.MEGABYTE)));

    sizeFieldValue = new FieldValue("100MB", FieldValueType.SIZE);
    assertThat(sizeFieldValue.asString(), is("100.0MB"));
    assertThat(sizeFieldValue.asSize(), is(new Size(100, Size.Unit.MEGABYTE)));

    try {
      new FieldValue("100", FieldValueType.SIZE);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    try {
      new FieldValue(100, FieldValueType.SIZE);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    // Percent
    FieldValue percentFieldValue =
      new FieldValue(100f, FieldValueType.PERCENT);
    assertThat(percentFieldValue.asString(), is("100.00%"));
    assertThat(percentFieldValue.asFloat(), is(100f));

    percentFieldValue = new FieldValue("100%", FieldValueType.PERCENT);
    assertThat(percentFieldValue.asString(), is("100.00%"));
    assertThat(percentFieldValue.asFloat(), is(100f));

    percentFieldValue = new FieldValue("100", FieldValueType.PERCENT);
    assertThat(percentFieldValue.asString(), is("100.00%"));
    assertThat(percentFieldValue.asFloat(), is(100f));

    try {
      new FieldValue(100, FieldValueType.PERCENT);
      fail();
    } catch (IllegalArgumentException ignored) {
    }
  }

  @Test
  public void testCompareTo() {
    // String
    FieldValue stringAFieldValue = new FieldValue("a", FieldValueType.STRING);
    FieldValue stringAFieldValue2 = new FieldValue("a", FieldValueType.STRING);
    FieldValue stringBFieldValue = new FieldValue("b", FieldValueType.STRING);
    FieldValue stringCapitalAFieldValue = new FieldValue("A", FieldValueType.STRING);

    assertThat(stringAFieldValue.compareTo(stringAFieldValue2), is(0));
    assertThat(stringBFieldValue.compareTo(stringAFieldValue), is(1));
    assertThat(stringAFieldValue.compareTo(stringBFieldValue), is(-1));
    assertThat(stringAFieldValue.compareTo(stringCapitalAFieldValue), is(32));

    // Integer
    FieldValue integer1FieldValue = new FieldValue(1, FieldValueType.INTEGER);
    FieldValue integer1FieldValue2 = new FieldValue(1, FieldValueType.INTEGER);
    FieldValue integer2FieldValue = new FieldValue(2, FieldValueType.INTEGER);

    assertThat(integer1FieldValue.compareTo(integer1FieldValue2), is(0));
    assertThat(integer2FieldValue.compareTo(integer1FieldValue), is(1));
    assertThat(integer1FieldValue.compareTo(integer2FieldValue), is(-1));

    // Long
    FieldValue long1FieldValue = new FieldValue(1L, FieldValueType.LONG);
    FieldValue long1FieldValue2 = new FieldValue(1L, FieldValueType.LONG);
    FieldValue long2FieldValue = new FieldValue(2L, FieldValueType.LONG);

    assertThat(long1FieldValue.compareTo(long1FieldValue2), is(0));
    assertThat(long2FieldValue.compareTo(long1FieldValue), is(1));
    assertThat(long1FieldValue.compareTo(long2FieldValue), is(-1));

    // Float
    FieldValue float1FieldValue = new FieldValue(1.0f, FieldValueType.FLOAT);
    FieldValue float1FieldValue2 = new FieldValue(1.0f, FieldValueType.FLOAT);
    FieldValue float2FieldValue = new FieldValue(2.0f, FieldValueType.FLOAT);

    assertThat(float1FieldValue.compareTo(float1FieldValue2), is(0));
    assertThat(float2FieldValue.compareTo(float1FieldValue), is(1));
    assertThat(float1FieldValue.compareTo(float2FieldValue), is(-1));

    // Size
    FieldValue size100MBFieldValue =
      new FieldValue(new Size(100, Size.Unit.MEGABYTE), FieldValueType.SIZE);
    FieldValue size100MBFieldValue2 =
      new FieldValue(new Size(100, Size.Unit.MEGABYTE), FieldValueType.SIZE);
    FieldValue size200MBFieldValue =
      new FieldValue(new Size(200, Size.Unit.MEGABYTE), FieldValueType.SIZE);

    assertThat(size100MBFieldValue.compareTo(size100MBFieldValue2), is(0));
    assertThat(size200MBFieldValue.compareTo(size100MBFieldValue), is(1));
    assertThat(size100MBFieldValue.compareTo(size200MBFieldValue), is(-1));

    // Percent
    FieldValue percent50FieldValue = new FieldValue(50.0f, FieldValueType.PERCENT);
    FieldValue percent50FieldValue2 = new FieldValue(50.0f, FieldValueType.PERCENT);
    FieldValue percent100FieldValue = new FieldValue(100.0f, FieldValueType.PERCENT);

    assertThat(percent50FieldValue.compareTo(percent50FieldValue2), is(0));
    assertThat(percent100FieldValue.compareTo(percent50FieldValue), is(1));
    assertThat(percent50FieldValue.compareTo(percent100FieldValue), is(-1));
  }

  @Test
  public void testPlus() {
    // String
    FieldValue stringFieldValue = new FieldValue("a", FieldValueType.STRING);
    FieldValue stringFieldValue2 = new FieldValue("b", FieldValueType.STRING);
    assertThat(stringFieldValue.plus(stringFieldValue2).asString(), is("ab"));

    // Integer
    FieldValue integerFieldValue = new FieldValue(1, FieldValueType.INTEGER);
    FieldValue integerFieldValue2 = new FieldValue(2, FieldValueType.INTEGER);
    assertThat(integerFieldValue.plus(integerFieldValue2).asInt(), is(3));

    // Long
    FieldValue longFieldValue = new FieldValue(1L, FieldValueType.LONG);
    FieldValue longFieldValue2 = new FieldValue(2L, FieldValueType.LONG);
    assertThat(longFieldValue.plus(longFieldValue2).asLong(), is(3L));

    // Float
    FieldValue floatFieldValue = new FieldValue(1.2f, FieldValueType.FLOAT);
    FieldValue floatFieldValue2 = new FieldValue(2.2f, FieldValueType.FLOAT);
    assertThat(floatFieldValue.plus(floatFieldValue2).asFloat(), is(3.4f));

    // Size
    FieldValue sizeFieldValue =
      new FieldValue(new Size(100, Size.Unit.MEGABYTE), FieldValueType.SIZE);
    FieldValue sizeFieldValue2 =
      new FieldValue(new Size(200, Size.Unit.MEGABYTE), FieldValueType.SIZE);
    assertThat(sizeFieldValue.plus(sizeFieldValue2).asString(), is("300.0MB"));
    assertThat(sizeFieldValue.plus(sizeFieldValue2).asSize(),
      is(new Size(300, Size.Unit.MEGABYTE)));

    // Percent
    FieldValue percentFieldValue = new FieldValue(30f, FieldValueType.PERCENT);
    FieldValue percentFieldValue2 = new FieldValue(60f, FieldValueType.PERCENT);
    assertThat(percentFieldValue.plus(percentFieldValue2).asString(), is("90.00%"));
    assertThat(percentFieldValue.plus(percentFieldValue2).asFloat(), is(90f));
  }

  @Test
  public void testCompareToIgnoreCase() {
    FieldValue stringAFieldValue = new FieldValue("a", FieldValueType.STRING);
    FieldValue stringCapitalAFieldValue = new FieldValue("A", FieldValueType.STRING);
    FieldValue stringCapitalBFieldValue = new FieldValue("B", FieldValueType.STRING);

    assertThat(stringAFieldValue.compareToIgnoreCase(stringCapitalAFieldValue), is(0));
    assertThat(stringCapitalBFieldValue.compareToIgnoreCase(stringAFieldValue), is(1));
    assertThat(stringAFieldValue.compareToIgnoreCase(stringCapitalBFieldValue), is(-1));
  }

  @Test
  public void testOptimizeSize() {
    FieldValue sizeFieldValue =
      new FieldValue(new Size(1, Size.Unit.BYTE), FieldValueType.SIZE);
    assertThat(sizeFieldValue.asString(), is("1.0B"));

    sizeFieldValue =
      new FieldValue(new Size(1024, Size.Unit.BYTE), FieldValueType.SIZE);
    assertThat(sizeFieldValue.asString(), is("1.0KB"));

    sizeFieldValue =
      new FieldValue(new Size(2 * 1024, Size.Unit.BYTE), FieldValueType.SIZE);
    assertThat(sizeFieldValue.asString(), is("2.0KB"));

    sizeFieldValue =
      new FieldValue(new Size(2 * 1024, Size.Unit.KILOBYTE), FieldValueType.SIZE);
    assertThat(sizeFieldValue.asString(), is("2.0MB"));

    sizeFieldValue =
      new FieldValue(new Size(1024 * 1024, Size.Unit.KILOBYTE), FieldValueType.SIZE);
    assertThat(sizeFieldValue.asString(), is("1.0GB"));

    sizeFieldValue =
      new FieldValue(new Size(2 * 1024 * 1024, Size.Unit.MEGABYTE), FieldValueType.SIZE);
    assertThat(sizeFieldValue.asString(), is("2.0TB"));

    sizeFieldValue =
      new FieldValue(new Size(2 * 1024, Size.Unit.TERABYTE), FieldValueType.SIZE);
    assertThat(sizeFieldValue.asString(), is("2.0PB"));

    sizeFieldValue =
      new FieldValue(new Size(1024 * 1024, Size.Unit.TERABYTE), FieldValueType.SIZE);
    assertThat(sizeFieldValue.asString(), is("1024.0PB"));

    sizeFieldValue =
      new FieldValue(new Size(1, Size.Unit.PETABYTE), FieldValueType.SIZE);
    assertThat(sizeFieldValue.asString(), is("1.0PB"));

    sizeFieldValue =
      new FieldValue(new Size(1024, Size.Unit.PETABYTE), FieldValueType.SIZE);
    assertThat(sizeFieldValue.asString(), is("1024.0PB"));
  }
}
