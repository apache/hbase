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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests the value filter
 */
@Tag(FilterTests.TAG)
@Tag(SmallTests.TAG)
public class TestSingleColumnValueFilter {

  private static final byte[] ROW = Bytes.toBytes("test");
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("test");
  private static final byte[] COLUMN_QUALIFIER = Bytes.toBytes("foo");
  private static final byte[] VAL_1 = Bytes.toBytes("a");
  private static final byte[] VAL_2 = Bytes.toBytes("ab");
  private static final byte[] VAL_3 = Bytes.toBytes("abc");
  private static final byte[] VAL_4 = Bytes.toBytes("abcd");
  private static final byte[] FULLSTRING_1 =
    Bytes.toBytes("The quick brown fox jumps over the lazy dog.");
  private static final byte[] FULLSTRING_2 =
    Bytes.toBytes("The slow grey fox trips over the lazy dog.");
  private static final String QUICK_SUBSTR = "quick";
  private static final String QUICK_REGEX = ".+quick.+";
  private static final Pattern QUICK_PATTERN =
    Pattern.compile("QuIcK", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  Filter basicFilter;
  Filter nullFilter;
  Filter substrFilter;
  Filter regexFilter;
  Filter regexPatternFilter;

  @BeforeEach
  public void setUp() throws Exception {
    basicFilter = basicFilterNew();
    nullFilter = nullFilterNew();
    substrFilter = substrFilterNew();
    regexFilter = regexFilterNew();
    regexPatternFilter = regexFilterNew(QUICK_PATTERN);
  }

  private Filter basicFilterNew() {
    return new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_QUALIFIER,
      CompareOperator.GREATER_OR_EQUAL, VAL_2);
  }

  private Filter nullFilterNew() {
    return new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_QUALIFIER, CompareOperator.NOT_EQUAL,
      new NullComparator());
  }

  private Filter substrFilterNew() {
    return new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_QUALIFIER, CompareOperator.EQUAL,
      new SubstringComparator(QUICK_SUBSTR));
  }

  private Filter regexFilterNew() {
    return new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_QUALIFIER, CompareOperator.EQUAL,
      new RegexStringComparator(QUICK_REGEX));
  }

  private Filter regexFilterNew(Pattern pattern) {
    return new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_QUALIFIER, CompareOperator.EQUAL,
      new RegexStringComparator(pattern.pattern(), pattern.flags()));
  }

  @Test
  public void testLongComparator() throws IOException {
    Filter filter = new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_QUALIFIER,
      CompareOperator.GREATER, new LongComparator(100L));
    KeyValue cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, Bytes.toBytes(1L));
    assertEquals(Filter.ReturnCode.NEXT_ROW, filter.filterCell(cell), "less than");
    filter.reset();
    byte[] buffer = cell.getBuffer();
    Cell c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.NEXT_ROW, filter.filterCell(c), "less than");
    filter.reset();

    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, Bytes.toBytes(100L));
    assertEquals(Filter.ReturnCode.NEXT_ROW, filter.filterCell(cell), "Equals 100");
    filter.reset();
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.NEXT_ROW, filter.filterCell(c), "Equals 100");
    filter.reset();

    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, Bytes.toBytes(120L));
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(cell), "include 120");
    filter.reset();
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(c), "include 120");
  }

  private void basicFilterTests(SingleColumnValueFilter filter) throws Exception {
    KeyValue cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_2);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(cell), "basicFilter1");
    byte[] buffer = cell.getBuffer();
    Cell c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(c), "basicFilter1");
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_3);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(cell), "basicFilter2");
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(c), "basicFilter2");
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_4);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(cell), "basicFilter3");
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(c), "basicFilter3");
    assertFalse(filter.filterRow(), "basicFilterNotNull");
    filter.reset();
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_1);
    assertEquals(Filter.ReturnCode.NEXT_ROW, filter.filterCell(cell), "basicFilter4");
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.NEXT_ROW, filter.filterCell(c), "basicFilter4");
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_2);
    assertEquals(Filter.ReturnCode.NEXT_ROW, filter.filterCell(cell), "basicFilter4");
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.NEXT_ROW, filter.filterCell(c), "basicFilter4");
    assertFalse(filter.filterAllRemaining(), "basicFilterAllRemaining");
    assertTrue(filter.filterRow(), "basicFilterNotNull");
    filter.reset();
    filter.setLatestVersionOnly(false);
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_1);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(cell), "basicFilter5");
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(c), "basicFilter5");
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_2);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(cell), "basicFilter5");
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(c), "basicFilter5");
    assertFalse(filter.filterRow(), "basicFilterNotNull");
  }

  private void nullFilterTests(Filter filter) throws Exception {
    ((SingleColumnValueFilter) filter).setFilterIfMissing(true);
    KeyValue cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, FULLSTRING_1);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(cell), "null1");
    byte[] buffer = cell.getBuffer();
    Cell c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(c), "null1");
    assertFalse(filter.filterRow(), "null1FilterRow");
    filter.reset();
    cell = new KeyValue(ROW, COLUMN_FAMILY, Bytes.toBytes("qual2"), FULLSTRING_2);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(cell), "null2");
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(c), "null2");
    assertTrue(filter.filterRow(), "null2FilterRow");
  }

  private void substrFilterTests(Filter filter) throws Exception {
    KeyValue cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, FULLSTRING_1);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(cell), "substrTrue");
    byte[] buffer = cell.getBuffer();
    Cell c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(c), "substrTrue");
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, FULLSTRING_2);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(cell), "substrFalse");
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(c), "substrFalse");
    assertFalse(filter.filterAllRemaining(), "substrFilterAllRemaining");
    assertFalse(filter.filterRow(), "substrFilterNotNull");
  }

  private void regexFilterTests(Filter filter) throws Exception {
    KeyValue cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, FULLSTRING_1);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(cell), "regexTrue");
    byte[] buffer = cell.getBuffer();
    Cell c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(c), "regexTrue");
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, FULLSTRING_2);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(cell), "regexFalse");
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(c), "regexFalse");
    assertFalse(filter.filterAllRemaining(), "regexFilterAllRemaining");
    assertFalse(filter.filterRow(), "regexFilterNotNull");
  }

  private void regexPatternFilterTests(Filter filter) throws Exception {
    KeyValue cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, FULLSTRING_1);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(cell), "regexTrue");
    byte[] buffer = cell.getBuffer();
    Cell c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(c), "regexTrue");
    assertFalse(filter.filterAllRemaining(), "regexFilterAllRemaining");
    assertFalse(filter.filterRow(), "regexFilterNotNull");
  }

  private Filter serializationTest(Filter filter) throws Exception {
    // Decompose filter to bytes.
    byte[] buffer = filter.toByteArray();

    // Recompose filter.
    Filter newFilter = SingleColumnValueFilter.parseFrom(buffer);
    return newFilter;
  }

  /**
   * Tests identification of the stop row
   */
  @Test
  public void testStop() throws Exception {
    basicFilterTests((SingleColumnValueFilter) basicFilter);
    nullFilterTests(nullFilter);
    substrFilterTests(substrFilter);
    regexFilterTests(regexFilter);
    regexPatternFilterTests(regexPatternFilter);
  }

  /**
   * Tests serialization
   */
  @Test
  public void testSerialization() throws Exception {
    Filter newFilter = serializationTest(basicFilter);
    basicFilterTests((SingleColumnValueFilter) newFilter);
    newFilter = serializationTest(nullFilter);
    nullFilterTests(newFilter);
    newFilter = serializationTest(substrFilter);
    substrFilterTests(newFilter);
    newFilter = serializationTest(regexFilter);
    regexFilterTests(newFilter);
    newFilter = serializationTest(regexPatternFilter);
    regexPatternFilterTests(newFilter);
  }

}
