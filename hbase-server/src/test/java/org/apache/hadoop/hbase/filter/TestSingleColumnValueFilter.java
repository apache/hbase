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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the value filter
 */
@Category({FilterTests.class, SmallTests.class})
public class TestSingleColumnValueFilter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSingleColumnValueFilter.class);

  private static final byte[] ROW = Bytes.toBytes("test");
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("test");
  private static final byte [] COLUMN_QUALIFIER = Bytes.toBytes("foo");
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
  private static final Pattern QUICK_PATTERN = Pattern.compile("QuIcK", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  Filter basicFilter;
  Filter nullFilter;
  Filter substrFilter;
  Filter regexFilter;
  Filter regexPatternFilter;

  @Before
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
    return new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_QUALIFIER,
    CompareOperator.EQUAL,
      new SubstringComparator(QUICK_SUBSTR));
  }

  private Filter regexFilterNew() {
    return new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_QUALIFIER,
    CompareOperator.EQUAL,
      new RegexStringComparator(QUICK_REGEX));
  }

  private Filter regexFilterNew(Pattern pattern) {
    return new SingleColumnValueFilter(COLUMN_FAMILY, COLUMN_QUALIFIER,
    CompareOperator.EQUAL,
        new RegexStringComparator(pattern.pattern(), pattern.flags()));
  }

  @Test
  public void testLongComparator() throws IOException {
    Filter filter = new SingleColumnValueFilter(COLUMN_FAMILY,
        COLUMN_QUALIFIER, CompareOperator.GREATER, new LongComparator(100L));
    KeyValue cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER,
      Bytes.toBytes(1L));
    assertTrue("less than", filter.filterCell(cell) == Filter.ReturnCode.NEXT_ROW);
    filter.reset();
    byte[] buffer = cell.getBuffer();
    Cell c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("less than", filter.filterCell(c) == Filter.ReturnCode.NEXT_ROW);
    filter.reset();

    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER,
      Bytes.toBytes(100L));
    assertTrue("Equals 100", filter.filterCell(cell) == Filter.ReturnCode.NEXT_ROW);
    filter.reset();
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("Equals 100", filter.filterCell(c) == Filter.ReturnCode.NEXT_ROW);
    filter.reset();

    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER,
      Bytes.toBytes(120L));
    assertTrue("include 120", filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    filter.reset();
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("include 120", filter.filterCell(c) == Filter.ReturnCode.INCLUDE);
  }

  private void basicFilterTests(SingleColumnValueFilter filter)
      throws Exception {
    KeyValue cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_2);
    assertTrue("basicFilter1", filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    byte[] buffer = cell.getBuffer();
    Cell c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("basicFilter1", filter.filterCell(c) == Filter.ReturnCode.INCLUDE);
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_3);
    assertTrue("basicFilter2", filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("basicFilter2", filter.filterCell(c) == Filter.ReturnCode.INCLUDE);
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_4);
    assertTrue("basicFilter3", filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("basicFilter3", filter.filterCell(c) == Filter.ReturnCode.INCLUDE);
    assertFalse("basicFilterNotNull", filter.filterRow());
    filter.reset();
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_1);
    assertTrue("basicFilter4", filter.filterCell(cell) == Filter.ReturnCode.NEXT_ROW);
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("basicFilter4", filter.filterCell(c) == Filter.ReturnCode.NEXT_ROW);
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_2);
    assertTrue("basicFilter4", filter.filterCell(cell) == Filter.ReturnCode.NEXT_ROW);
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("basicFilter4", filter.filterCell(c) == Filter.ReturnCode.NEXT_ROW);
    assertFalse("basicFilterAllRemaining", filter.filterAllRemaining());
    assertTrue("basicFilterNotNull", filter.filterRow());
    filter.reset();
    filter.setLatestVersionOnly(false);
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_1);
    assertTrue("basicFilter5", filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("basicFilter5", filter.filterCell(c) == Filter.ReturnCode.INCLUDE);
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VAL_2);
    assertTrue("basicFilter5", filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("basicFilter5", filter.filterCell(c) == Filter.ReturnCode.INCLUDE);
    assertFalse("basicFilterNotNull", filter.filterRow());
  }

  private void nullFilterTests(Filter filter) throws Exception {
    ((SingleColumnValueFilter) filter).setFilterIfMissing(true);
    KeyValue cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, FULLSTRING_1);
    assertTrue("null1", filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    byte[] buffer = cell.getBuffer();
    Cell c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("null1", filter.filterCell(c) == Filter.ReturnCode.INCLUDE);
    assertFalse("null1FilterRow", filter.filterRow());
    filter.reset();
    cell = new KeyValue(ROW, COLUMN_FAMILY, Bytes.toBytes("qual2"), FULLSTRING_2);
    assertTrue("null2", filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("null2", filter.filterCell(c) == Filter.ReturnCode.INCLUDE);
    assertTrue("null2FilterRow", filter.filterRow());
  }

  private void substrFilterTests(Filter filter)
      throws Exception {
    KeyValue cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER,
      FULLSTRING_1);
    assertTrue("substrTrue",
      filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    byte[] buffer = cell.getBuffer();
    Cell c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("substrTrue", filter.filterCell(c) == Filter.ReturnCode.INCLUDE);
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER,
      FULLSTRING_2);
    assertTrue("substrFalse", filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("substrFalse", filter.filterCell(c) == Filter.ReturnCode.INCLUDE);
    assertFalse("substrFilterAllRemaining", filter.filterAllRemaining());
    assertFalse("substrFilterNotNull", filter.filterRow());
  }

  private void regexFilterTests(Filter filter)
      throws Exception {
    KeyValue cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER,
      FULLSTRING_1);
    assertTrue("regexTrue",
      filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    byte[] buffer = cell.getBuffer();
    Cell c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("regexTrue", filter.filterCell(c) == Filter.ReturnCode.INCLUDE);
    cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER,
      FULLSTRING_2);
    assertTrue("regexFalse", filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    buffer = cell.getBuffer();
    c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("regexFalse", filter.filterCell(c) == Filter.ReturnCode.INCLUDE);
    assertFalse("regexFilterAllRemaining", filter.filterAllRemaining());
    assertFalse("regexFilterNotNull", filter.filterRow());
  }

  private void regexPatternFilterTests(Filter filter)
      throws Exception {
    KeyValue cell = new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER,
      FULLSTRING_1);
    assertTrue("regexTrue",
      filter.filterCell(cell) == Filter.ReturnCode.INCLUDE);
    byte[] buffer = cell.getBuffer();
    Cell c = new ByteBufferKeyValue(ByteBuffer.wrap(buffer), 0, buffer.length);
    assertTrue("regexTrue", filter.filterCell(c) == Filter.ReturnCode.INCLUDE);
    assertFalse("regexFilterAllRemaining", filter.filterAllRemaining());
    assertFalse("regexFilterNotNull", filter.filterRow());
  }

  private Filter serializationTest(Filter filter)
      throws Exception {
    // Decompose filter to bytes.
    byte[] buffer = filter.toByteArray();

    // Recompose filter.
    Filter newFilter = SingleColumnValueFilter.parseFrom(buffer);
    return newFilter;
  }

  /**
   * Tests identification of the stop row
   * @throws Exception
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
   * @throws Exception
   */
  @Test
  public void testSerialization() throws Exception {
    Filter newFilter = serializationTest(basicFilter);
    basicFilterTests((SingleColumnValueFilter)newFilter);
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

