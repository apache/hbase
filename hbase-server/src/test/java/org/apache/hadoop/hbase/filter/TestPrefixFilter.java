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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ FilterTests.class, SmallTests.class })
public class TestPrefixFilter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPrefixFilter.class);

  Filter mainFilter;
  static final char FIRST_CHAR = 'a';
  static final char LAST_CHAR = 'e';
  static final String HOST_PREFIX = "org.apache.site-";

  @Before
  public void setUp() throws Exception {
    this.mainFilter = new PrefixFilter(Bytes.toBytes(HOST_PREFIX));
  }

  @Test
  public void testPrefixOnRow() throws Exception {
    prefixRowTests(mainFilter);
  }

  @Test
  public void testPrefixOnRowInsideWhileMatchRow() throws Exception {
    prefixRowTests(new WhileMatchFilter(this.mainFilter), true);
  }

  @Test
  public void testSerialization() throws Exception {
    // Decompose mainFilter to bytes.
    byte[] buffer = mainFilter.toByteArray();

    // Recompose filter.
    Filter newFilter = PrefixFilter.parseFrom(buffer);

    // Ensure the serialization preserved the filter by running all test.
    prefixRowTests(newFilter);
  }

  private void prefixRowTests(Filter filter) throws Exception {
    prefixRowTests(filter, false);
  }

  private void prefixRowTests(Filter filter, boolean lastFilterAllRemaining) throws Exception {
    for (char c = FIRST_CHAR; c <= LAST_CHAR; c++) {
      byte[] t = createRow(c);
      assertFalse("Failed with character " + c,
        filter.filterRowKey(KeyValueUtil.createFirstOnRow(t)));
      assertFalse(filter.filterAllRemaining());
    }
    String yahooSite = "com.yahoo.www";
    byte[] yahooSiteBytes = Bytes.toBytes(yahooSite);
    KeyValue yahooSiteCell = KeyValueUtil.createFirstOnRow(yahooSiteBytes);
    assertFalse("Failed with character " + yahooSite, filter.filterRowKey(yahooSiteCell));
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, filter.filterCell(yahooSiteCell));
    assertEquals(lastFilterAllRemaining, filter.filterAllRemaining());
  }

  private byte[] createRow(final char c) {
    return Bytes.toBytes(HOST_PREFIX + Character.toString(c));
  }

  @Test
  public void shouldProvideHintWhenKeyBefore() {
    byte[] prefix = Bytes.toBytes("gg");
    PrefixFilter filter = new PrefixFilter(prefix);

    KeyValue cell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("aa"));

    // Should include this row so that filterCell() will be invoked.
    assertFalse(filter.filterRowKey(cell));
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, filter.filterCell(cell));
    Cell actualCellHint = filter.getNextCellHint(cell);
    assertNotNull(actualCellHint);
    Cell expectedCellHint = KeyValueUtil.createFirstOnRow(prefix);
    assertEquals(expectedCellHint, actualCellHint);
    assertFalse(filter.filterAllRemaining());
    assertTrue(filter.filterRow());
  }

  @Test
  public void shouldProvideHintWhenKeyBeforeAndShorter() {
    byte[] prefix = Bytes.toBytes("gggg");
    PrefixFilter filter = new PrefixFilter(prefix);

    KeyValue cell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("aa"));

    // Should include this row so that filterCell() will be invoked.
    assertFalse(filter.filterRowKey(cell));
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, filter.filterCell(cell));
    Cell actualCellHint = filter.getNextCellHint(cell);
    assertNotNull(actualCellHint);
    Cell expectedCellHint = KeyValueUtil.createFirstOnRow(prefix);
    assertEquals(expectedCellHint, actualCellHint);
    assertFalse(filter.filterAllRemaining());
    assertTrue(filter.filterRow());
  }

  @Test
  public void shouldIncludeWhenKeyMatches() {
    PrefixFilter filter = new PrefixFilter(Bytes.toBytes("gg"));

    KeyValue matchingCell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("gg"));

    assertFalse(filter.filterRowKey(matchingCell));
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(matchingCell));
    assertFalse(filter.filterAllRemaining());
    assertFalse(filter.filterRow());
  }

  @Test
  public void shouldReturnNextRowWhenKeyAfter() {
    PrefixFilter filter = new PrefixFilter(Bytes.toBytes("gg"));

    KeyValue afterCell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("pp"));

    assertTrue(filter.filterRowKey(afterCell));
    assertEquals(Filter.ReturnCode.NEXT_ROW, filter.filterCell(afterCell));
    assertTrue(filter.filterAllRemaining());
    assertTrue(filter.filterRow());
  }

  @Test
  public void shouldProvideHintWhenKeyBeforeReversed() {
    PrefixFilter filter = new PrefixFilter(Bytes.toBytes("aa"));
    filter.setReversed(true);

    KeyValue cell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("x"));

    // Should include this row so that filterCell() will be invoked.
    assertFalse(filter.filterRowKey(cell));
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, filter.filterCell(cell));
    Cell actualCellHint = filter.getNextCellHint(cell);
    assertNotNull(actualCellHint);
    Cell expectedCellHint = KeyValueUtil.createFirstOnRow(Bytes.toBytes("ab"));
    assertEquals(expectedCellHint, actualCellHint);
    assertFalse(filter.filterAllRemaining());
    assertTrue(filter.filterRow());
  }

  @Test
  public void hintShouldIncreaseLastNonMaxByteWhenReversed() {
    PrefixFilter filter = new PrefixFilter(new byte[] { 'a', 'a', Byte.MAX_VALUE });
    filter.setReversed(true);

    KeyValue cell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("x"));

    // Should include this row so that filterCell() will be invoked.
    assertFalse(filter.filterRowKey(cell));
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, filter.filterCell(cell));
    Cell actualCellHint = filter.getNextCellHint(cell);
    assertNotNull(actualCellHint);
    Cell expectedCellHint = KeyValueUtil.createFirstOnRow(new byte[] { 'a', 'b', Byte.MAX_VALUE });
    assertEquals(expectedCellHint, actualCellHint);
    assertFalse(filter.filterAllRemaining());
    assertTrue(filter.filterRow());
  }

  @Test
  public void shouldIncludeWhenKeyMatchesReversed() {
    PrefixFilter filter = new PrefixFilter(Bytes.toBytes("aa"));
    filter.setReversed(true);

    KeyValue matchingCell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("aa"));

    assertFalse(filter.filterRowKey(matchingCell));
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(matchingCell));
    assertFalse(filter.filterAllRemaining());
    assertFalse(filter.filterRow());
  }

  @Test
  public void shouldReturnNextRowWhenKeyAfterReversed() {
    PrefixFilter filter = new PrefixFilter(Bytes.toBytes("dd"));
    filter.setReversed(true);

    KeyValue cell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("aa"));

    assertTrue(filter.filterRowKey(cell));
    assertEquals(Filter.ReturnCode.NEXT_ROW, filter.filterCell(cell));
    assertTrue(filter.filterAllRemaining());
    assertTrue(filter.filterRow());
  }

  @Test
  public void hintShouldNotIncreaseMaxBytesWhenReversed() {
    PrefixFilter filter =
      new PrefixFilter(new byte[] { Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE });
    filter.setReversed(true);

    KeyValue cell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("x"));

    assertTrue(filter.filterRowKey(cell));
    assertEquals(Filter.ReturnCode.NEXT_ROW, filter.filterCell(cell));
    Cell actualCellHint = filter.getNextCellHint(cell);
    assertNotNull(actualCellHint);
    Cell expectedCellHint =
      KeyValueUtil.createFirstOnRow(new byte[] { Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE });
    assertEquals(expectedCellHint, actualCellHint);
    assertTrue(filter.filterAllRemaining());
    assertTrue(filter.filterRow());
  }

  @Test
  public void shouldNotThrowWhenCreatedWithNullPrefix() {
    PrefixFilter filter = new PrefixFilter(null);
    KeyValue cell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("doesNotMatter"));

    assertNull(filter.getNextCellHint(cell));
    filter.setReversed(true);
    assertNull(filter.getNextCellHint(cell));
  }

  @Test
  public void shouldNotThrowWhenCreatedWithEmptyByteArrayPrefix() {
    byte[] emptyPrefix = {};
    KeyValue emptyPrefixCell = KeyValueUtil.createFirstOnRow(emptyPrefix);
    KeyValue cell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("doesNotMatter"));

    PrefixFilter filter = new PrefixFilter(emptyPrefix);

    Cell forwardNextCellHint = filter.getNextCellHint(cell);
    assertNotNull(forwardNextCellHint);
    assertEquals(emptyPrefixCell, forwardNextCellHint);

    filter.setReversed(true);
    Cell reverseNextCellHint = filter.getNextCellHint(cell);
    assertNotNull(reverseNextCellHint);
    assertEquals(emptyPrefixCell, reverseNextCellHint);
  }
}
