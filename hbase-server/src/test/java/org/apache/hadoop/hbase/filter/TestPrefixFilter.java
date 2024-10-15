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

import static org.junit.Assert.*;

import java.io.IOException;
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
    assertTrue("Failed with character " + yahooSite,
      filter.filterRowKey(KeyValueUtil.createFirstOnRow(yahooSiteBytes)));
    assertEquals(filter.filterAllRemaining(), lastFilterAllRemaining);
  }

  private byte[] createRow(final char c) {
    return Bytes.toBytes(HOST_PREFIX + Character.toString(c));
  }

  @Test
  public void shouldReturnSeekNextUsingHintWhenKeyBefore() throws IOException {
    KeyValue cell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("com.yahoo.www.a."));

    // Should include this row so that filterCell() will be invoked.
    assertFalse(mainFilter.filterRowKey(cell));
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, mainFilter.filterCell(cell));
    Cell actualCellHint = mainFilter.getNextCellHint(cell);
    assertNotNull(actualCellHint);
    Cell expectedCellHint = KeyValueUtil.createFirstOnRow(Bytes.toBytes(HOST_PREFIX));
    assertEquals(expectedCellHint, actualCellHint);
    assertFalse(mainFilter.filterAllRemaining());
  }

  @Test
  public void shouldReturnIncludeWhenKeyMatches() throws IOException {
    KeyValue matchingCell = KeyValueUtil.createFirstOnRow(createRow('a'));

    assertFalse(mainFilter.filterRowKey(matchingCell));
    assertEquals(Filter.ReturnCode.INCLUDE, mainFilter.filterCell(matchingCell));
    assertFalse(mainFilter.filterAllRemaining());
  }

  @Test
  public void shouldReturnNextRowWhenKeyAfter() throws IOException {
    KeyValue afterCell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("pt.example.www.1"));

    assertTrue(mainFilter.filterRowKey(afterCell));
    assertEquals(Filter.ReturnCode.NEXT_ROW, mainFilter.filterCell(afterCell));
    assertTrue(mainFilter.filterAllRemaining());
  }

  @Test
  public void shouldReturnSeekNextUsingHintWhenKeyBeforeReversed() throws IOException {
    mainFilter.setReversed(true);

    KeyValue cell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("pt.example.www.1"));

    // Should include this row so that filterCell() will be invoked.
    assertFalse(mainFilter.filterRowKey(cell));
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, mainFilter.filterCell(cell));
    Cell actualCellHint = mainFilter.getNextCellHint(cell);
    assertNotNull(actualCellHint);
    Cell expectedCellHint = KeyValueUtil.createFirstOnRow(Bytes.toBytes(HOST_PREFIX));
    assertEquals(expectedCellHint, actualCellHint);
    assertFalse(mainFilter.filterAllRemaining());
  }

  @Test
  public void shouldReturnIncludeWhenKeyMatchesReversed() throws IOException {
    mainFilter.setReversed(true);

    KeyValue matchingCell = KeyValueUtil.createFirstOnRow(createRow('a'));

    assertFalse(mainFilter.filterRowKey(matchingCell));
    assertEquals(Filter.ReturnCode.INCLUDE, mainFilter.filterCell(matchingCell));
    assertFalse(mainFilter.filterAllRemaining());
  }

  @Test
  public void shouldReturnNextRowWhenKeyAfterReversed() throws IOException {
    mainFilter.setReversed(true);

    KeyValue cell = KeyValueUtil.createFirstOnRow(Bytes.toBytes("com.yahoo.www.a."));

    assertTrue(mainFilter.filterRowKey(cell));
    assertEquals(Filter.ReturnCode.NEXT_ROW, mainFilter.filterCell(cell));
    assertTrue(mainFilter.filterAllRemaining());
  }
}
