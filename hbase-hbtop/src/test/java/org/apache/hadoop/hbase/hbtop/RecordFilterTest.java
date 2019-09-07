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
package org.apache.hadoop.hbase.hbtop;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(SmallTests.class)
public class RecordFilterTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(RecordFilterTest.class);

  @Test
  public void testParseAndBuilder() {
    testParseAndBuilder("REGION=region1", false,
      RecordFilter.newBuilder(Field.REGION).equal("region1"));

    testParseAndBuilder("REGION=", false,
      RecordFilter.newBuilder(Field.REGION).equal(""));

    testParseAndBuilder("!REGION=region1", false,
      RecordFilter.newBuilder(Field.REGION).notEqual("region1"));

    testParseAndBuilder("REGION==region2", true,
      RecordFilter.newBuilder(Field.REGION, true).doubleEquals("region2"));

    testParseAndBuilder("!REGION==region2", true,
      RecordFilter.newBuilder(Field.REGION, true).notDoubleEquals("region2"));

    testParseAndBuilder("#REQ/S>100", false,
      RecordFilter.newBuilder(Field.REQUEST_COUNT_PER_SECOND).greater(100L));

    testParseAndBuilder("!#REQ/S>100", false,
      RecordFilter.newBuilder(Field.REQUEST_COUNT_PER_SECOND).notGreater(100L));

    testParseAndBuilder("SF>=50MB", true,
      RecordFilter.newBuilder(Field.STORE_FILE_SIZE, true).greaterOrEqual("50MB"));

    testParseAndBuilder("!SF>=50MB", true,
      RecordFilter.newBuilder(Field.STORE_FILE_SIZE, true).notGreaterOrEqual("50MB"));

    testParseAndBuilder("#REQ/S<20", false,
      RecordFilter.newBuilder(Field.REQUEST_COUNT_PER_SECOND).less(20L));

    testParseAndBuilder("!#REQ/S<20", false,
      RecordFilter.newBuilder(Field.REQUEST_COUNT_PER_SECOND).notLess(20L));

    testParseAndBuilder("%COMP<=50%", true,
      RecordFilter.newBuilder(Field.COMPACTION_PROGRESS, true).lessOrEqual("50%"));

    testParseAndBuilder("!%COMP<=50%", true,
      RecordFilter.newBuilder(Field.COMPACTION_PROGRESS, true).notLessOrEqual("50%"));
  }

  private void testParseAndBuilder(String filterString, boolean ignoreCase, RecordFilter expected) {
    RecordFilter actual = RecordFilter.parse(filterString, ignoreCase);
    assertThat(expected, is(actual));
  }

  @Test
  public void testParseFailure() {
    RecordFilter filter = RecordFilter.parse("REGIO=region1", false);
    assertThat(filter, is(nullValue()));

    filter = RecordFilter.parse("", false);
    assertThat(filter, is(nullValue()));

    filter = RecordFilter.parse("#REQ/S==aaa", false);
    assertThat(filter, is(nullValue()));

    filter = RecordFilter.parse("SF>=50", false);
    assertThat(filter, is(nullValue()));
  }

  @Test
  public void testToString() {
    testToString("REGION=region1");
    testToString("!REGION=region1");
    testToString("REGION==region2");
    testToString("!REGION==region2");
    testToString("#REQ/S>100");
    testToString("!#REQ/S>100");
    testToString("SF>=50.0MB");
    testToString("!SF>=50.0MB");
    testToString("#REQ/S<20");
    testToString("!#REQ/S<20");
    testToString("%COMP<=50.00%");
    testToString("!%COMP<=50.00%");
  }

  private void testToString(String filterString) {
    RecordFilter filter = RecordFilter.parse(filterString, false);
    assertThat(filter, is(notNullValue()));
    assertThat(filterString, is(filter.toString()));
  }

  @Test
  public void testFilters() {
    List<Record> records = createTestRecords();

    testFilter(records, "REGION=region", false,
      "region1", "region2", "region3", "region4", "region5");
    testFilter(records, "!REGION=region", false);
    testFilter(records, "REGION=Region", false);

    testFilter(records, "REGION==region", false);
    testFilter(records, "REGION==region1", false, "region1");
    testFilter(records, "!REGION==region1", false, "region2", "region3", "region4", "region5");

    testFilter(records, "#REQ/S==100", false, "region1");
    testFilter(records, "#REQ/S>100", false, "region2", "region5");
    testFilter(records, "SF>=100MB", false, "region1", "region2", "region4", "region5");
    testFilter(records, "!#SF>=10", false, "region1", "region4");
    testFilter(records, "LOCALITY<0.5", false, "region5");
    testFilter(records, "%COMP<=50%", false, "region2", "region3", "region4", "region5");

    testFilters(records, Arrays.asList("SF>=100MB", "#REQ/S>100"), false,
      "region2", "region5");
    testFilters(records, Arrays.asList("%COMP<=50%", "!#SF>=10"), false, "region4");
    testFilters(records, Arrays.asList("!REGION==region1", "LOCALITY<0.5", "#REQ/S>100"), false,
      "region5");
  }

  @Test
  public void testFiltersIgnoreCase() {
    List<Record> records = createTestRecords();

    testFilter(records, "REGION=Region", true,
      "region1", "region2", "region3", "region4", "region5");
    testFilter(records, "REGION=REGION", true,
      "region1", "region2", "region3", "region4", "region5");
  }

  private List<Record> createTestRecords() {
    List<Record> ret = new ArrayList<>();
    ret.add(createTestRecord("region1", 100L, new Size(100, Size.Unit.MEGABYTE), 2, 1.0f, 80f));
    ret.add(createTestRecord("region2", 120L, new Size(100, Size.Unit.GIGABYTE), 10, 0.5f, 20f));
    ret.add(createTestRecord("region3", 50L, new Size(500, Size.Unit.KILOBYTE), 15, 0.8f, 50f));
    ret.add(createTestRecord("region4", 90L, new Size(10, Size.Unit.TERABYTE), 5, 0.9f, 30f));
    ret.add(createTestRecord("region5", 200L, new Size(1, Size.Unit.PETABYTE), 13, 0.1f, 40f));
    return ret;
  }

  private Record createTestRecord(String region, long requestCountPerSecond,
    Size storeFileSize, int numStoreFiles, float locality, float compactionProgress) {
    Record.Builder builder = Record.builder();
    builder.put(Field.REGION, region);
    builder.put(Field.REQUEST_COUNT_PER_SECOND, requestCountPerSecond);
    builder.put(Field.STORE_FILE_SIZE, storeFileSize);
    builder.put(Field.NUM_STORE_FILES, numStoreFiles);
    builder.put(Field.LOCALITY, locality);
    builder.put(Field.COMPACTION_PROGRESS, compactionProgress);
    return builder.build();
  }

  private void testFilter(List<Record> records, String filterString, boolean ignoreCase,
    String... expectedRegions) {
    testFilters(records, Collections.singletonList(filterString), ignoreCase, expectedRegions);
  }

  private void testFilters(List<Record> records, List<String> filterStrings, boolean ignoreCase,
    String... expectedRegions) {
    List<String> actual =
      records.stream().filter(r -> filterStrings.stream()
        .map(f -> RecordFilter.parse(f, ignoreCase))
        .allMatch(f -> f.execute(r)))
        .map(r -> r.get(Field.REGION).asString())
        .collect(Collectors.toList());
    assertThat(actual, hasItems(expectedRegions));
    assertThat(actual.size(), is(expectedRegions.length));
  }
}
