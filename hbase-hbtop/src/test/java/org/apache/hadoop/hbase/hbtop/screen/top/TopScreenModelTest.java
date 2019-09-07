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
package org.apache.hadoop.hbase.hbtop.screen.top;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.hbtop.Record;
import org.apache.hadoop.hbase.hbtop.RecordFilter;
import org.apache.hadoop.hbase.hbtop.TestUtils;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldInfo;
import org.apache.hadoop.hbase.hbtop.field.FieldValue;
import org.apache.hadoop.hbase.hbtop.mode.Mode;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


@Category(SmallTests.class)
@RunWith(MockitoJUnitRunner.class)
public class TopScreenModelTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TopScreenModelTest.class);

  @Mock
  private Admin admin;

  private TopScreenModel topScreenModel;

  private List<Field> fields;

  @Before
  public void setup() throws IOException {
    when(admin.getClusterMetrics()).thenReturn(TestUtils.createDummyClusterMetrics());
    topScreenModel = new TopScreenModel(admin, Mode.REGION);

    fields = Mode.REGION.getFieldInfos().stream()
      .map(FieldInfo::getField)
      .collect(Collectors.toList());
  }

  @Test
  public void testSummary() {
    topScreenModel.refreshMetricsData();
    Summary summary = topScreenModel.getSummary();
    TestUtils.assertSummary(summary);
  }

  @Test
  public void testRecords() {
    // Region Mode
    topScreenModel.refreshMetricsData();
    TestUtils.assertRecordsInRegionMode(topScreenModel.getRecords());

    // Namespace Mode
    topScreenModel.switchMode(Mode.NAMESPACE, null, false);
    topScreenModel.refreshMetricsData();
    TestUtils.assertRecordsInNamespaceMode(topScreenModel.getRecords());

    // Table Mode
    topScreenModel.switchMode(Mode.TABLE, null, false);
    topScreenModel.refreshMetricsData();
    TestUtils.assertRecordsInTableMode(topScreenModel.getRecords());

    // Namespace Mode
    topScreenModel.switchMode(Mode.REGION_SERVER, null, false);
    topScreenModel.refreshMetricsData();
    TestUtils.assertRecordsInRegionServerMode(topScreenModel.getRecords());
  }

  @Test
  public void testSort() {
    // The sort key is LOCALITY
    topScreenModel.setSortFieldAndFields(Field.LOCALITY, fields);

    FieldValue previous = null;

    // Test for ascending sort
    topScreenModel.refreshMetricsData();

    for (Record record : topScreenModel.getRecords()) {
      FieldValue current = record.get(Field.LOCALITY);
      if (previous != null) {
        assertTrue(current.compareTo(previous) < 0);
      }
      previous = current;
    }

    // Test for descending sort
    topScreenModel.switchSortOrder();
    topScreenModel.refreshMetricsData();

    previous = null;
    for (Record record : topScreenModel.getRecords()) {
      FieldValue current = record.get(Field.LOCALITY);
      if (previous != null) {
        assertTrue(current.compareTo(previous) > 0);
      }
      previous = current;
    }
  }

  @Test
  public void testFilters() {
    topScreenModel.addFilter("TABLE==table1", false);
    topScreenModel.refreshMetricsData();
    for (Record record : topScreenModel.getRecords()) {
      FieldValue value = record.get(Field.TABLE);
      assertThat(value.asString(), is("table1"));
    }

    topScreenModel.clearFilters();
    topScreenModel.addFilter("TABLE==TABLE1", false);
    topScreenModel.refreshMetricsData();
    assertThat(topScreenModel.getRecords().size(), is(0));

    // Test for ignore case
    topScreenModel.clearFilters();
    topScreenModel.addFilter("TABLE==TABLE1", true);
    topScreenModel.refreshMetricsData();
    for (Record record : topScreenModel.getRecords()) {
      FieldValue value = record.get(Field.TABLE);
      assertThat(value.asString(), is("table1"));
    }
  }

  @Test
  public void testFilterHistories() {
    topScreenModel.addFilter("TABLE==table1", false);
    topScreenModel.addFilter("TABLE==table2", false);
    topScreenModel.addFilter("TABLE==table3", false);

    assertThat(topScreenModel.getFilterHistories().get(0), is("TABLE==table1"));
    assertThat(topScreenModel.getFilterHistories().get(1), is("TABLE==table2"));
    assertThat(topScreenModel.getFilterHistories().get(2), is("TABLE==table3"));
  }

  @Test
  public void testSwitchMode() {
    topScreenModel.switchMode(Mode.TABLE, null, false);
    assertThat(topScreenModel.getCurrentMode(), is(Mode.TABLE));

    // Test for initialFilters
    List<RecordFilter> initialFilters = Arrays.asList(
      RecordFilter.parse("TABLE==table1", fields, true),
      RecordFilter.parse("TABLE==table2", fields, true));

    topScreenModel.switchMode(Mode.TABLE, initialFilters, false);

    assertThat(topScreenModel.getFilters().size(), is(initialFilters.size()));
    for (int i = 0; i < topScreenModel.getFilters().size(); i++) {
      assertThat(topScreenModel.getFilters().get(i).toString(),
        is(initialFilters.get(i).toString()));
    }

    // Test when keepSortFieldAndSortOrderIfPossible is true
    topScreenModel.setSortFieldAndFields(Field.NAMESPACE, fields);
    topScreenModel.switchMode(Mode.NAMESPACE, null, true);
    assertThat(topScreenModel.getCurrentSortField(), is(Field.NAMESPACE));
  }

  @Test
  public void testDrillDown() {
    topScreenModel.switchMode(Mode.TABLE, null, false);
    topScreenModel.setSortFieldAndFields(Field.NAMESPACE, fields);
    topScreenModel.refreshMetricsData();

    boolean success = topScreenModel.drillDown(topScreenModel.getRecords().get(0));
    assertThat(success, is(true));

    assertThat(topScreenModel.getFilters().get(0).toString(), is("NAMESPACE==namespace"));
    assertThat(topScreenModel.getFilters().get(1).toString(), is("TABLE==table3"));
    assertThat(topScreenModel.getCurrentSortField(), is(Field.NAMESPACE));
  }
}
