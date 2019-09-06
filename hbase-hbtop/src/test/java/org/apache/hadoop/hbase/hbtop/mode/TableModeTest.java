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
package org.apache.hadoop.hbase.hbtop.mode;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.hbtop.Record;
import org.apache.hadoop.hbase.hbtop.TestUtils;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;


@Category(SmallTests.class)
public class TableModeTest extends ModeTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TableModeTest.class);

  @Override
  protected Mode getMode() {
    return Mode.TABLE;
  }

  @Override
  protected void assertRecords(List<Record> records) {
    TestUtils.assertRecordsInTableMode(records);
  }

  @Override
  protected void assertDrillDown(Record currentRecord, DrillDownInfo drillDownInfo) {
    assertThat(drillDownInfo.getNextMode(), is(Mode.REGION));
    assertThat(drillDownInfo.getInitialFilters().size(), is(2));

    String tableName = String.format("%s:%s", currentRecord.get(Field.NAMESPACE).asString(),
      currentRecord.get(Field.TABLE).asString());

    switch (tableName) {
      case "default:table1":
        assertThat(drillDownInfo.getInitialFilters().get(0).toString(), is("NAMESPACE==default"));
        assertThat(drillDownInfo.getInitialFilters().get(1).toString(), is("TABLE==table1"));
        break;

      case "default:table2":
        assertThat(drillDownInfo.getInitialFilters().get(0).toString(), is("NAMESPACE==default"));
        assertThat(drillDownInfo.getInitialFilters().get(1).toString(), is("TABLE==table2"));
        break;

      case "namespace:table3":
        assertThat(drillDownInfo.getInitialFilters().get(0).toString(),
          is("NAMESPACE==namespace"));
        assertThat(drillDownInfo.getInitialFilters().get(1).toString(), is("TABLE==table3"));
        break;

      default:
        fail();
    }
  }
}
