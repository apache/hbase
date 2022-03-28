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
import static org.hamcrest.MatcherAssert.assertThat;
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
public class TestClientMode extends TestModeBase {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestClientMode.class);

  @Override protected Mode getMode() {
    return Mode.CLIENT;
  }

  @Override protected void assertRecords(List<Record> records) {
    TestUtils.assertRecordsInClientMode(records);
  }

  @Override protected void assertDrillDown(Record currentRecord, DrillDownInfo drillDownInfo) {
    assertThat(drillDownInfo.getNextMode(), is(Mode.USER));
    assertThat(drillDownInfo.getInitialFilters().size(), is(1));
    String client = currentRecord.get(Field.CLIENT).asString();
    switch (client) {
      case "CLIENT_A_FOO":
        assertThat(drillDownInfo.getInitialFilters().get(0).toString(), is("CLIENT==CLIENT_A_FOO"));
        break;

      case "CLIENT_B_FOO":
        assertThat(drillDownInfo.getInitialFilters().get(0).toString(), is("CLIENT==CLIENT_B_FOO"));
        break;

      case "CLIENT_A_BAR":
        assertThat(drillDownInfo.getInitialFilters().get(0).toString(), is("CLIENT==CLIENT_A_BAR"));
        break;
      case "CLIENT_B_BAR":
        assertThat(drillDownInfo.getInitialFilters().get(0).toString(), is("CLIENT==CLIENT_B_BAR"));
        break;

      default:
        fail();
    }
  }
}
