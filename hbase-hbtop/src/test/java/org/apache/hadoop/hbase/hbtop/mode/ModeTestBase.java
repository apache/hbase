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

import java.util.List;
import org.apache.hadoop.hbase.hbtop.Record;
import org.apache.hadoop.hbase.hbtop.TestUtils;
import org.junit.Test;


public abstract class ModeTestBase {

  @Test
  public void testGetRecords() {
    List<Record> records = getMode().getRecords(TestUtils.createDummyClusterMetrics());
    assertRecords(records);
  }

  protected abstract Mode getMode();
  protected abstract void assertRecords(List<Record> records);

  @Test
  public void testDrillDown() {
    List<Record> records = getMode().getRecords(TestUtils.createDummyClusterMetrics());
    for (Record record : records) {
      assertDrillDown(record, getMode().drillDown(record));
    }
  }

  protected abstract void assertDrillDown(Record currentRecord, DrillDownInfo drillDownInfo);
}
