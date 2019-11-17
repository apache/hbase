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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.hbtop.Record;
import org.apache.hadoop.hbase.hbtop.RecordFilter;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Implementation for {@link ModeStrategy} for User Mode.
 */
@InterfaceAudience.Private public final class UserModeStrategy implements ModeStrategy {

  private final List<FieldInfo> fieldInfos = Arrays
      .asList(new FieldInfo(Field.USER, 0, true),
          new FieldInfo(Field.CLIENT_COUNT, 7, true),
          new FieldInfo(Field.REQUEST_COUNT_PER_SECOND, 10, true),
          new FieldInfo(Field.READ_REQUEST_COUNT_PER_SECOND, 10, true),
          new FieldInfo(Field.WRITE_REQUEST_COUNT_PER_SECOND, 10, true),
          new FieldInfo(Field.FILTERED_READ_REQUEST_COUNT_PER_SECOND, 10, true));
  private final ClientModeStrategy clientModeStrategy = new ClientModeStrategy();

  UserModeStrategy() {
  }

  @Override public List<FieldInfo> getFieldInfos() {
    return fieldInfos;
  }

  @Override public Field getDefaultSortField() {
    return Field.REQUEST_COUNT_PER_SECOND;
  }

  @Override public List<Record> getRecords(ClusterMetrics clusterMetrics,
      List<RecordFilter> pushDownFilters) {
    List<Record> records = clientModeStrategy.createRecords(clusterMetrics);
    return clientModeStrategy.aggregateRecordsAndAddDistinct(
        ModeStrategyUtils.applyFilterAndGet(records, pushDownFilters), Field.USER, Field.CLIENT,
        Field.CLIENT_COUNT);
  }

  @Override public DrillDownInfo drillDown(Record selectedRecord) {
    //Drill down to client and using selected USER as a filter
    List<RecordFilter> initialFilters = Collections.singletonList(
        RecordFilter.newBuilder(Field.USER).doubleEquals(selectedRecord.get(Field.USER)));
    return new DrillDownInfo(Mode.CLIENT, initialFilters);
  }
}
