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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.UserMetrics;
import org.apache.hadoop.hbase.hbtop.Record;
import org.apache.hadoop.hbase.hbtop.RecordFilter;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldInfo;
import org.apache.hadoop.hbase.hbtop.field.FieldValue;
import org.apache.hadoop.hbase.hbtop.field.FieldValueType;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Implementation for {@link ModeStrategy} for client Mode.
 */
@InterfaceAudience.Private public final class ClientModeStrategy implements ModeStrategy {

  private final List<FieldInfo> fieldInfos = Arrays
      .asList(new FieldInfo(Field.CLIENT, 0, true),
          new FieldInfo(Field.USER_COUNT, 5, true),
          new FieldInfo(Field.REQUEST_COUNT_PER_SECOND, 10, true),
          new FieldInfo(Field.READ_REQUEST_COUNT_PER_SECOND, 10, true),
          new FieldInfo(Field.WRITE_REQUEST_COUNT_PER_SECOND, 10, true),
          new FieldInfo(Field.FILTERED_READ_REQUEST_COUNT_PER_SECOND, 10, true));
  private final Map<String, RequestCountPerSecond> requestCountPerSecondMap = new HashMap<>();

  ClientModeStrategy() {
  }

  @Override public List<FieldInfo> getFieldInfos() {
    return fieldInfos;
  }

  @Override public Field getDefaultSortField() {
    return Field.REQUEST_COUNT_PER_SECOND;
  }

  @Override public List<Record> getRecords(ClusterMetrics clusterMetrics,
      List<RecordFilter> pushDownFilters) {
    List<Record> records = createRecords(clusterMetrics);
    return aggregateRecordsAndAddDistinct(
        ModeStrategyUtils.applyFilterAndGet(records, pushDownFilters), Field.CLIENT, Field.USER,
        Field.USER_COUNT);
  }

  List<Record> createRecords(ClusterMetrics clusterMetrics) {
    List<Record> ret = new ArrayList<>();
    for (ServerMetrics serverMetrics : clusterMetrics.getLiveServerMetrics().values()) {
      long lastReportTimestamp = serverMetrics.getLastReportTimestamp();
      serverMetrics.getUserMetrics().values().forEach(um -> um.getClientMetrics().values().forEach(
        clientMetrics -> ret.add(
              createRecord(um.getNameAsString(), clientMetrics, lastReportTimestamp,
                  serverMetrics.getServerName().getServerName()))));
    }
    return ret;
  }

  /**
   * Aggregate the records and count the unique values for the given distinctField
   *
   * @param records               records to be processed
   * @param groupBy               Field on which group by needs to be done
   * @param distinctField         Field whose unique values needs to be counted
   * @param uniqueCountAssignedTo a target field to which the unique count is assigned to
   * @return aggregated records
   */
  List<Record> aggregateRecordsAndAddDistinct(List<Record> records, Field groupBy,
      Field distinctField, Field uniqueCountAssignedTo) {
    List<Record> result = new ArrayList<>();
    records.stream().collect(Collectors.groupingBy(r -> r.get(groupBy))).values()
        .forEach(val -> {
          Set<FieldValue> distinctValues = new HashSet<>();
          Map<Field, FieldValue> map = new HashMap<>();
          for (Record record : val) {
            for (Map.Entry<Field, FieldValue> field : record.entrySet()) {
              if (distinctField.equals(field.getKey())) {
                //We will not be adding the field in the new record whose distinct count is required
                distinctValues.add(record.get(distinctField));
              } else {
                if (field.getKey().getFieldValueType() == FieldValueType.STRING) {
                  map.put(field.getKey(), field.getValue());
                } else {
                  if (map.get(field.getKey()) == null) {
                    map.put(field.getKey(), field.getValue());
                  } else {
                    map.put(field.getKey(), map.get(field.getKey()).plus(field.getValue()));
                  }
                }
              }
            }
          }
          // Add unique count field
          map.put(uniqueCountAssignedTo, uniqueCountAssignedTo.newValue(distinctValues.size()));
          result.add(Record.ofEntries(map.entrySet().stream()
            .map(k -> Record.entry(k.getKey(), k.getValue()))));
        });
    return result;
  }

  Record createRecord(String user, UserMetrics.ClientMetrics clientMetrics,
      long lastReportTimestamp, String server) {
    Record.Builder builder = Record.builder();
    String client = clientMetrics.getHostName();
    builder.put(Field.CLIENT, clientMetrics.getHostName());
    String mapKey = client + "$" + user + "$" + server;
    RequestCountPerSecond requestCountPerSecond = requestCountPerSecondMap.get(mapKey);
    if (requestCountPerSecond == null) {
      requestCountPerSecond = new RequestCountPerSecond();
      requestCountPerSecondMap.put(mapKey, requestCountPerSecond);
    }
    requestCountPerSecond.refresh(lastReportTimestamp, clientMetrics.getReadRequestsCount(),
        clientMetrics.getFilteredReadRequestsCount(), clientMetrics.getWriteRequestsCount());
    builder.put(Field.REQUEST_COUNT_PER_SECOND, requestCountPerSecond.getRequestCountPerSecond());
    builder.put(Field.READ_REQUEST_COUNT_PER_SECOND,
        requestCountPerSecond.getReadRequestCountPerSecond());
    builder.put(Field.WRITE_REQUEST_COUNT_PER_SECOND,
        requestCountPerSecond.getWriteRequestCountPerSecond());
    builder.put(Field.FILTERED_READ_REQUEST_COUNT_PER_SECOND,
        requestCountPerSecond.getFilteredReadRequestCountPerSecond());
    builder.put(Field.USER, user);
    return builder.build();
  }

  @Override public DrillDownInfo drillDown(Record selectedRecord) {
    List<RecordFilter> initialFilters = Collections.singletonList(
        RecordFilter.newBuilder(Field.CLIENT).doubleEquals(selectedRecord.get(Field.CLIENT)));
    return new DrillDownInfo(Mode.USER, initialFilters);
  }
}
