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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.hbtop.Record;
import org.apache.hadoop.hbase.hbtop.RecordFilter;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldInfo;

/**
 * Implementation for {@link ModeStrategy} for Namespace Mode.
 */
@InterfaceAudience.Private
public final class NamespaceModeStrategy implements ModeStrategy {

  private final List<FieldInfo> fieldInfos = Arrays.asList(
    new FieldInfo(Field.NAMESPACE, 0, true),
    new FieldInfo(Field.REGION_COUNT, 7, true),
    new FieldInfo(Field.REQUEST_COUNT_PER_SECOND, 10, true),
    new FieldInfo(Field.READ_REQUEST_COUNT_PER_SECOND, 10, true),
    new FieldInfo(Field.WRITE_REQUEST_COUNT_PER_SECOND, 10, true),
    new FieldInfo(Field.STORE_FILE_SIZE, 13, true),
    new FieldInfo(Field.UNCOMPRESSED_STORE_FILE_SIZE, 15, false),
    new FieldInfo(Field.NUM_STORE_FILES, 7, true),
    new FieldInfo(Field.MEM_STORE_SIZE, 11, true)
  );

  private final RegionModeStrategy regionModeStrategy = new RegionModeStrategy();

  NamespaceModeStrategy(){
  }

  @Override
  public List<FieldInfo> getFieldInfos() {
    return fieldInfos;
  }

  @Override
  public Field getDefaultSortField() {
    return Field.REQUEST_COUNT_PER_SECOND;
  }

  @Override
  public List<Record> getRecords(ClusterStatus clusterStatus) {
    // Get records from RegionModeStrategy and add REGION_COUNT field
    List<Record> records = new ArrayList<>();
    for (Record record : regionModeStrategy.getRecords(clusterStatus)) {
      List<Record.Entry> entries = new ArrayList<>();
      for (FieldInfo fieldInfo : fieldInfos) {
        if (record.containsKey(fieldInfo.getField())) {
          entries.add(Record.entry(fieldInfo.getField(),
            record.get(fieldInfo.getField())));
        }
      }

      // Add REGION_COUNT field
      records.add(Record.builder().putAll(Record.ofEntries(entries))
        .put(Field.REGION_COUNT, 1).build());
    }

    // Aggregation by NAMESPACE field
    Map<String, Record> retMap = new HashMap<>();
    for (Record record : records) {
      String namespace = record.get(Field.NAMESPACE).asString();
      if (retMap.containsKey(namespace)) {
        retMap.put(namespace, retMap.get(namespace).combine(record));
      } else {
        retMap.put(namespace, record);
      }
    }
    return new ArrayList<>(retMap.values());
  }

  @Override
  public DrillDownInfo drillDown(Record selectedRecord) {
    List<RecordFilter> initialFilters =
      Collections.singletonList(RecordFilter.newBuilder(Field.NAMESPACE)
        .doubleEquals(selectedRecord.get(Field.NAMESPACE)));
    return new DrillDownInfo(Mode.TABLE, initialFilters);
  }
}
