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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.hbtop.Record;
import org.apache.hadoop.hbase.hbtop.RecordFilter;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldInfo;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Implementation for {@link ModeStrategy} for RegionServer Mode.
 */
@InterfaceAudience.Private
public final class RegionServerModeStrategy implements ModeStrategy {

  private final List<FieldInfo> fieldInfos = Arrays.asList(
    new FieldInfo(Field.REGION_SERVER, 0, true),
    new FieldInfo(Field.LONG_REGION_SERVER, 0, false),
    new FieldInfo(Field.REGION_COUNT, 7, true),
    new FieldInfo(Field.REQUEST_COUNT_PER_SECOND, 10, true),
    new FieldInfo(Field.READ_REQUEST_COUNT_PER_SECOND, 10, true),
    new FieldInfo(Field.FILTERED_READ_REQUEST_COUNT_PER_SECOND, 8, true),
    new FieldInfo(Field.WRITE_REQUEST_COUNT_PER_SECOND, 10, true),
    new FieldInfo(Field.STORE_FILE_SIZE, 13, true),
    new FieldInfo(Field.UNCOMPRESSED_STORE_FILE_SIZE, 15, false),
    new FieldInfo(Field.NUM_STORE_FILES, 7, true),
    new FieldInfo(Field.MEM_STORE_SIZE, 11, true),
    new FieldInfo(Field.USED_HEAP_SIZE, 11, true),
    new FieldInfo(Field.MAX_HEAP_SIZE, 11, true)
  );

  private final RegionModeStrategy regionModeStrategy = new RegionModeStrategy();

  RegionServerModeStrategy(){
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
  public List<Record> getRecords(ClusterMetrics clusterMetrics) {
    // Get records from RegionModeStrategy and add REGION_COUNT field
    List<Record> records = regionModeStrategy.getRecords(clusterMetrics).stream()
      .map(record ->
        Record.ofEntries(fieldInfos.stream()
          .filter(fi -> record.containsKey(fi.getField()))
          .map(fi -> Record.entry(fi.getField(), record.get(fi.getField())))))
      .map(record -> Record.builder().putAll(record).put(Field.REGION_COUNT, 1).build())
      .collect(Collectors.toList());

    // Aggregation by LONG_REGION_SERVER field
    Map<String, Record> retMap = records.stream()
      .collect(Collectors.groupingBy(r -> r.get(Field.LONG_REGION_SERVER).asString()))
      .entrySet().stream()
      .flatMap(
        e -> e.getValue().stream()
          .reduce(Record::combine)
          .map(Stream::of)
          .orElse(Stream.empty()))
      .collect(Collectors.toMap(r -> r.get(Field.LONG_REGION_SERVER).asString(), r -> r));

    // Add USED_HEAP_SIZE field and MAX_HEAP_SIZE field
    for (ServerMetrics sm : clusterMetrics.getLiveServerMetrics().values()) {
      Record record = retMap.get(sm.getServerName().getServerName());
      if (record == null) {
        continue;
      }

      Record newRecord = Record.builder().putAll(record)
        .put(Field.USED_HEAP_SIZE, sm.getUsedHeapSize())
        .put(Field.MAX_HEAP_SIZE, sm.getMaxHeapSize()).build();

      retMap.put(sm.getServerName().getServerName(), newRecord);
    }

    return new ArrayList<>(retMap.values());
  }

  @Override
  public DrillDownInfo drillDown(Record selectedRecord) {
    List<RecordFilter> initialFilters = Collections.singletonList(RecordFilter
      .newBuilder(Field.REGION_SERVER)
      .doubleEquals(selectedRecord.get(Field.REGION_SERVER)));
    return new DrillDownInfo(Mode.REGION, initialFilters);
  }
}
