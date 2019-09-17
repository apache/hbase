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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.hbtop.Record;
import org.apache.hadoop.hbase.hbtop.RecordFilter;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldInfo;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Implementation for {@link ModeStrategy} for Table Mode.
 */
@InterfaceAudience.Private
public final class TableModeStrategy implements ModeStrategy {

  private final List<FieldInfo> fieldInfos = Arrays.asList(
    new FieldInfo(Field.NAMESPACE, 0, true),
    new FieldInfo(Field.TABLE, 0, true),
    new FieldInfo(Field.REGION_COUNT, 7, true),
    new FieldInfo(Field.REQUEST_COUNT_PER_SECOND, 10, true),
    new FieldInfo(Field.READ_REQUEST_COUNT_PER_SECOND, 10, true),
    new FieldInfo(Field.FILTERED_READ_REQUEST_COUNT_PER_SECOND, 8, true),
    new FieldInfo(Field.WRITE_REQUEST_COUNT_PER_SECOND, 10, true),
    new FieldInfo(Field.STORE_FILE_SIZE, 13, true),
    new FieldInfo(Field.UNCOMPRESSED_STORE_FILE_SIZE, 15, false),
    new FieldInfo(Field.NUM_STORE_FILES, 7, true),
    new FieldInfo(Field.MEM_STORE_SIZE, 11, true)
  );

  private final RegionModeStrategy regionModeStrategy = new RegionModeStrategy();

  TableModeStrategy() {
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

    // Aggregation by NAMESPACE field and TABLE field
    return records.stream()
      .collect(Collectors.groupingBy(r -> {
        String namespace = r.get(Field.NAMESPACE).asString();
        String table = r.get(Field.TABLE).asString();
        return TableName.valueOf(namespace, table);
      }))
      .entrySet().stream()
      .flatMap(
        e -> e.getValue().stream()
          .reduce(Record::combine)
          .map(Stream::of)
          .orElse(Stream.empty()))
      .collect(Collectors.toList());
  }

  @Override
  public DrillDownInfo drillDown(Record selectedRecord) {
    List<RecordFilter> initialFilters = Arrays.asList(
      RecordFilter.newBuilder(Field.NAMESPACE).doubleEquals(selectedRecord.get(Field.NAMESPACE)),
      RecordFilter.newBuilder(Field.TABLE).doubleEquals(selectedRecord.get(Field.TABLE)));
    return new DrillDownInfo(Mode.REGION, initialFilters);
  }
}
