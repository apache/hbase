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

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.hbtop.Record;
import org.apache.hadoop.hbase.hbtop.RecordFilter;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Implementation for {@link ModeStrategy} for Region Mode.
 */
@InterfaceAudience.Private
public final class RegionModeStrategy implements ModeStrategy {

  private final List<FieldInfo> fieldInfos = Arrays.asList(
    new FieldInfo(Field.REGION_NAME, 0, false),
    new FieldInfo(Field.NAMESPACE, 0, true),
    new FieldInfo(Field.TABLE, 0,  true),
    new FieldInfo(Field.START_CODE, 13, false),
    new FieldInfo(Field.REPLICA_ID, 5, false),
    new FieldInfo(Field.REGION, 32, true),
    new FieldInfo(Field.REGION_SERVER, 0, true),
    new FieldInfo(Field.LONG_REGION_SERVER, 0, false),
    new FieldInfo(Field.REQUEST_COUNT_PER_SECOND, 8, true),
    new FieldInfo(Field.READ_REQUEST_COUNT_PER_SECOND, 8, true),
    new FieldInfo(Field.FILTERED_READ_REQUEST_COUNT_PER_SECOND, 8, true),
    new FieldInfo(Field.WRITE_REQUEST_COUNT_PER_SECOND, 8, true),
    new FieldInfo(Field.STORE_FILE_SIZE, 10, true),
    new FieldInfo(Field.UNCOMPRESSED_STORE_FILE_SIZE, 12, false),
    new FieldInfo(Field.NUM_STORE_FILES,4, true),
    new FieldInfo(Field.MEM_STORE_SIZE, 8, true),
    new FieldInfo(Field.LOCALITY, 8, true),
    new FieldInfo(Field.START_KEY, 0, false),
    new FieldInfo(Field.COMPACTING_CELL_COUNT, 12, false),
    new FieldInfo(Field.COMPACTED_CELL_COUNT, 12, false),
    new FieldInfo(Field.COMPACTION_PROGRESS, 7, false),
    new FieldInfo(Field.LAST_MAJOR_COMPACTION_TIME, 19, false)
  );

  private final Map<String, RequestCountPerSecond> requestCountPerSecondMap = new HashMap<>();

  RegionModeStrategy() {
  }

  @Override
  public List<FieldInfo> getFieldInfos() {
    return fieldInfos;
  }

  @Override
  public Field getDefaultSortField() {
    return Field.REQUEST_COUNT_PER_SECOND;
  }

  @Override public List<Record> getRecords(ClusterMetrics clusterMetrics,
      List<RecordFilter> pushDownFilters) {
    List<Record> ret = new ArrayList<>();
    for (ServerMetrics sm : clusterMetrics.getLiveServerMetrics().values()) {
      long lastReportTimestamp = sm.getLastReportTimestamp();
      for (RegionMetrics rm : sm.getRegionMetrics().values()) {
        ret.add(createRecord(sm, rm, lastReportTimestamp));
      }
    }
    return ret;
  }

  private Record createRecord(ServerMetrics serverMetrics, RegionMetrics regionMetrics,
    long lastReportTimestamp) {

    Record.Builder builder = Record.builder();

    String regionName = regionMetrics.getNameAsString();
    builder.put(Field.REGION_NAME, regionName);

    String namespaceName = "";
    String tableName = "";
    String region = "";
    String startKey = "";
    String startCode = "";
    String replicaId = "";
    try {
      byte[][] elements = RegionInfo.parseRegionName(regionMetrics.getRegionName());
      TableName tn = TableName.valueOf(elements[0]);
      namespaceName = tn.getNamespaceAsString();
      tableName = tn.getQualifierAsString();
      startKey = Bytes.toStringBinary(elements[1]);
      startCode = Bytes.toString(elements[2]);
      replicaId = elements.length == 4 ?
        Integer.valueOf(Bytes.toString(elements[3])).toString() : "";
      region = RegionInfo.encodeRegionName(regionMetrics.getRegionName());
    } catch (IOException ignored) {
    }

    builder.put(Field.NAMESPACE, namespaceName);
    builder.put(Field.TABLE, tableName);
    builder.put(Field.START_CODE, startCode);
    builder.put(Field.REPLICA_ID, replicaId);
    builder.put(Field.REGION, region);
    builder.put(Field.START_KEY, startKey);
    builder.put(Field.REGION_SERVER, serverMetrics.getServerName().toShortString());
    builder.put(Field.LONG_REGION_SERVER, serverMetrics.getServerName().getServerName());

    RequestCountPerSecond requestCountPerSecond = requestCountPerSecondMap.get(regionName);
    if (requestCountPerSecond == null) {
      requestCountPerSecond = new RequestCountPerSecond();
      requestCountPerSecondMap.put(regionName, requestCountPerSecond);
    }
    requestCountPerSecond.refresh(lastReportTimestamp, regionMetrics.getReadRequestCount(),
      regionMetrics.getFilteredReadRequestCount(), regionMetrics.getWriteRequestCount());

    builder.put(Field.READ_REQUEST_COUNT_PER_SECOND,
      requestCountPerSecond.getReadRequestCountPerSecond());
    builder.put(Field.FILTERED_READ_REQUEST_COUNT_PER_SECOND,
        requestCountPerSecond.getFilteredReadRequestCountPerSecond());
    builder.put(Field.WRITE_REQUEST_COUNT_PER_SECOND,
      requestCountPerSecond.getWriteRequestCountPerSecond());
    builder.put(Field.REQUEST_COUNT_PER_SECOND,
      requestCountPerSecond.getRequestCountPerSecond());

    builder.put(Field.STORE_FILE_SIZE, regionMetrics.getStoreFileSize());
    builder.put(Field.UNCOMPRESSED_STORE_FILE_SIZE, regionMetrics.getUncompressedStoreFileSize());
    builder.put(Field.NUM_STORE_FILES, regionMetrics.getStoreFileCount());
    builder.put(Field.MEM_STORE_SIZE, regionMetrics.getMemStoreSize());
    builder.put(Field.LOCALITY, regionMetrics.getDataLocality());

    long compactingCellCount = regionMetrics.getCompactingCellCount();
    long compactedCellCount = regionMetrics.getCompactedCellCount();
    float compactionProgress = 0;
    if  (compactedCellCount > 0) {
      compactionProgress = 100 * ((float) compactedCellCount / compactingCellCount);
    }

    builder.put(Field.COMPACTING_CELL_COUNT, compactingCellCount);
    builder.put(Field.COMPACTED_CELL_COUNT, compactedCellCount);
    builder.put(Field.COMPACTION_PROGRESS, compactionProgress);

    FastDateFormat df = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    long lastMajorCompactionTimestamp = regionMetrics.getLastMajorCompactionTimestamp();

    builder.put(Field.LAST_MAJOR_COMPACTION_TIME,
      lastMajorCompactionTimestamp == 0 ? "" : df.format(lastMajorCompactionTimestamp));

    return builder.build();
  }

  /**
   * Form new record list with records formed by only fields provided through fieldInfo and
   * add a count field for each record with value 1
   * We are doing two operation of selecting and adding new field
   * because of saving some CPU cycles on rebuilding the record again
   *
   * @param fieldInfos List of FieldInfos required in the record
   * @param records    List of records which needs to be processed
   * @param countField Field which needs to be added with value 1 for each record
   * @return records after selecting required fields and adding count field
   */
  List<Record> selectModeFieldsAndAddCountField(List<FieldInfo> fieldInfos, List<Record> records,
      Field countField) {

    return records.stream().map(record -> Record.ofEntries(
        fieldInfos.stream().filter(fi -> record.containsKey(fi.getField()))
            .map(fi -> Record.entry(fi.getField(), record.get(fi.getField())))))
        .map(record -> Record.builder().putAll(record).put(countField, 1).build())
        .collect(Collectors.toList());
  }

  @Nullable
  @Override
  public DrillDownInfo drillDown(Record selectedRecord) {
    // do nothing
    return null;
  }
}
