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
package org.apache.hadoop.hbase.hbtop;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.RegionMetricsBuilder;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.screen.top.Summary;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Bytes;


public final class TestUtils {

  private TestUtils() {
  }

  public static ClusterMetrics createDummyClusterMetrics() {
    Map<ServerName, ServerMetrics> serverMetricsMap = new HashMap<>();

    // host1
    List<RegionMetrics> regionMetricsList = new ArrayList<>();
    regionMetricsList.add(createRegionMetrics(
      "table1,,1.00000000000000000000000000000000.",
      100, 50, 100,
      new Size(100, Size.Unit.MEGABYTE), new Size(200, Size.Unit.MEGABYTE), 1,
      new Size(100, Size.Unit.MEGABYTE), 0.1f, 100, 100, "2019-07-22 00:00:00"));
    regionMetricsList.add(createRegionMetrics(
      "table2,1,2.00000000000000000000000000000001.",
      200, 100, 200,
      new Size(200, Size.Unit.MEGABYTE), new Size(400, Size.Unit.MEGABYTE), 2,
      new Size(200, Size.Unit.MEGABYTE), 0.2f, 50, 200, "2019-07-22 00:00:01"));
    regionMetricsList.add(createRegionMetrics(
      "namespace:table3,,3_0001.00000000000000000000000000000002.",
      300, 150, 300,
      new Size(300, Size.Unit.MEGABYTE), new Size(600, Size.Unit.MEGABYTE), 3,
      new Size(300, Size.Unit.MEGABYTE), 0.3f, 100, 300, "2019-07-22 00:00:02"));

    ServerName host1 = ServerName.valueOf("host1.apache.com", 1000, 1);
    serverMetricsMap.put(host1, createServerMetrics(host1, 100,
      new Size(100, Size.Unit.MEGABYTE), new Size(200, Size.Unit.MEGABYTE), 100,
      regionMetricsList));

    // host2
    regionMetricsList.clear();
    regionMetricsList.add(createRegionMetrics(
      "table1,1,4.00000000000000000000000000000003.",
      100, 50, 100,
      new Size(100, Size.Unit.MEGABYTE), new Size(200, Size.Unit.MEGABYTE), 1,
      new Size(100, Size.Unit.MEGABYTE), 0.4f, 50, 100, "2019-07-22 00:00:03"));
    regionMetricsList.add(createRegionMetrics(
      "table2,,5.00000000000000000000000000000004.",
      200, 100, 200,
      new Size(200, Size.Unit.MEGABYTE), new Size(400, Size.Unit.MEGABYTE), 2,
      new Size(200, Size.Unit.MEGABYTE), 0.5f, 150, 200, "2019-07-22 00:00:04"));
    regionMetricsList.add(createRegionMetrics(
      "namespace:table3,,6.00000000000000000000000000000005.",
      300, 150, 300,
      new Size(300, Size.Unit.MEGABYTE), new Size(600, Size.Unit.MEGABYTE), 3,
      new Size(300, Size.Unit.MEGABYTE), 0.6f, 200, 300, "2019-07-22 00:00:05"));

    ServerName host2 = ServerName.valueOf("host2.apache.com", 1001, 2);
    serverMetricsMap.put(host2, createServerMetrics(host2, 200,
      new Size(16, Size.Unit.GIGABYTE), new Size(32, Size.Unit.GIGABYTE), 200,
      regionMetricsList));

    ServerName host3 = ServerName.valueOf("host3.apache.com", 1002, 3);
    return ClusterMetricsBuilder.newBuilder()
      .setHBaseVersion("3.0.0-SNAPSHOT")
      .setClusterId("01234567-89ab-cdef-0123-456789abcdef")
      .setLiveServerMetrics(serverMetricsMap)
      .setDeadServerNames(Collections.singletonList(host3))
      .setRegionsInTransition(Collections.singletonList(
        new RegionState(RegionInfoBuilder.newBuilder(TableName.valueOf("table4"))
          .setStartKey(new byte [0])
          .setEndKey(new byte [0])
          .setOffline(true)
          .setReplicaId(0)
          .setRegionId(0)
          .setSplit(false)
          .build(),
          RegionState.State.OFFLINE, host3)))
      .build();
  }

  private static RegionMetrics createRegionMetrics(String regionName, long readRequestCount,
    long filteredReadRequestCount, long writeRequestCount, Size storeFileSize,
    Size uncompressedStoreFileSize, int storeFileCount, Size memStoreSize, float locality,
    long compactedCellCount, long compactingCellCount, String lastMajorCompactionTime) {

    FastDateFormat df = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    try {
      return RegionMetricsBuilder.newBuilder(Bytes.toBytes(regionName))
        .setReadRequestCount(readRequestCount)
        .setFilteredReadRequestCount(filteredReadRequestCount)
        .setWriteRequestCount(writeRequestCount).setStoreFileSize(storeFileSize)
        .setUncompressedStoreFileSize(uncompressedStoreFileSize).setStoreFileCount(storeFileCount)
        .setMemStoreSize(memStoreSize).setDataLocality(locality)
        .setCompactedCellCount(compactedCellCount).setCompactingCellCount(compactingCellCount)
        .setLastMajorCompactionTimestamp(df.parse(lastMajorCompactionTime).getTime()).build();
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static ServerMetrics createServerMetrics(ServerName serverName, long reportTimestamp,
    Size usedHeapSize, Size maxHeapSize, long requestCountPerSecond,
    List<RegionMetrics> regionMetricsList) {

    return ServerMetricsBuilder.newBuilder(serverName)
      .setReportTimestamp(reportTimestamp)
      .setUsedHeapSize(usedHeapSize)
      .setMaxHeapSize(maxHeapSize)
      .setRequestCountPerSecond(requestCountPerSecond)
      .setRegionMetrics(regionMetricsList).build();
  }

  public static void assertRecordsInRegionMode(List<Record> records) {
    assertThat(records.size(), is(6));

    for (Record record : records) {
      switch (record.get(Field.REGION_NAME).asString()) {
        case "table1,,1.00000000000000000000000000000000.":
          assertRecordInRegionMode(record, "default", "1", "", "table1",
            "00000000000000000000000000000000", "host1:1000", "host1.apache.com,1000,1",0L,
            0L, 0L, 0L, new Size(100, Size.Unit.MEGABYTE), new Size(200, Size.Unit.MEGABYTE), 1,
            new Size(100, Size.Unit.MEGABYTE), 0.1f, "", 100L, 100L, 100f,
            "2019-07-22 00:00:00");
          break;

        case "table1,1,4.00000000000000000000000000000003.":
          assertRecordInRegionMode(record, "default", "4", "", "table1",
            "00000000000000000000000000000003", "host2:1001", "host2.apache.com,1001,2",0L,
            0L, 0L, 0L, new Size(100, Size.Unit.MEGABYTE), new Size(200, Size.Unit.MEGABYTE), 1,
            new Size(100, Size.Unit.MEGABYTE), 0.4f, "1", 100L, 50L, 50f,
            "2019-07-22 00:00:03");
          break;

        case "table2,,5.00000000000000000000000000000004.":
          assertRecordInRegionMode(record, "default", "5", "", "table2",
            "00000000000000000000000000000004", "host2:1001", "host2.apache.com,1001,2",0L,
            0L, 0L, 0L, new Size(200, Size.Unit.MEGABYTE), new Size(400, Size.Unit.MEGABYTE), 2,
            new Size(200, Size.Unit.MEGABYTE), 0.5f, "", 200L, 150L, 75f,
            "2019-07-22 00:00:04");
          break;

        case "table2,1,2.00000000000000000000000000000001.":
          assertRecordInRegionMode(record, "default", "2", "", "table2",
            "00000000000000000000000000000001", "host1:1000", "host1.apache.com,1000,1",0L,
            0L, 0L, 0L, new Size(200, Size.Unit.MEGABYTE), new Size(400, Size.Unit.MEGABYTE), 2,
            new Size(200, Size.Unit.MEGABYTE), 0.2f, "1", 200L, 50L, 25f,
            "2019-07-22 00:00:01");
          break;

        case "namespace:table3,,6.00000000000000000000000000000005.":
          assertRecordInRegionMode(record, "namespace", "6", "", "table3",
            "00000000000000000000000000000005", "host2:1001", "host2.apache.com,1001,2",0L,
            0L, 0L, 0L, new Size(300, Size.Unit.MEGABYTE), new Size(600, Size.Unit.MEGABYTE), 3,
            new Size(300, Size.Unit.MEGABYTE), 0.6f, "", 300L, 200L, 66.66667f,
            "2019-07-22 00:00:05");
          break;

        case "namespace:table3,,3_0001.00000000000000000000000000000002.":
          assertRecordInRegionMode(record, "namespace", "3", "1", "table3",
            "00000000000000000000000000000002", "host1:1000", "host1.apache.com,1000,1",0L,
            0L, 0L, 0L, new Size(300, Size.Unit.MEGABYTE), new Size(600, Size.Unit.MEGABYTE), 3,
            new Size(300, Size.Unit.MEGABYTE), 0.3f, "", 300L, 100L, 33.333336f,
            "2019-07-22 00:00:02");
          break;

        default:
          fail();
      }
    }
  }

  private static void assertRecordInRegionMode(Record record, String namespace, String startCode,
    String replicaId, String table, String region, String regionServer, String longRegionServer,
    long requestCountPerSecond, long readRequestCountPerSecond,
    long filteredReadRequestCountPerSecond, long writeCountRequestPerSecond,
    Size storeFileSize, Size uncompressedStoreFileSize, int numStoreFiles,
    Size memStoreSize, float Locality, String startKey, long compactingCellCount,
    long compactedCellCount, float compactionProgress, String lastMajorCompactionTime) {
    assertThat(record.size(), is(22));
    assertThat(record.get(Field.NAMESPACE).asString(), is(namespace));
    assertThat(record.get(Field.START_CODE).asString(), is(startCode));
    assertThat(record.get(Field.REPLICA_ID).asString(), is(replicaId));
    assertThat(record.get(Field.TABLE).asString(), is(table));
    assertThat(record.get(Field.REGION).asString(), is(region));
    assertThat(record.get(Field.REGION_SERVER).asString(), is(regionServer));
    assertThat(record.get(Field.LONG_REGION_SERVER).asString(), is(longRegionServer));
    assertThat(record.get(Field.REQUEST_COUNT_PER_SECOND).asLong(),
      is(requestCountPerSecond));
    assertThat(record.get(Field.READ_REQUEST_COUNT_PER_SECOND).asLong(),
      is(readRequestCountPerSecond));
    assertThat(record.get(Field.FILTERED_READ_REQUEST_COUNT_PER_SECOND).asLong(),
      is(filteredReadRequestCountPerSecond));
    assertThat(record.get(Field.WRITE_REQUEST_COUNT_PER_SECOND).asLong(),
      is(writeCountRequestPerSecond));
    assertThat(record.get(Field.STORE_FILE_SIZE).asSize(), is(storeFileSize));
    assertThat(record.get(Field.UNCOMPRESSED_STORE_FILE_SIZE).asSize(),
      is(uncompressedStoreFileSize));
    assertThat(record.get(Field.NUM_STORE_FILES).asInt(), is(numStoreFiles));
    assertThat(record.get(Field.MEM_STORE_SIZE).asSize(), is(memStoreSize));
    assertThat(record.get(Field.LOCALITY).asFloat(), is(Locality));
    assertThat(record.get(Field.START_KEY).asString(), is(startKey));
    assertThat(record.get(Field.COMPACTING_CELL_COUNT).asLong(), is(compactingCellCount));
    assertThat(record.get(Field.COMPACTED_CELL_COUNT).asLong(), is(compactedCellCount));
    assertThat(record.get(Field.COMPACTION_PROGRESS).asFloat(), is(compactionProgress));
    assertThat(record.get(Field.LAST_MAJOR_COMPACTION_TIME).asString(),
      is(lastMajorCompactionTime));
  }

  public static void assertRecordsInNamespaceMode(List<Record> records) {
    assertThat(records.size(), is(2));

    for (Record record : records) {
      switch (record.get(Field.NAMESPACE).asString()) {
        case "default":
          assertRecordInNamespaceMode(record, 0L, 0L, 0L, 0L, new Size(600, Size.Unit.MEGABYTE),
            new Size(1200, Size.Unit.MEGABYTE), 6, new Size(600, Size.Unit.MEGABYTE), 4);
          break;

        case "namespace":
          assertRecordInNamespaceMode(record, 0L, 0L, 0L, 0L, new Size(600, Size.Unit.MEGABYTE),
            new Size(1200, Size.Unit.MEGABYTE), 6, new Size(600, Size.Unit.MEGABYTE), 2);
          break;

        default:
          fail();
      }
    }
  }

  private static void assertRecordInNamespaceMode(Record record, long requestCountPerSecond,
    long readRequestCountPerSecond, long filteredReadRequestCountPerSecond,
    long writeCountRequestPerSecond, Size storeFileSize, Size uncompressedStoreFileSize,
    int numStoreFiles, Size memStoreSize, int regionCount) {
    assertThat(record.size(), is(10));
    assertThat(record.get(Field.REQUEST_COUNT_PER_SECOND).asLong(),
      is(requestCountPerSecond));
    assertThat(record.get(Field.READ_REQUEST_COUNT_PER_SECOND).asLong(),
      is(readRequestCountPerSecond));
    assertThat(record.get(Field.FILTERED_READ_REQUEST_COUNT_PER_SECOND).asLong(),
      is(filteredReadRequestCountPerSecond));
    assertThat(record.get(Field.WRITE_REQUEST_COUNT_PER_SECOND).asLong(),
      is(writeCountRequestPerSecond));
    assertThat(record.get(Field.STORE_FILE_SIZE).asSize(), is(storeFileSize));
    assertThat(record.get(Field.UNCOMPRESSED_STORE_FILE_SIZE).asSize(),
      is(uncompressedStoreFileSize));
    assertThat(record.get(Field.NUM_STORE_FILES).asInt(), is(numStoreFiles));
    assertThat(record.get(Field.MEM_STORE_SIZE).asSize(), is(memStoreSize));
    assertThat(record.get(Field.REGION_COUNT).asInt(), is(regionCount));
  }

  public static void assertRecordsInTableMode(List<Record> records) {
    assertThat(records.size(), is(3));

    for (Record record : records) {
      String tableName = String.format("%s:%s", record.get(Field.NAMESPACE).asString(),
        record.get(Field.TABLE).asString());

      switch (tableName) {
        case "default:table1":
          assertRecordInTableMode(record, 0L, 0L, 0L, 0L, new Size(200, Size.Unit.MEGABYTE),
            new Size(400, Size.Unit.MEGABYTE), 2, new Size(200, Size.Unit.MEGABYTE), 2);
          break;

        case "default:table2":
          assertRecordInTableMode(record, 0L, 0L, 0L, 0L, new Size(400, Size.Unit.MEGABYTE),
            new Size(800, Size.Unit.MEGABYTE), 4, new Size(400, Size.Unit.MEGABYTE), 2);
          break;

        case "namespace:table3":
          assertRecordInTableMode(record, 0L, 0L, 0L, 0L, new Size(600, Size.Unit.MEGABYTE),
            new Size(1200, Size.Unit.MEGABYTE), 6, new Size(600, Size.Unit.MEGABYTE), 2);
          break;

        default:
          fail();
      }
    }
  }

  private static void assertRecordInTableMode(Record record, long requestCountPerSecond,
    long readRequestCountPerSecond, long filteredReadRequestCountPerSecond,
    long writeCountRequestPerSecond, Size storeFileSize, Size uncompressedStoreFileSize,
    int numStoreFiles, Size memStoreSize, int regionCount) {
    assertThat(record.size(), is(11));
    assertThat(record.get(Field.REQUEST_COUNT_PER_SECOND).asLong(),
      is(requestCountPerSecond));
    assertThat(record.get(Field.READ_REQUEST_COUNT_PER_SECOND).asLong(),
      is(readRequestCountPerSecond));
    assertThat(record.get(Field.FILTERED_READ_REQUEST_COUNT_PER_SECOND).asLong(),
      is(filteredReadRequestCountPerSecond));
    assertThat(record.get(Field.WRITE_REQUEST_COUNT_PER_SECOND).asLong(),
      is(writeCountRequestPerSecond));
    assertThat(record.get(Field.STORE_FILE_SIZE).asSize(), is(storeFileSize));
    assertThat(record.get(Field.UNCOMPRESSED_STORE_FILE_SIZE).asSize(),
      is(uncompressedStoreFileSize));
    assertThat(record.get(Field.NUM_STORE_FILES).asInt(), is(numStoreFiles));
    assertThat(record.get(Field.MEM_STORE_SIZE).asSize(), is(memStoreSize));
    assertThat(record.get(Field.REGION_COUNT).asInt(), is(regionCount));
  }

  public static void assertRecordsInRegionServerMode(List<Record> records) {
    assertThat(records.size(), is(2));

    for (Record record : records) {
      switch (record.get(Field.REGION_SERVER).asString()) {
        case "host1:1000":
          assertRecordInRegionServerMode(record, "host1.apache.com,1000,1", 0L, 0L, 0L, 0L,
            new Size(600, Size.Unit.MEGABYTE), new Size(1200, Size.Unit.MEGABYTE), 6,
            new Size(600, Size.Unit.MEGABYTE), 3, new Size(100, Size.Unit.MEGABYTE),
            new Size(200, Size.Unit.MEGABYTE));
          break;

        case "host2:1001":
          assertRecordInRegionServerMode(record, "host2.apache.com,1001,2", 0L, 0L, 0L, 0L,
            new Size(600, Size.Unit.MEGABYTE), new Size(1200, Size.Unit.MEGABYTE), 6,
            new Size(600, Size.Unit.MEGABYTE), 3, new Size(16, Size.Unit.GIGABYTE),
            new Size(32, Size.Unit.GIGABYTE));
          break;

        default:
          fail();
      }
    }
  }

  private static void assertRecordInRegionServerMode(Record record, String longRegionServer,
    long requestCountPerSecond, long readRequestCountPerSecond,
    long filteredReadRequestCountPerSecond, long writeCountRequestPerSecond,
    Size storeFileSize, Size uncompressedStoreFileSize, int numStoreFiles,
    Size memStoreSize, int regionCount, Size usedHeapSize, Size maxHeapSize) {
    assertThat(record.size(), is(13));
    assertThat(record.get(Field.LONG_REGION_SERVER).asString(),
      is(longRegionServer));
    assertThat(record.get(Field.REQUEST_COUNT_PER_SECOND).asLong(),
      is(requestCountPerSecond));
    assertThat(record.get(Field.READ_REQUEST_COUNT_PER_SECOND).asLong(),
      is(readRequestCountPerSecond));
    assertThat(record.get(Field.FILTERED_READ_REQUEST_COUNT_PER_SECOND).asLong(),
      is(filteredReadRequestCountPerSecond));
    assertThat(record.get(Field.WRITE_REQUEST_COUNT_PER_SECOND).asLong(),
      is(writeCountRequestPerSecond));
    assertThat(record.get(Field.STORE_FILE_SIZE).asSize(), is(storeFileSize));
    assertThat(record.get(Field.UNCOMPRESSED_STORE_FILE_SIZE).asSize(),
      is(uncompressedStoreFileSize));
    assertThat(record.get(Field.NUM_STORE_FILES).asInt(), is(numStoreFiles));
    assertThat(record.get(Field.MEM_STORE_SIZE).asSize(), is(memStoreSize));
    assertThat(record.get(Field.REGION_COUNT).asInt(), is(regionCount));
    assertThat(record.get(Field.USED_HEAP_SIZE).asSize(), is(usedHeapSize));
    assertThat(record.get(Field.MAX_HEAP_SIZE).asSize(), is(maxHeapSize));
  }

  public static void assertSummary(Summary summary) {
    assertThat(summary.getVersion(), is("3.0.0-SNAPSHOT"));
    assertThat(summary.getClusterId(), is("01234567-89ab-cdef-0123-456789abcdef"));
    assertThat(summary.getServers(), is(3));
    assertThat(summary.getLiveServers(), is(2));
    assertThat(summary.getDeadServers(), is(1));
    assertThat(summary.getRegionCount(), is(6));
    assertThat(summary.getRitCount(), is(1));
    assertThat(summary.getAverageLoad(), is(3.0));
    assertThat(summary.getAggregateRequestPerSecond(), is(300L));
  }
}
