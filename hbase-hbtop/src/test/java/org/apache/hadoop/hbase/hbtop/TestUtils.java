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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.Size;
import org.apache.hadoop.hbase.hbtop.field.Size.Unit;
import org.apache.hadoop.hbase.hbtop.screen.top.Summary;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;

public final class TestUtils {

  static final String HBASE_VERSION = "1.5.0-SNAPSHOT";
  static final String CLUSTER_UUID = "01234567-89ab-cdef-0123-456789abcdef";

  private TestUtils() { }

  public static ClusterStatus createDummyClusterStatus() {
    Map<ServerName,ServerLoad> serverLoads = Maps.newHashMap();
    List<ServerName> deadServers = Lists.newArrayList();
    Map<String,RegionState> rit = Maps.newHashMap();

    ServerName host1 = ServerName.valueOf("host1.apache.com", 1000, 1);

    serverLoads.put(host1,
      createServerLoad(100,
        new Size(100, Size.Unit.MEGABYTE), new Size(200, Size.Unit.MEGABYTE), 100,
        Lists.newArrayList(
          createRegionLoad("table1,,1.00000000000000000000000000000000.", 100, 100,
            new Size(100, Size.Unit.MEGABYTE), new Size(200, Size.Unit.MEGABYTE), 1,
            new Size(100, Size.Unit.MEGABYTE), 0.1f, 100, 100, "2019-07-22 00:00:00"),
          createRegionLoad("table2,1,2.00000000000000000000000000000001.", 200, 200,
            new Size(200, Size.Unit.MEGABYTE), new Size(400, Size.Unit.MEGABYTE), 2,
            new Size(200, Size.Unit.MEGABYTE), 0.2f, 50, 200, "2019-07-22 00:00:01"),
          createRegionLoad(
            "namespace:table3,,3_0001.00000000000000000000000000000002.", 300, 300,
            new Size(300, Size.Unit.MEGABYTE), new Size(600, Size.Unit.MEGABYTE), 3,
            new Size(300, Size.Unit.MEGABYTE), 0.3f, 100, 300, "2019-07-22 00:00:02"))));

    ServerName host2 = ServerName.valueOf("host2.apache.com", 1001, 2);

    serverLoads.put(host2,
      createServerLoad(200,
        new Size(16, Size.Unit.GIGABYTE), new Size(32, Size.Unit.GIGABYTE), 200,
        Lists.newArrayList(
          createRegionLoad("table1,1,4.00000000000000000000000000000003.", 100, 100,
            new Size(100, Size.Unit.MEGABYTE), new Size(200, Size.Unit.MEGABYTE), 1,
            new Size(100, Size.Unit.MEGABYTE), 0.4f, 50, 100, "2019-07-22 00:00:03"),
          createRegionLoad("table2,,5.00000000000000000000000000000004.", 200, 200,
            new Size(200, Size.Unit.MEGABYTE), new Size(400, Size.Unit.MEGABYTE), 2,
            new Size(200, Size.Unit.MEGABYTE), 0.5f, 150, 200, "2019-07-22 00:00:04"),
          createRegionLoad("namespace:table3,,6.00000000000000000000000000000005.", 300, 300,
            new Size(300, Size.Unit.MEGABYTE), new Size(600, Size.Unit.MEGABYTE), 3,
            new Size(300, Size.Unit.MEGABYTE), 0.6f, 200, 300, "2019-07-22 00:00:05"))));

    ServerName host3 = ServerName.valueOf("host3.apache.com", 1002, 3);

    deadServers.add(host3);

    rit.put("table4", new RegionState(new HRegionInfo(0, TableName.valueOf("table4"), 0),
      RegionState.State.OFFLINE, host3));

    return new ClusterStatus(HBASE_VERSION, CLUSTER_UUID, serverLoads, deadServers, null, null,
      rit, new String[0], true);
  }

  private static ClusterStatusProtos.RegionLoad createRegionLoad(String regionName,
    long readRequestCount, long writeRequestCount, Size storeFileSize,
    Size uncompressedStoreFileSize, int storeFileCount, Size memStoreSize, float locality,
    long compactedCellCount, long compactingCellCount, String lastMajorCompactionTime) {
    FastDateFormat df = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    try {
      return ClusterStatusProtos.RegionLoad.newBuilder()
        .setRegionSpecifier(HBaseProtos.RegionSpecifier.newBuilder()
          .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME)
          .setValue(ByteString.copyFromUtf8(regionName)).build())
        .setReadRequestsCount(readRequestCount)
        .setWriteRequestsCount(writeRequestCount)
        .setStorefileSizeMB((int)storeFileSize.get(Unit.MEGABYTE))
        .setStoreUncompressedSizeMB((int)uncompressedStoreFileSize.get(Unit.MEGABYTE))
        .setStorefiles(storeFileCount)
        .setMemstoreSizeMB((int)memStoreSize.get(Unit.MEGABYTE))
        .setDataLocality(locality)
        .setCurrentCompactedKVs(compactedCellCount)
        .setTotalCompactingKVs(compactingCellCount)
        .setLastMajorCompactionTs(df.parse(lastMajorCompactionTime).getTime())
        .build();
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static ServerLoad createServerLoad(long reportTimestamp,
    Size usedHeapSize, Size maxHeapSize, long requestCountPerSecond,
    List<ClusterStatusProtos.RegionLoad> regionLoads) {
    return new ServerLoad(ClusterStatusProtos.ServerLoad.newBuilder()
        .setReportStartTime(reportTimestamp)
        .setReportEndTime(reportTimestamp)
        .setUsedHeapMB((int)usedHeapSize.get(Unit.MEGABYTE))
        .setMaxHeapMB((int)maxHeapSize.get(Unit.MEGABYTE))
        .setNumberOfRequests(requestCountPerSecond)
        .addAllRegionLoads(regionLoads)
        .build());
  }

  public static void assertRecordsInRegionMode(List<Record> records) {
    assertEquals(6, records.size());

    for (Record record : records) {
      switch (record.get(Field.REGION_NAME).asString()) {
        case "table1,,1.00000000000000000000000000000000.":
          assertRecordInRegionMode(record, "default", "1", "", "table1",
            "00000000000000000000000000000000", "host1:1000", "host1.apache.com,1000,1",
            0L, 0L, 0L, new Size(100, Size.Unit.MEGABYTE), new Size(200, Size.Unit.MEGABYTE), 1,
            new Size(100, Size.Unit.MEGABYTE), 0.1f, "", 100L, 100L, 100f,
            "2019-07-22 00:00:00");
          break;

        case "table1,1,4.00000000000000000000000000000003.":
          assertRecordInRegionMode(record, "default", "4", "", "table1",
            "00000000000000000000000000000003", "host2:1001", "host2.apache.com,1001,2",
            0L, 0L, 0L, new Size(100, Size.Unit.MEGABYTE), new Size(200, Size.Unit.MEGABYTE), 1,
            new Size(100, Size.Unit.MEGABYTE), 0.4f, "1", 100L, 50L, 50f,
            "2019-07-22 00:00:03");
          break;

        case "table2,,5.00000000000000000000000000000004.":
          assertRecordInRegionMode(record, "default", "5", "", "table2",
            "00000000000000000000000000000004", "host2:1001", "host2.apache.com,1001,2",
            0L, 0L, 0L, new Size(200, Size.Unit.MEGABYTE), new Size(400, Size.Unit.MEGABYTE), 2,
            new Size(200, Size.Unit.MEGABYTE), 0.5f, "", 200L, 150L, 75f,
            "2019-07-22 00:00:04");
          break;

        case "table2,1,2.00000000000000000000000000000001.":
          assertRecordInRegionMode(record, "default", "2", "", "table2",
            "00000000000000000000000000000001", "host1:1000", "host1.apache.com,1000,1",
            0L, 0L, 0L, new Size(200, Size.Unit.MEGABYTE), new Size(400, Size.Unit.MEGABYTE), 2,
            new Size(200, Size.Unit.MEGABYTE), 0.2f, "1", 200L, 50L, 25f,
            "2019-07-22 00:00:01");
          break;

        case "namespace:table3,,6.00000000000000000000000000000005.":
          assertRecordInRegionMode(record, "namespace", "6", "", "table3",
            "00000000000000000000000000000005", "host2:1001", "host2.apache.com,1001,2",
            0L, 0L, 0L, new Size(300, Size.Unit.MEGABYTE), new Size(600, Size.Unit.MEGABYTE), 3,
            new Size(300, Size.Unit.MEGABYTE), 0.6f, "", 300L, 200L, 66.66667f,
            "2019-07-22 00:00:05");
          break;

        case "namespace:table3,,3_0001.00000000000000000000000000000002.":
          assertRecordInRegionMode(record, "namespace", "3", "1", "table3",
            "00000000000000000000000000000002", "host1:1000", "host1.apache.com,1000,1",
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
    long requestCountPerSecond, long readRequestCountPerSecond, long writeCountRequestPerSecond,
    Size storeFileSize, Size uncompressedStoreFileSize, int numStoreFiles,
    Size memStoreSize, float locality, String startKey, long compactingCellCount,
    long compactedCellCount, float compactionProgress, String lastMajorCompactionTime) {
    assertEquals(21, record.size());
    assertEquals(namespace, record.get(Field.NAMESPACE).asString());
    assertEquals(startCode, record.get(Field.START_CODE).asString());
    assertEquals(replicaId, record.get(Field.REPLICA_ID).asString());
    assertEquals(table, record.get(Field.TABLE).asString());
    assertEquals(region, record.get(Field.REGION).asString());
    assertEquals(regionServer, record.get(Field.REGION_SERVER).asString());
    assertEquals(longRegionServer, record.get(Field.LONG_REGION_SERVER).asString());
    assertEquals(requestCountPerSecond, record.get(Field.REQUEST_COUNT_PER_SECOND).asLong());
    assertEquals(readRequestCountPerSecond,
      record.get(Field.READ_REQUEST_COUNT_PER_SECOND).asLong());
    assertEquals(writeCountRequestPerSecond,
      record.get(Field.WRITE_REQUEST_COUNT_PER_SECOND).asLong());
    assertEquals(storeFileSize, record.get(Field.STORE_FILE_SIZE).asSize());
    assertEquals(uncompressedStoreFileSize,
      record.get(Field.UNCOMPRESSED_STORE_FILE_SIZE).asSize());
    assertEquals(numStoreFiles, record.get(Field.NUM_STORE_FILES).asInt());
    assertEquals(record.get(Field.MEM_STORE_SIZE).asSize(), memStoreSize);
    assertEquals(locality, record.get(Field.LOCALITY).asFloat(), 0.001);
    assertEquals(startKey, record.get(Field.START_KEY).asString());
    assertEquals(compactingCellCount, record.get(Field.COMPACTING_CELL_COUNT).asLong());
    assertEquals(compactedCellCount, record.get(Field.COMPACTED_CELL_COUNT).asLong());
    assertEquals(compactionProgress, record.get(Field.COMPACTION_PROGRESS).asFloat(), 0.001);
    assertEquals(lastMajorCompactionTime,
      record.get(Field.LAST_MAJOR_COMPACTION_TIME).asString());
  }

  public static void assertRecordsInNamespaceMode(List<Record> records) {
    assertEquals(2, records.size());

    for (Record record : records) {
      switch (record.get(Field.NAMESPACE).asString()) {
        case "default":
          assertRecordInNamespaceMode(record, 0L, 0L, 0L, new Size(600, Size.Unit.MEGABYTE),
            new Size(1200, Size.Unit.MEGABYTE), 6, new Size(600, Size.Unit.MEGABYTE), 4);
          break;

        case "namespace":
          assertRecordInNamespaceMode(record, 0L, 0L, 0L, new Size(600, Size.Unit.MEGABYTE),
            new Size(1200, Size.Unit.MEGABYTE), 6, new Size(600, Size.Unit.MEGABYTE), 2);
          break;

        default:
          fail();
      }
    }
  }

  private static void assertRecordInNamespaceMode(Record record, long requestCountPerSecond,
    long readRequestCountPerSecond, long writeCountRequestPerSecond, Size storeFileSize,
    Size uncompressedStoreFileSize, int numStoreFiles, Size memStoreSize, int regionCount) {
    assertEquals(9, record.size());
    assertEquals(requestCountPerSecond, record.get(Field.REQUEST_COUNT_PER_SECOND).asLong());
    assertEquals(readRequestCountPerSecond,
      record.get(Field.READ_REQUEST_COUNT_PER_SECOND).asLong());
    assertEquals(writeCountRequestPerSecond,
      record.get(Field.WRITE_REQUEST_COUNT_PER_SECOND).asLong());
    assertEquals(storeFileSize, record.get(Field.STORE_FILE_SIZE).asSize());
    assertEquals(uncompressedStoreFileSize,
      record.get(Field.UNCOMPRESSED_STORE_FILE_SIZE).asSize());
    assertEquals(numStoreFiles, record.get(Field.NUM_STORE_FILES).asInt());
    assertEquals(memStoreSize, record.get(Field.MEM_STORE_SIZE).asSize());
    assertEquals(regionCount, record.get(Field.REGION_COUNT).asInt());
  }

  public static void assertRecordsInTableMode(List<Record> records) {
    assertEquals(3, records.size());

    for (Record record : records) {
      String tableName = String.format("%s:%s", record.get(Field.NAMESPACE).asString(),
        record.get(Field.TABLE).asString());

      switch (tableName) {
        case "default:table1":
          assertRecordInTableMode(record, 0L, 0L, 0L, new Size(200, Size.Unit.MEGABYTE),
            new Size(400, Size.Unit.MEGABYTE), 2, new Size(200, Size.Unit.MEGABYTE), 2);
          break;

        case "default:table2":
          assertRecordInTableMode(record, 0L, 0L, 0L, new Size(400, Size.Unit.MEGABYTE),
            new Size(800, Size.Unit.MEGABYTE), 4, new Size(400, Size.Unit.MEGABYTE), 2);
          break;

        case "namespace:table3":
          assertRecordInTableMode(record, 0L, 0L, 0L, new Size(600, Size.Unit.MEGABYTE),
            new Size(1200, Size.Unit.MEGABYTE), 6, new Size(600, Size.Unit.MEGABYTE), 2);
          break;

        default:
          fail();
      }
    }
  }

  private static void assertRecordInTableMode(Record record, long requestCountPerSecond,
    long readRequestCountPerSecond,  long writeCountRequestPerSecond, Size storeFileSize,
    Size uncompressedStoreFileSize, int numStoreFiles, Size memStoreSize, int regionCount) {
    assertEquals(10, record.size());
    assertEquals(requestCountPerSecond, record.get(Field.REQUEST_COUNT_PER_SECOND).asLong());
    assertEquals(readRequestCountPerSecond,
      record.get(Field.READ_REQUEST_COUNT_PER_SECOND).asLong());
    assertEquals(writeCountRequestPerSecond,
      record.get(Field.WRITE_REQUEST_COUNT_PER_SECOND).asLong());
    assertEquals(storeFileSize, record.get(Field.STORE_FILE_SIZE).asSize());
    assertEquals(uncompressedStoreFileSize,
      record.get(Field.UNCOMPRESSED_STORE_FILE_SIZE).asSize());
    assertEquals(numStoreFiles, record.get(Field.NUM_STORE_FILES).asInt());
    assertEquals(memStoreSize, record.get(Field.MEM_STORE_SIZE).asSize());
    assertEquals(regionCount, record.get(Field.REGION_COUNT).asInt());
  }

  public static void assertRecordsInRegionServerMode(List<Record> records) {
    assertEquals(2, records.size());

    for (Record record : records) {
      switch (record.get(Field.REGION_SERVER).asString()) {
        case "host1:1000":
          assertRecordInRegionServerMode(record, "host1.apache.com,1000,1", 0L, 0L, 0L,
            new Size(600, Size.Unit.MEGABYTE), new Size(1200, Size.Unit.MEGABYTE), 6,
            new Size(600, Size.Unit.MEGABYTE), 3, new Size(100, Size.Unit.MEGABYTE),
            new Size(200, Size.Unit.MEGABYTE));
          break;

        case "host2:1001":
          assertRecordInRegionServerMode(record, "host2.apache.com,1001,2", 0L, 0L, 0L,
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
    long requestCountPerSecond, long readRequestCountPerSecond, long writeCountRequestPerSecond,
    Size storeFileSize, Size uncompressedStoreFileSize, int numStoreFiles,
    Size memStoreSize, int regionCount, Size usedHeapSize, Size maxHeapSize) {
    assertEquals(12, record.size());
    assertEquals(longRegionServer, record.get(Field.LONG_REGION_SERVER).asString());
    assertEquals(requestCountPerSecond, record.get(Field.REQUEST_COUNT_PER_SECOND).asLong());
    assertEquals(readRequestCountPerSecond,
      record.get(Field.READ_REQUEST_COUNT_PER_SECOND).asLong());
    assertEquals(writeCountRequestPerSecond,
      record.get(Field.WRITE_REQUEST_COUNT_PER_SECOND).asLong());
    assertEquals(storeFileSize, record.get(Field.STORE_FILE_SIZE).asSize());
    assertEquals(uncompressedStoreFileSize,
      record.get(Field.UNCOMPRESSED_STORE_FILE_SIZE).asSize());
    assertEquals(numStoreFiles, record.get(Field.NUM_STORE_FILES).asInt());
    assertEquals(memStoreSize, record.get(Field.MEM_STORE_SIZE).asSize());
    assertEquals(regionCount, record.get(Field.REGION_COUNT).asInt());
    assertEquals(usedHeapSize, record.get(Field.USED_HEAP_SIZE).asSize());
    assertEquals(maxHeapSize, record.get(Field.MAX_HEAP_SIZE).asSize());
  }

  public static void assertSummary(Summary summary) {
    assertEquals(HBASE_VERSION, summary.getVersion());
    assertEquals(CLUSTER_UUID, summary.getClusterId());
    assertEquals(3, summary.getServers());
    assertEquals(2, summary.getLiveServers());
    assertEquals(1, summary.getDeadServers());
    assertEquals(6, summary.getRegionCount());
    assertEquals(1, summary.getRitCount());
    assertEquals(3.0, summary.getAverageLoad(), 0.001);
    assertEquals(300L, summary.getAggregateRequestPerSecond());
  }
}
