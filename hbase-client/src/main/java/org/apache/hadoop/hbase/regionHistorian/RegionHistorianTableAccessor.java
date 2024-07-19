/*
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

package org.apache.hadoop.hbase.regionHistorian;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionHist;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class RegionHistorianTableAccessor {
  private static final Logger LOG = LoggerFactory.getLogger(RegionHistorianTableAccessor.class);

  public static final TableName REGION_HISTORIAN_TABLE_NAME = TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "regionHistorian");

  private static void doPut(final Connection connection, final List<Put> puts) throws IOException {
    try (Table table = connection.getTable(REGION_HISTORIAN_TABLE_NAME)) {
      table.put(puts);
    }
  }

  public static void addRegionHistorianRecords(final List<RegionHist.RegionHistorianPayload> regionHistorianPayloads, Connection connection) {
    List<Put> puts = new ArrayList<>(regionHistorianPayloads.size());
    for (RegionHist.RegionHistorianPayload regionHistorianPayload : regionHistorianPayloads){
      final byte[] rowKey = getRowKey(regionHistorianPayload);
      final Put put = new Put(rowKey).setDurability(Durability.SKIP_WAL)
        .setPriority(HConstants.NORMAL_QOS)
        .addColumn(HConstants.REGIONHISTORIAN_INFO_FAMILY, Bytes.toBytes("host_name"),
          Bytes.toBytes(regionHistorianPayload.getHostName()))
        .addColumn(HConstants.REGIONHISTORIAN_INFO_FAMILY, Bytes.toBytes("region_name"),
          Bytes.toBytes(regionHistorianPayload.getRegionName()))
        .addColumn(HConstants.REGIONHISTORIAN_INFO_FAMILY, Bytes.toBytes("table_name"),
          Bytes.toBytes(regionHistorianPayload.getTableName()))
        .addColumn(HConstants.REGIONHISTORIAN_INFO_FAMILY, Bytes.toBytes("event_type"),
          Bytes.toBytes(regionHistorianPayload.getEventType()))
        .addColumn(HConstants.REGIONHISTORIAN_INFO_FAMILY, Bytes.toBytes("event_timestamp"),
          Bytes.toBytes(regionHistorianPayload.getEventTimestamp()))
        .addColumn(HConstants.REGIONHISTORIAN_INFO_FAMILY, Bytes.toBytes("pid"),
          Bytes.toBytes(regionHistorianPayload.getPid()))
        .addColumn(HConstants.REGIONHISTORIAN_INFO_FAMILY, Bytes.toBytes("ppid"),
          Bytes.toBytes(regionHistorianPayload.getPpid()));
      puts.add(put);
    }
    try {
      doPut(connection, puts);
    } catch (Exception e) {
      LOG.warn("Failed to add regionHistorian records to hbase:regionHistorian table.",e);
    }
  }

  /**
   * Create rowKey: regionName APPEND eventTimestamp APPEND hostname
   * @param regionHistorianPayload RegionHistorianPayload to process
   * @return rowKey byte[]
   */
  private static byte[] getRowKey(final RegionHist.RegionHistorianPayload regionHistorianPayload) {
    final long eventTimestamp = regionHistorianPayload.getEventTimestamp();
    final String regionName = regionHistorianPayload.getRegionName();
    final String hostName = regionHistorianPayload.getHostName();
    final String rowKeyString = regionName +"||"+ eventTimestamp +"||"+ hostName;
    return Bytes.toBytes(rowKeyString);
  }
}
