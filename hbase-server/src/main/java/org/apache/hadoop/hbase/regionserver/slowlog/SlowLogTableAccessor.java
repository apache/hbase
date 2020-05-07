/*
 *
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

package org.apache.hadoop.hbase.regionserver.slowlog;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TooSlowLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Slowlog Accessor to record slow/large RPC log identified at each RegionServer RpcServer level.
 * This can be done only optionally to record the entire history of slow/large rpc calls
 * since RingBuffer can handle only limited latest records.
 */
@InterfaceAudience.Private
public class SlowLogTableAccessor {

  private static final Logger LOG = LoggerFactory.getLogger(SlowLogTableAccessor.class);

  private static final Random RANDOM = new Random();

  private static void doPut(final Connection connection, final Put put)
      throws IOException {
    try (Table table = connection.getTable(TableName.SLOW_LOG_TABLE_NAME)) {
      table.put(put);
    }
  }

  /**
   * Add slow/large log records to hbase:slowlog table
   *
   * @param slowLogPayload SlowLogPayload to process
   * @param connection Connection to put data
   */
  public static void addSlowLogRecord(final TooSlowLog.SlowLogPayload slowLogPayload,
      final Connection connection) {
    final byte[] rowKey = getRowKey(slowLogPayload);
    final Put put = new Put(rowKey)
      .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("call_details"),
        Bytes.toBytes(slowLogPayload.getCallDetails()))
      .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("client_address"),
        Bytes.toBytes(slowLogPayload.getClientAddress()))
      .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("method_name"),
        Bytes.toBytes(slowLogPayload.getMethodName()))
      .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("param"),
        Bytes.toBytes(slowLogPayload.getParam()))
      .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("processing_time"),
        Bytes.toBytes(slowLogPayload.getProcessingTime()))
      .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("queue_time"),
        Bytes.toBytes(slowLogPayload.getQueueTime()))
      .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("region_name"),
        Bytes.toBytes(slowLogPayload.getRegionName()))
      .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("response_size"),
        Bytes.toBytes(slowLogPayload.getResponseSize()))
      .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("server_class"),
        Bytes.toBytes(slowLogPayload.getServerClass()))
      .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("start_time"),
        Bytes.toBytes(slowLogPayload.getStartTime()))
      .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("type"),
        Bytes.toBytes(slowLogPayload.getType().name()))
      .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("username"),
        Bytes.toBytes(slowLogPayload.getUserName()));
    try {
      doPut(connection, put);
    } catch (IOException e) {
      LOG.error("Failed to add slow/large log record to hbase:slowlog table for region: {}",
        slowLogPayload.getRegionName(), e);
    }
  }

  /**
   * Create rowKey: currentTimeMillis APPEND slowLogPayload.hashcode
   * Scan on slowlog table should keep records with sorted order of time, however records
   * added at the very same time (currentTimeMillis) could be in random order.
   *
   * @param slowLogPayload SlowLogPayload to process
   * @return rowKey byte[]
   */
  private static byte[] getRowKey(final TooSlowLog.SlowLogPayload slowLogPayload) {
    String hashcode = String.valueOf(slowLogPayload.hashCode());
    String lastFiveDig =
      hashcode.substring((hashcode.length() > 5) ? (hashcode.length() - 5) : 0);
    if (lastFiveDig.startsWith("-")) {
      lastFiveDig = String.valueOf(RANDOM.nextInt(99999));
    }
    final long currentTimeMillis = EnvironmentEdgeManager.currentTime();
    final String timeAndHashcode = currentTimeMillis + lastFiveDig;
    final long rowKeyLong = Long.parseLong(timeAndHashcode);
    return Bytes.toBytes(rowKeyLong);
  }

}
