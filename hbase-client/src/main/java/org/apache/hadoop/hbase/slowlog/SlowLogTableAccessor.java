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

package org.apache.hadoop.hbase.slowlog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
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

  private static Connection connection;

  /**
   * hbase:slowlog table name - can be enabled
   * with config - hbase.regionserver.slowlog.systable.enabled
   */
  public static final TableName SLOW_LOG_TABLE_NAME =
    TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "slowlog");

  private static void doPut(final Connection connection, final List<Put> puts)
      throws IOException {
    try (Table table = connection.getTable(SLOW_LOG_TABLE_NAME)) {
      table.put(puts);
    }
  }

  /**
   * Add slow/large log records to hbase:slowlog table
   * @param slowLogPayloads List of SlowLogPayload to process
   * @param configuration Configuration to use for connection
   */
  public static void addSlowLogRecords(final List<TooSlowLog.SlowLogPayload> slowLogPayloads,
      final Configuration configuration) {
    List<Put> puts = new ArrayList<>(slowLogPayloads.size());
    for (TooSlowLog.SlowLogPayload slowLogPayload : slowLogPayloads) {
      final byte[] rowKey = getRowKey(slowLogPayload);
      final Put put = new Put(rowKey).setDurability(Durability.SKIP_WAL)
        .setPriority(HConstants.NORMAL_QOS)
        .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("call_details"),
          Bytes.toBytes(slowLogPayload.getCallDetails()))
        .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("client_address"),
          Bytes.toBytes(slowLogPayload.getClientAddress()))
        .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("method_name"),
          Bytes.toBytes(slowLogPayload.getMethodName()))
        .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("param"),
          Bytes.toBytes(slowLogPayload.getParam()))
        .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("processing_time"),
          Bytes.toBytes(Integer.toString(slowLogPayload.getProcessingTime())))
        .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("queue_time"),
          Bytes.toBytes(Integer.toString(slowLogPayload.getQueueTime())))
        .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("region_name"),
          Bytes.toBytes(slowLogPayload.getRegionName()))
        .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("response_size"),
          Bytes.toBytes(Long.toString(slowLogPayload.getResponseSize())))
        .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("server_class"),
          Bytes.toBytes(slowLogPayload.getServerClass()))
        .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("start_time"),
          Bytes.toBytes(Long.toString(slowLogPayload.getStartTime())))
        .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("type"),
          Bytes.toBytes(slowLogPayload.getType().name()))
        .addColumn(HConstants.SLOWLOG_INFO_FAMILY, Bytes.toBytes("username"),
          Bytes.toBytes(slowLogPayload.getUserName()));
      puts.add(put);
    }
    try {
      if (connection == null) {
        createConnection(configuration);
      }
      doPut(connection, puts);
    } catch (Exception e) {
      LOG.warn("Failed to add slow/large log records to hbase:slowlog table.", e);
    }
  }

  private static synchronized void createConnection(Configuration configuration)
      throws IOException {
    Configuration conf = new Configuration(configuration);
    // rpc timeout: 20s
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 20000);
    // retry count: 5
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5);
    conf.setInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER, 1);
    connection = ConnectionFactory.createConnection(conf);
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
      lastFiveDig = String.valueOf(ThreadLocalRandom.current().nextInt(99999));
    }
    final long currentTimeMillis = EnvironmentEdgeManager.currentTime();
    final String timeAndHashcode = currentTimeMillis + lastFiveDig;
    final long rowKeyLong = Long.parseLong(timeAndHashcode);
    return Bytes.toBytes(rowKeyLong);
  }

}
