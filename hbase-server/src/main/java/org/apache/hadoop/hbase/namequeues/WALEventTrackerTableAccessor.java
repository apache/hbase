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
package org.apache.hadoop.hbase.namequeues;

import static org.apache.hadoop.hbase.master.waleventtracker.WALEventTrackerTableCreator.WAL_EVENT_TRACKER_INFO_FAMILY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class WALEventTrackerTableAccessor {
  private static final Logger LOG = LoggerFactory.getLogger(WALEventTrackerTableAccessor.class);

  public static final String RS_COLUMN = "region_server_name";
  public static final String WAL_NAME_COLUMN = "wal_name";
  public static final String TIMESTAMP_COLUMN = "timestamp";
  public static final String WAL_STATE_COLUMN = "wal_state";
  public static final String WAL_LENGTH_COLUMN = "wal_length";
  public static final String MAX_ATTEMPTS_KEY = "wal.event.tracker.max.attempts";
  public static final String SLEEP_INTERVAL_KEY = "wal.event.tracker.sleep.interval.msec";
  public static final String MAX_SLEEP_TIME_KEY = "wal.event.tracker.max.sleep.time.msec";
  public static final int DEFAULT_MAX_ATTEMPTS = 3;
  public static final long DEFAULT_SLEEP_INTERVAL = 1000L; // 1 second
  public static final long DEFAULT_MAX_SLEEP_TIME = 60000L; // 60 seconds
  public static final String WAL_EVENT_TRACKER_TABLE_NAME_STR = "REPLICATION.WALEVENTTRACKER";
  public static final String DELIMITER = "_";

  private WALEventTrackerTableAccessor() {
  }

  /**
   * {@link #WAL_EVENT_TRACKER_TABLE_NAME_STR} table name - can be enabled with config -
   * hbase.regionserver.wal.event.tracker.enabled
   */
  public static final TableName WAL_EVENT_TRACKER_TABLE_NAME =
    TableName.valueOf(WAL_EVENT_TRACKER_TABLE_NAME_STR);

  private static void doPut(final Connection connection, final List<Put> puts) throws Exception {
    RetryCounter retryCounter = getRetryFactory(connection.getConfiguration()).create();
    while (true) {
      try (Table table = connection.getTable(WAL_EVENT_TRACKER_TABLE_NAME)) {
        table.put(puts);
        return;
      } catch (IOException ioe) {
        retryOrThrow(retryCounter, ioe);
      }
      retryCounter.sleepUntilNextRetry();
    }
  }

  private static RetryCounterFactory getRetryFactory(Configuration conf) {
    int maxAttempts = conf.getInt(MAX_ATTEMPTS_KEY, DEFAULT_MAX_ATTEMPTS);
    long sleepIntervalMs = conf.getLong(SLEEP_INTERVAL_KEY, DEFAULT_SLEEP_INTERVAL);
    long maxSleepTimeMs = conf.getLong(MAX_SLEEP_TIME_KEY, DEFAULT_MAX_SLEEP_TIME);
    RetryCounter.RetryConfig retryConfig =
      new RetryCounter.RetryConfig(maxAttempts, sleepIntervalMs, maxSleepTimeMs,
        TimeUnit.MILLISECONDS, new RetryCounter.ExponentialBackoffPolicyWithLimit());
    return new RetryCounterFactory(retryConfig);
  }

  private static void retryOrThrow(RetryCounter retryCounter, IOException ioe) throws IOException {
    if (retryCounter.shouldRetry()) {
      return;
    }
    throw ioe;
  }

  /**
   * Add wal event tracker rows to hbase:waleventtracker table
   * @param walEventPayloads List of walevents to process
   * @param connection       Connection to use.
   */
  public static void addWalEventTrackerRows(Queue<WALEventTrackerPayload> walEventPayloads,
    final Connection connection) throws Exception {
    List<Put> puts = new ArrayList<>(walEventPayloads.size());
    for (WALEventTrackerPayload payload : walEventPayloads) {
      final byte[] rowKey = getRowKey(payload);
      final Put put = new Put(rowKey);
      // TODO Do we need to SKIP_WAL ?
      put.setPriority(HConstants.NORMAL_QOS);
      put
        .addColumn(WAL_EVENT_TRACKER_INFO_FAMILY, Bytes.toBytes(RS_COLUMN),
          Bytes.toBytes(payload.getRsName()))
        .addColumn(WAL_EVENT_TRACKER_INFO_FAMILY, Bytes.toBytes(WAL_NAME_COLUMN),
          Bytes.toBytes(payload.getWalName()))
        .addColumn(WAL_EVENT_TRACKER_INFO_FAMILY, Bytes.toBytes(TIMESTAMP_COLUMN),
          Bytes.toBytes(payload.getTimeStamp()))
        .addColumn(WAL_EVENT_TRACKER_INFO_FAMILY, Bytes.toBytes(WAL_STATE_COLUMN),
          Bytes.toBytes(payload.getState()))
        .addColumn(WAL_EVENT_TRACKER_INFO_FAMILY, Bytes.toBytes(WAL_LENGTH_COLUMN),
          Bytes.toBytes(payload.getWalLength()));
      puts.add(put);
    }
    doPut(connection, puts);
  }

  /**
   * Create rowKey: 1. We want RS name to be the leading part of rowkey so that we can query by RS
   * name filter. WAL name contains rs name as a leading part. 2. Timestamp when the event was
   * generated. 3. Add state of the wal. Combination of 1 + 2 + 3 is definitely going to create a
   * unique rowkey.
   * @param payload payload to process
   * @return rowKey byte[]
   */
  public static byte[] getRowKey(final WALEventTrackerPayload payload) {
    String walName = payload.getWalName();
    // converting to string since this will help seeing the timestamp in string format using
    // hbase shell commands.
    String timestampStr = String.valueOf(payload.getTimeStamp());
    String walState = payload.getState();
    final String rowKeyStr = walName + DELIMITER + timestampStr + DELIMITER + walState;
    return Bytes.toBytes(rowKeyStr);
  }
}
