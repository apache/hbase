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
package org.apache.hadoop.hbase.regionserver.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Histogram;
import org.apache.hadoop.hbase.metrics.Meter;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsTableRequests {

  public static final String ENABLE_TABLE_LATENCIES_METRICS_KEY =
    "hbase.regionserver.enable.table.latencies";

  public static final boolean ENABLE_TABLE_LATENCIES_METRICS_DEFAULT = true;

  public static final String ENABLE_TABLE_QUERY_METER_METRICS_KEY =
    "hbase.regionserver.enable.table.query.meter";

  public static final boolean ENABLE_TABLE_QUERY_METER_METRICS_KEY_DEFAULT = false;

  /**
   * The name of the metrics
   */
  private final static String METRICS_NAME = "TableRequests";

  /**
   * The name of the metrics context that metrics will be under.
   */
  private final static String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  private final static String METRICS_DESCRIPTION =
    "Metrics about Tables on a single HBase RegionServer";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  private final static String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  private final static String GET_TIME = "getTime";
  private final static String SCAN_TIME = "scanTime";
  private final static String SCAN_SIZE = "scanSize";
  private final static String PUT_TIME = "putTime";
  private final static String PUT_BATCH_TIME = "putBatchTime";
  private final static String DELETE_TIME = "deleteTime";
  private final static String DELETE_BATCH_TIME = "deleteBatchTime";
  private final static String INCREMENT_TIME = "incrementTime";
  private final static String APPEND_TIME = "appendTime";
  private final static String CHECK_AND_DELETE_TIME = "checkAndDeleteTime";
  private final static String CHECK_AND_PUT_TIME = "checkAndPutTime";
  private final static String CHECK_AND_MUTATE_TIME = "checkAndMutateTime";
  String BLOCK_BYTES_SCANNED_KEY = "blockBytesScannedCount";
  String GET_BLOCK_BYTES_SCANNED_KEY = "getBlockBytesScanned";
  String SCAN_BLOCK_BYTES_SCANNED_KEY = "scanBlockBytesScanned";
  String CHECK_AND_MUTATE_BLOCK_BYTES_SCANNED_KEY = "checkAndMutateBlockBytesScanned";
  String INCREMENT_BLOCK_BYTES_SCANNED_KEY = "incrementBlockBytesScanned";
  String APPEND_BLOCK_BYTES_SCANNED_KEY = "appendBlockBytesScanned";
  private final static String TABLE_READ_QUERY_PER_SECOND = "tableReadQueryPerSecond";
  private final static String TABLE_WRITE_QUERY_PER_SECOND = "tableWriteQueryPerSecond";

  private Histogram getTimeHistogram;
  private Histogram scanTimeHistogram;
  private Histogram scanSizeHistogram;
  private Histogram putTimeHistogram;
  private Histogram putBatchTimeHistogram;
  private Histogram deleteTimeHistogram;
  private Histogram deleteBatchTimeHistogram;
  private Histogram incrementTimeHistogram;
  private Histogram appendTimeHistogram;
  private Histogram checkAndDeleteTimeHistogram;
  private Histogram checkAndPutTimeHistogram;
  private Histogram checkAndMutateTimeHistogram;
  private Counter blockBytesScannedCount;
  private Histogram checkAndMutateBlockBytesScanned;
  private Histogram getBlockBytesScanned;
  private Histogram incrementBlockBytesScanned;
  private Histogram appendBlockBytesScanned;
  private Histogram scanBlockBytesScanned;

  private Meter readMeter;
  private Meter writeMeter;

  private MetricRegistry registry;
  private TableName tableName;
  private Configuration conf;
  private MetricRegistryInfo registryInfo;

  private boolean enableTableLatenciesMetrics;
  private boolean enabTableQueryMeterMetrics;

  public boolean isEnableTableLatenciesMetrics() {
    return enableTableLatenciesMetrics;
  }

  public boolean isEnabTableQueryMeterMetrics() {
    return enabTableQueryMeterMetrics;
  }

  public MetricsTableRequests(TableName tableName, Configuration conf) {
    init(tableName, conf);
  }

  private void init(TableName tableName, Configuration conf) {
    this.tableName = tableName;
    this.conf = conf;
    enableTableLatenciesMetrics = this.conf.getBoolean(ENABLE_TABLE_LATENCIES_METRICS_KEY,
      ENABLE_TABLE_LATENCIES_METRICS_DEFAULT);
    enabTableQueryMeterMetrics = this.conf.getBoolean(ENABLE_TABLE_QUERY_METER_METRICS_KEY,
      ENABLE_TABLE_QUERY_METER_METRICS_KEY_DEFAULT);
    if (enableTableLatenciesMetrics || enabTableQueryMeterMetrics) {
      registry = createRegistryForTableRequests();
      if (enableTableLatenciesMetrics) {
        getTimeHistogram = registry.histogram(GET_TIME);
        scanTimeHistogram = registry.histogram(SCAN_TIME);
        scanSizeHistogram = registry.histogram(SCAN_SIZE);
        putTimeHistogram = registry.histogram(PUT_TIME);
        putBatchTimeHistogram = registry.histogram(PUT_BATCH_TIME);
        deleteTimeHistogram = registry.histogram(DELETE_TIME);
        deleteBatchTimeHistogram = registry.histogram(DELETE_BATCH_TIME);
        incrementTimeHistogram = registry.histogram(INCREMENT_TIME);
        appendTimeHistogram = registry.histogram(APPEND_TIME);
        checkAndDeleteTimeHistogram = registry.histogram(CHECK_AND_DELETE_TIME);
        checkAndPutTimeHistogram = registry.histogram(CHECK_AND_PUT_TIME);
        checkAndMutateTimeHistogram = registry.histogram(CHECK_AND_MUTATE_TIME);
        blockBytesScannedCount = registry.counter(BLOCK_BYTES_SCANNED_KEY);
        checkAndMutateBlockBytesScanned =
          registry.histogram(CHECK_AND_MUTATE_BLOCK_BYTES_SCANNED_KEY);
        getBlockBytesScanned = registry.histogram(GET_BLOCK_BYTES_SCANNED_KEY);
        incrementBlockBytesScanned = registry.histogram(INCREMENT_BLOCK_BYTES_SCANNED_KEY);
        appendBlockBytesScanned = registry.histogram(APPEND_BLOCK_BYTES_SCANNED_KEY);
        scanBlockBytesScanned = registry.histogram(SCAN_BLOCK_BYTES_SCANNED_KEY);
      }

      if (enabTableQueryMeterMetrics) {
        readMeter = registry.meter(TABLE_READ_QUERY_PER_SECOND);
        writeMeter = registry.meter(TABLE_WRITE_QUERY_PER_SECOND);
      }
    }
  }

  private MetricRegistry createRegistryForTableRequests() {
    return MetricRegistries.global().create(createRegistryInfoForTableRequests());
  }

  private MetricRegistryInfo createRegistryInfoForTableRequests() {
    registryInfo = new MetricRegistryInfo(qualifyMetrics(METRICS_NAME, tableName),
      METRICS_DESCRIPTION, qualifyMetrics(METRICS_JMX_CONTEXT, tableName), METRICS_CONTEXT, false);
    return registryInfo;
  }

  public void removeRegistry() {
    if (enableTableLatenciesMetrics || enabTableQueryMeterMetrics) {
      MetricRegistries.global().remove(registry.getMetricRegistryInfo());
    }
  }

  private static String qualifyMetrics(String prefix, TableName tableName) {
    StringBuilder sb = new StringBuilder();
    sb.append(prefix).append("_");
    sb.append("Namespace_").append(tableName.getNamespaceAsString());
    sb.append("_table_").append(tableName.getQualifierAsString());
    return sb.toString();
  }

  /**
   * Update the Put time histogram
   * @param t time it took
   */
  public void updatePut(long t) {
    if (isEnableTableLatenciesMetrics()) {
      putTimeHistogram.update(t);
    }
  }

  /**
   * Update the batch Put time histogram
   * @param t time it took
   */
  public void updatePutBatch(long t) {
    if (isEnableTableLatenciesMetrics()) {
      putBatchTimeHistogram.update(t);
    }
  }

  /**
   * Update the Delete time histogram
   * @param t time it took
   */
  public void updateDelete(long t) {
    if (isEnableTableLatenciesMetrics()) {
      deleteTimeHistogram.update(t);
    }
  }

  /**
   * Update the batch Delete time histogram
   * @param t time it took
   */
  public void updateDeleteBatch(long t) {
    if (isEnableTableLatenciesMetrics()) {
      deleteBatchTimeHistogram.update(t);
    }
  }

  /**
   * Update the Get time histogram .
   * @param time              time it took
   * @param blockBytesScanned size of block bytes scanned to retrieve the response
   */
  public void updateGet(long time, long blockBytesScanned) {
    if (isEnableTableLatenciesMetrics()) {
      getTimeHistogram.update(time);
      if (blockBytesScanned > 0) {
        blockBytesScannedCount.increment(blockBytesScanned);
        getBlockBytesScanned.update(blockBytesScanned);
      }
    }
  }

  /**
   * Update the Increment time histogram.
   * @param time              time it took
   * @param blockBytesScanned size of block bytes scanned to retrieve the response
   */
  public void updateIncrement(long time, long blockBytesScanned) {
    if (isEnableTableLatenciesMetrics()) {
      incrementTimeHistogram.update(time);
      if (blockBytesScanned > 0) {
        blockBytesScannedCount.increment(blockBytesScanned);
        incrementBlockBytesScanned.update(blockBytesScanned);
      }
    }
  }

  /**
   * Update the Append time histogram.
   * @param time              time it took
   * @param blockBytesScanned size of block bytes scanned to retrieve the response
   */
  public void updateAppend(long time, long blockBytesScanned) {
    if (isEnableTableLatenciesMetrics()) {
      appendTimeHistogram.update(time);
      if (blockBytesScanned > 0) {
        blockBytesScannedCount.increment(blockBytesScanned);
        appendBlockBytesScanned.update(blockBytesScanned);
      }
    }
  }

  /**
   * Update the scan metrics.
   * @param time              response time of scan
   * @param responseCellSize  size of the scan resposne
   * @param blockBytesScanned size of block bytes scanned to retrieve the response
   */
  public void updateScan(long time, long responseCellSize, long blockBytesScanned) {
    if (isEnableTableLatenciesMetrics()) {
      scanTimeHistogram.update(time);
      scanSizeHistogram.update(responseCellSize);
      if (blockBytesScanned > 0) {
        blockBytesScannedCount.increment(blockBytesScanned);
        scanBlockBytesScanned.update(blockBytesScanned);
      }
    }
  }

  /**
   * Update the CheckAndDelete time histogram.
   * @param time time it took
   */
  public void updateCheckAndDelete(long time) {
    if (isEnableTableLatenciesMetrics()) {
      checkAndDeleteTimeHistogram.update(time);
    }
  }

  /**
   * Update the CheckAndPut time histogram.
   * @param time time it took
   */
  public void updateCheckAndPut(long time) {
    if (isEnableTableLatenciesMetrics()) {
      checkAndPutTimeHistogram.update(time);
    }
  }

  /**
   * Update the CheckAndMutate time histogram.
   * @param time time it took
   */
  public void updateCheckAndMutate(long time, long blockBytesScanned) {
    if (isEnableTableLatenciesMetrics()) {
      checkAndMutateTimeHistogram.update(time);
      if (blockBytesScanned > 0) {
        blockBytesScannedCount.increment(blockBytesScanned);
        checkAndMutateBlockBytesScanned.update(blockBytesScanned);
      }
    }
  }

  /**
   * Update table read QPS
   * @param count Number of occurrences to record
   */
  public void updateTableReadQueryMeter(long count) {
    if (isEnabTableQueryMeterMetrics()) {
      readMeter.mark(count);
    }
  }

  /**
   * Update table read QPS
   */
  public void updateTableReadQueryMeter() {
    if (isEnabTableQueryMeterMetrics()) {
      readMeter.mark();
    }
  }

  /**
   * Update table write QPS
   * @param count Number of occurrences to record
   */
  public void updateTableWriteQueryMeter(long count) {
    if (isEnabTableQueryMeterMetrics()) {
      writeMeter.mark(count);
    }
  }

  /**
   * Update table write QPS
   */
  public void updateTableWriteQueryMeter() {
    if (isEnabTableQueryMeterMetrics()) {
      writeMeter.mark();
    }
  }

  // Visible for testing
  public MetricRegistryInfo getMetricRegistryInfo() {
    return registryInfo;
  }

  // Visible for testing
  public MetricRegistry getMetricRegistry() {
    return registry;
  }
}
