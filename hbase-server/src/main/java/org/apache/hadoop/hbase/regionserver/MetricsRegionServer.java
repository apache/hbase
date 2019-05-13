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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This class is for maintaining the various regionserver statistics
 * and publishing them through the metrics interfaces.
 * </p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values.
 */
@InterfaceStability.Evolving
@InterfaceAudience.Private
public class MetricsRegionServer {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsRegionServer.class);

  private MetricsRegionServerSource serverSource;
  private MetricsRegionServerWrapper regionServerWrapper;
  private RegionServerTableMetrics tableMetrics;
  private final MetricsTable metricsTable;
  private MetricsRegionServerQuotaSource quotaSource;
  private final Mode modeForTableMetrics, modeForMetricsTable; // See ctor comment.

  private MetricRegistry metricRegistry;
  private Timer bulkLoadTimer;

  public MetricsRegionServer(MetricsRegionServerWrapper regionServerWrapper, Configuration conf,
      MetricsTable metricsTable) {
    this(regionServerWrapper,
        CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
            .createServer(regionServerWrapper),
        createTableMetrics(conf),
        metricsTable);

    // Create hbase-metrics module based metrics. The registry should already be registered by the
    // MetricsRegionServerSource
    metricRegistry = MetricRegistries.global().get(serverSource.getMetricRegistryInfo()).get();

    // create and use metrics from the new hbase-metrics based registry.
    bulkLoadTimer = metricRegistry.timer("Bulkload");

    quotaSource = CompatibilitySingletonFactory.getInstance(MetricsRegionServerQuotaSource.class);
  }

  private enum Mode {
    TABLE,
    SERVER,
    BOTH
  }

  MetricsRegionServer(MetricsRegionServerWrapper regionServerWrapper,
                      MetricsRegionServerSource serverSource,
                      RegionServerTableMetrics tableMetrics,
                      MetricsTable metricsTable) {
    this.regionServerWrapper = regionServerWrapper;
    this.serverSource = serverSource;
    this.tableMetrics = tableMetrics;
    this.metricsTable = metricsTable;
    // This is ugly, but that's how it is. We have tableMetrics, and metricsTable...
    this.modeForTableMetrics = (tableMetrics == null) ? Mode.SERVER
        : (tableMetrics.isScoped() ? Mode.TABLE : Mode.BOTH);
    this.modeForMetricsTable = (metricsTable == null) ? Mode.SERVER
        : (metricsTable.isScoped() ? Mode.TABLE : Mode.BOTH);
  }

  /**
   * Creates an instance of {@link RegionServerTableMetrics} only if the feature is enabled.
   */
  static RegionServerTableMetrics createTableMetrics(Configuration conf) {
    if (MetricsTableSourceImpl.areTableLatenciesEnabled(conf)) {
      return new RegionServerTableMetrics(conf);
    }
    return null;
  }

  @VisibleForTesting
  public MetricsRegionServerSource getMetricsSource() {
    return serverSource;
  }

  public MetricsRegionServerWrapper getRegionServerWrapper() {
    return regionServerWrapper;
  }

  /**
   * Basically checks table name object for nulls.
   * TODO: why would we ever have nulls if per-table is on?
   */
  private static Mode deriveMode(Mode mode, Object tn) {
    return tn == null ? Mode.SERVER : mode;
  }

  public void updatePutBatch(TableName tn, long t) {
    switch (deriveMode(modeForTableMetrics, tn)) {
      case TABLE:
        tableMetrics.updatePutBatch(tn, t);
        break;
      case SERVER:
        serverSource.updatePutBatch(t);
        break;
      case BOTH:
        tableMetrics.updatePutBatch(tn, t);
        serverSource.updatePutBatch(t);
        break;
    }

    if (t > 1000) {
      serverSource.incrSlowPut();
    }
  }

  public void updatePut(TableName tn, long t) {
    switch (deriveMode(modeForTableMetrics, tn)) {
      case TABLE:
        tableMetrics.updatePut(tn, t);
        break;
      case SERVER:
        serverSource.updatePut(t);
        break;
      case BOTH:
        tableMetrics.updatePut(tn, t);
        serverSource.updatePut(t);
        break;
    }
  }

  public void updateDelete(TableName tn, long t) {
    switch (deriveMode(modeForTableMetrics, tn)) {
      case TABLE:
        tableMetrics.updateDelete(tn, t);
        break;
      case SERVER:
        serverSource.updateDelete(t);
        break;
      case BOTH:
        tableMetrics.updateDelete(tn, t);
        serverSource.updateDelete(t);
        break;
    }
  }

  public void updateDeleteBatch(TableName tn, long t) {
    switch (deriveMode(modeForTableMetrics, tn)) {
      case TABLE:
        tableMetrics.updateDeleteBatch(tn, t);
        break;
      case SERVER:
        serverSource.updateDeleteBatch(t);
        break;
      case BOTH:
        tableMetrics.updateDeleteBatch(tn, t);
        serverSource.updateDeleteBatch(t);
        break;
    }
    if (t > 1000) {
      serverSource.incrSlowDelete();
    }
  }

  public void updateCheckAndDelete(long t) {
    // TODO: add to table metrics?
    serverSource.updateCheckAndDelete(t);
  }

  public void updateCheckAndPut(long t) {
    // TODO: add to table metrics?
    serverSource.updateCheckAndPut(t);
  }

  public void updateGet(TableName tn, long t) {
    switch (deriveMode(modeForTableMetrics, tn)) {
      case TABLE:
        tableMetrics.updateGet(tn, t);
        break;
      case SERVER:
        serverSource.updateGet(t);
        break;
      case BOTH:
        tableMetrics.updateGet(tn, t);
        serverSource.updateGet(t);
        break;
    }
    if (t > 1000) {
      serverSource.incrSlowGet();
    }
  }

  public void updateIncrement(TableName tn, long t) {
    switch (deriveMode(modeForTableMetrics, tn)) {
      case TABLE:
        tableMetrics.updateIncrement(tn, t);
        break;
      case SERVER:
        serverSource.updateIncrement(t);
        break;
      case BOTH:
        tableMetrics.updateIncrement(tn, t);
        serverSource.updateIncrement(t);
        break;
    }
    if (t > 1000) {
      serverSource.incrSlowIncrement();
    }
  }

  public void updateAppend(TableName tn, long t) {
    switch (deriveMode(modeForTableMetrics, tn)) {
      case TABLE:
        tableMetrics.updateAppend(tn, t);
        break;
      case SERVER:
        serverSource.updateAppend(t);
        break;
      case BOTH:
        tableMetrics.updateAppend(tn, t);
        serverSource.updateAppend(t);
        break;
    }
    if (t > 1000) {
      serverSource.incrSlowAppend();
    }
  }

  public void updateReplay(long t) {
    // TODO: add to table metrics?
    serverSource.updateReplay(t);
  }

  public void updateScanSize(TableName tn, long scanSize) {
    switch (deriveMode(modeForTableMetrics, tn)) {
      case TABLE:
        tableMetrics.updateScanSize(tn, scanSize);
        break;
      case SERVER:
        serverSource.updateScanSize(scanSize);
        break;
      case BOTH:
        tableMetrics.updateScanSize(tn, scanSize);
        serverSource.updateScanSize(scanSize);
        break;
    }
  }

  public void updateScanTime(TableName tn, long t) {
    switch (deriveMode(modeForTableMetrics, tn)) {
      case TABLE:
        tableMetrics.updateScanTime(tn, t);
        break;
      case SERVER:
        serverSource.updateScanTime(t);
        break;
      case BOTH:
        tableMetrics.updateScanTime(tn, t);
        serverSource.updateScanTime(t);
        break;
    }
  }

  // TODO: never called
  public void updateSplitTime(String tn, long t) {
    switch (deriveMode(modeForMetricsTable, tn)) {
      case TABLE:
        metricsTable.updateSplitTime(tn, t);
        break;
      case SERVER:
        serverSource.updateScanTime(t);
        break;
      case BOTH:
        metricsTable.updateSplitTime(tn, t);
        serverSource.updateSplitTime(t);
        break;
    }
  }

  public void incrSplitRequest(String tn) {
    switch (deriveMode(modeForMetricsTable, tn)) {
      case TABLE:
        metricsTable.incrSplitRequest(tn);
        break;
      case SERVER:
        serverSource.incrSplitRequest();
        break;
      case BOTH:
        metricsTable.incrSplitRequest(tn);
        serverSource.incrSplitRequest();
        break;
    }
  }

  // TODO: never called
  public void incrSplitSuccess(String tn) {
    switch (deriveMode(modeForMetricsTable, tn)) {
      case TABLE:
        metricsTable.incrSplitSuccess(tn);
        break;
      case SERVER:
        serverSource.incrSplitSuccess();
        break;
      case BOTH:
        metricsTable.incrSplitSuccess(tn);
        serverSource.incrSplitSuccess();
        break;
    }
  }

  public void updateFlush(String table, long t, long memstoreSize, long fileSize) {
    switch (deriveMode(modeForMetricsTable, table)) {
      case TABLE:
        updateFlushTbl(table, t, memstoreSize, fileSize);
        break;
      case SERVER:
        updateFlushSrv(t, memstoreSize, fileSize);
        break;
      case BOTH:
        updateFlushTbl(table, t, memstoreSize, fileSize);
        updateFlushSrv(t, memstoreSize, fileSize);
        break;
    }
  }

  private void updateFlushSrv(long t, long memstoreSize, long fileSize) {
    serverSource.updateFlushTime(t);
    serverSource.updateFlushMemStoreSize(memstoreSize);
    serverSource.updateFlushOutputSize(fileSize);
  }

  private void updateFlushTbl(String table, long t, long memstoreSize, long fileSize) {
    metricsTable.updateFlushTime(table, t);
    metricsTable.updateFlushMemstoreSize(table, memstoreSize);
    metricsTable.updateFlushOutputSize(table, fileSize);
  }

  public void updateCompaction(String table, boolean isMajor, long t, int inputFileCount,
      int outputFileCount, long inputBytes, long outputBytes) {
    switch (deriveMode(modeForMetricsTable, table)) {
      case TABLE:
        updateCompactionTbl(table, isMajor, t, inputFileCount,
          outputFileCount, inputBytes, outputBytes);
        break;
      case SERVER:
        updateCompactionSrv(isMajor, t, inputFileCount,
          outputFileCount, inputBytes, outputBytes);
        break;
      case BOTH:
        updateCompactionTbl(table, isMajor, t, inputFileCount,
          outputFileCount, inputBytes, outputBytes);
        updateCompactionSrv(isMajor, t, inputFileCount,
          outputFileCount, inputBytes, outputBytes);
        break;
    }
  }

  private void updateCompactionTbl(String table, boolean isMajor, long t, int inputFileCount,
      int outputFileCount, long inputBytes, long outputBytes) {
    metricsTable.updateCompactionTime(table, isMajor, t);
    metricsTable.updateCompactionInputFileCount(table, isMajor, inputFileCount);
    metricsTable.updateCompactionOutputFileCount(table, isMajor, outputFileCount);
    metricsTable.updateCompactionInputSize(table, isMajor, inputBytes);
    metricsTable.updateCompactionOutputSize(table, isMajor, outputBytes);
  }

  private void updateCompactionSrv(boolean isMajor, long t, int inputFileCount,
      int outputFileCount, long inputBytes, long outputBytes) {
    serverSource.updateCompactionTime(isMajor, t);
    serverSource.updateCompactionInputFileCount(isMajor, inputFileCount);
    serverSource.updateCompactionOutputFileCount(isMajor, outputFileCount);
    serverSource.updateCompactionInputSize(isMajor, inputBytes);
    serverSource.updateCompactionOutputSize(isMajor, outputBytes);
  }

  public void updateBulkLoad(long millis) {
    // TODO: add to table metrics?
    this.bulkLoadTimer.updateMillis(millis);
  }

  /**
   * @see MetricsRegionServerQuotaSource#incrementNumRegionSizeReportsSent(long)
   */
  public void incrementNumRegionSizeReportsSent(long numReportsSent) {
    quotaSource.incrementNumRegionSizeReportsSent(numReportsSent);
  }

  /**
   * @see MetricsRegionServerQuotaSource#incrementRegionSizeReportingChoreTime(long)
   */
  public void incrementRegionSizeReportingChoreTime(long time) {
    quotaSource.incrementRegionSizeReportingChoreTime(time);
  }
}
