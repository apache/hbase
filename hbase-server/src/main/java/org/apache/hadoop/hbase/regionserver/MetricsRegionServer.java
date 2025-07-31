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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.metrics.Meter;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.Timer;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.regionserver.metrics.MetricsThrottleExceptions;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Maintains regionserver statistics and publishes them through the metrics interfaces. This class
 * has a number of metrics variables that are publicly accessible; these variables (objects) have
 * methods to update their values. Batch your updates rather than call on each instance else all
 * threads will do nothing but contend trying to maintain metric counters!
 */
@InterfaceStability.Evolving
@InterfaceAudience.Private
public class MetricsRegionServer {
  public static final String RS_ENABLE_SERVER_QUERY_METER_METRICS_KEY =
    "hbase.regionserver.enable.server.query.meter";
  public static final boolean RS_ENABLE_SERVER_QUERY_METER_METRICS_KEY_DEFAULT = false;
  public static final String SLOW_METRIC_TIME = "hbase.ipc.slow.metric.time";
  private final MetricsRegionServerSource serverSource;
  private final MetricsRegionServerWrapper regionServerWrapper;
  private final MetricsTable metricsTable;
  private MetricsRegionServerQuotaSource quotaSource;
  private final MetricsUserAggregate userAggregate;

  private MetricRegistry metricRegistry;
  private MetricsThrottleExceptions throttleMetrics;
  private Timer bulkLoadTimer;
  // Incremented once for each call to Scan#nextRaw
  private Meter serverReadQueryMeter;
  // Incremented per write.
  private Meter serverWriteQueryMeter;
  protected long slowMetricTime;
  protected static final int DEFAULT_SLOW_METRIC_TIME = 1000; // milliseconds

  public MetricsRegionServer(MetricsRegionServerWrapper regionServerWrapper, Configuration conf,
    MetricsTable metricsTable) {
    this(regionServerWrapper,
      CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
        .createServer(regionServerWrapper),
      metricsTable, MetricsUserAggregateFactory.getMetricsUserAggregate(conf));

    // Create hbase-metrics module based metrics. The registry should already be registered by the
    // MetricsRegionServerSource
    metricRegistry = MetricRegistries.global().get(serverSource.getMetricRegistryInfo()).get();

    // create and use metrics from the new hbase-metrics based registry.
    bulkLoadTimer = metricRegistry.timer("Bulkload");

    slowMetricTime = conf.getLong(SLOW_METRIC_TIME, DEFAULT_SLOW_METRIC_TIME);
    quotaSource = CompatibilitySingletonFactory.getInstance(MetricsRegionServerQuotaSource.class);
    if (
      conf.getBoolean(RS_ENABLE_SERVER_QUERY_METER_METRICS_KEY,
        RS_ENABLE_SERVER_QUERY_METER_METRICS_KEY_DEFAULT)
    ) {
      serverReadQueryMeter = metricRegistry.meter("ServerReadQueryPerSecond");
      serverWriteQueryMeter = metricRegistry.meter("ServerWriteQueryPerSecond");
    }

    throttleMetrics = new MetricsThrottleExceptions(metricRegistry);
  }

  MetricsRegionServer(MetricsRegionServerWrapper regionServerWrapper,
    MetricsRegionServerSource serverSource, MetricsTable metricsTable,
    MetricsUserAggregate userAggregate) {
    this.regionServerWrapper = regionServerWrapper;
    this.serverSource = serverSource;
    this.metricsTable = metricsTable;
    this.userAggregate = userAggregate;
  }

  public MetricsRegionServerSource getMetricsSource() {
    return serverSource;
  }

  public MetricsUserAggregate getMetricsUserAggregate() {
    return userAggregate;
  }

  public MetricsRegionServerWrapper getRegionServerWrapper() {
    return regionServerWrapper;
  }

  public void updatePutBatch(HRegion region, long t) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updatePutBatch(t);
    }
    serverSource.updatePutBatch(t);
  }

  public void updatePut(HRegion region, long t) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updatePut(t);
    }
    if (t > slowMetricTime) {
      serverSource.incrSlowPut();
    }
    serverSource.updatePut(t);
    userAggregate.updatePut(t);
  }

  public void updateDelete(HRegion region, long t) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateDelete(t);
    }
    if (t > slowMetricTime) {
      serverSource.incrSlowDelete();
    }
    serverSource.updateDelete(t);
    userAggregate.updateDelete(t);
  }

  public void updateDeleteBatch(HRegion region, long t) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateDeleteBatch(t);
    }
    serverSource.updateDeleteBatch(t);
  }

  public void updateCheckAndDelete(HRegion region, long t) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateCheckAndDelete(t);
    }
    serverSource.updateCheckAndDelete(t);
  }

  public void updateCheckAndPut(HRegion region, long t) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateCheckAndPut(t);
    }
    serverSource.updateCheckAndPut(t);
  }

  public void updateCheckAndMutate(HRegion region, long time, long blockBytesScanned) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateCheckAndMutate(time, blockBytesScanned);
    }
    serverSource.updateCheckAndMutate(time, blockBytesScanned);
    userAggregate.updateCheckAndMutate(blockBytesScanned);
  }

  public void updateGet(HRegion region, long time, long blockBytesScanned) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateGet(time, blockBytesScanned);
    }
    if (time > slowMetricTime) {
      serverSource.incrSlowGet();
    }
    serverSource.updateGet(time, blockBytesScanned);
    userAggregate.updateGet(time, blockBytesScanned);
  }

  public void updateIncrement(HRegion region, long time, long blockBytesScanned) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateIncrement(time, blockBytesScanned);
    }
    if (time > slowMetricTime) {
      serverSource.incrSlowIncrement();
    }
    serverSource.updateIncrement(time, blockBytesScanned);
    userAggregate.updateIncrement(time, blockBytesScanned);
  }

  public void updateAppend(HRegion region, long time, long blockBytesScanned) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateAppend(time, blockBytesScanned);
    }
    if (time > slowMetricTime) {
      serverSource.incrSlowAppend();
    }
    serverSource.updateAppend(time, blockBytesScanned);
    userAggregate.updateAppend(time, blockBytesScanned);
  }

  public void updateReplay(long t) {
    serverSource.updateReplay(t);
    userAggregate.updateReplay(t);
  }

  public void updateScan(HRegion region, long time, long responseCellSize, long blockBytesScanned) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateScan(time, responseCellSize, blockBytesScanned);
    }
    serverSource.updateScan(time, responseCellSize, blockBytesScanned);
    userAggregate.updateScan(time, blockBytesScanned);
  }

  public void updateSplitTime(long t) {
    serverSource.updateSplitTime(t);
  }

  public void incrSplitRequest() {
    serverSource.incrSplitRequest();
  }

  public void incrSplitSuccess() {
    serverSource.incrSplitSuccess();
  }

  public void updateFlush(String table, long t, long memstoreSize, long fileSize) {
    serverSource.updateFlushTime(t);
    serverSource.updateFlushMemStoreSize(memstoreSize);
    serverSource.updateFlushOutputSize(fileSize);

    if (table != null) {
      metricsTable.updateFlushTime(table, t);
      metricsTable.updateFlushMemstoreSize(table, memstoreSize);
      metricsTable.updateFlushOutputSize(table, fileSize);
    }

  }

  public void updateCompaction(String table, boolean isMajor, long t, int inputFileCount,
    int outputFileCount, long inputBytes, long outputBytes) {
    serverSource.updateCompactionTime(isMajor, t);
    serverSource.updateCompactionInputFileCount(isMajor, inputFileCount);
    serverSource.updateCompactionOutputFileCount(isMajor, outputFileCount);
    serverSource.updateCompactionInputSize(isMajor, inputBytes);
    serverSource.updateCompactionOutputSize(isMajor, outputBytes);

    if (table != null) {
      metricsTable.updateCompactionTime(table, isMajor, t);
      metricsTable.updateCompactionInputFileCount(table, isMajor, inputFileCount);
      metricsTable.updateCompactionOutputFileCount(table, isMajor, outputFileCount);
      metricsTable.updateCompactionInputSize(table, isMajor, inputBytes);
      metricsTable.updateCompactionOutputSize(table, isMajor, outputBytes);
    }
  }

  public void updateBulkLoad(long millis) {
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

  public void updateReadQueryMeter(HRegion region, long count) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateTableReadQueryMeter(count);
    }
    if (serverReadQueryMeter != null) {
      serverReadQueryMeter.mark(count);
    }
  }

  public void updateWriteQueryMeter(HRegion region, long count) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateTableWriteQueryMeter(count);
    }
    if (serverWriteQueryMeter != null) {
      serverWriteQueryMeter.mark(count);
    }
  }

  public void updateWriteQueryMeter(HRegion region) {
    if (region.getMetricsTableRequests() != null) {
      region.getMetricsTableRequests().updateTableWriteQueryMeter();
    }
    if (serverWriteQueryMeter != null) {
      serverWriteQueryMeter.mark();
    }
  }

  public void incrScannerLeaseExpired() {
    serverSource.incrScannerLeaseExpired();
  }

  /**
   * Record a throttle exception with contextual information.
   * @param throttleType the type of throttle exception from RpcThrottlingException.Type enum
   * @param user         the user who triggered the throttle
   * @param table        the table that was being accessed
   */
  public void recordThrottleException(RpcThrottlingException.Type throttleType, String user,
    String table) {
    throttleMetrics.recordThrottleException(throttleType, user, table);
  }

}
