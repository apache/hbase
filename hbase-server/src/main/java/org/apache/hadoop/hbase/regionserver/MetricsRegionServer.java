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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.metrics.Meter;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.Timer;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

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
  public static final String RS_ENABLE_TABLE_METRICS_KEY =
      "hbase.regionserver.enable.table.latencies";
  public static final boolean RS_ENABLE_TABLE_METRICS_DEFAULT = true;

  private final MetricsRegionServerSource serverSource;
  private final MetricsRegionServerWrapper regionServerWrapper;
  private final RegionServerTableMetrics tableMetrics;
  private final MetricsUserAggregate userAggregate;

  private MetricRegistry metricRegistry;
  private Timer bulkLoadTimer;
  private Meter serverReadQueryMeter;
  private Meter serverWriteQueryMeter;

  public MetricsRegionServer(MetricsRegionServerWrapper regionServerWrapper, Configuration conf) {
    this(regionServerWrapper,
        CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
            .createServer(regionServerWrapper), createTableMetrics(conf),
        MetricsUserAggregateFactory.getMetricsUserAggregate(conf));

    // Create hbase-metrics module based metrics. The registry should already be registered by the
    // MetricsRegionServerSource
    metricRegistry = MetricRegistries.global().get(serverSource.getMetricRegistryInfo()).get();

    // create and use metrics from the new hbase-metrics based registry.
    bulkLoadTimer = metricRegistry.timer("Bulkload");
    serverReadQueryMeter = metricRegistry.meter("ServerReadQueryPerSecond");
    serverWriteQueryMeter = metricRegistry.meter("ServerWriteQueryPerSecond");
  }

  MetricsRegionServer(MetricsRegionServerWrapper regionServerWrapper,
      MetricsRegionServerSource serverSource, RegionServerTableMetrics tableMetrics,
      MetricsUserAggregate userAggregate) {
    this.regionServerWrapper = regionServerWrapper;
    this.serverSource = serverSource;
    this.tableMetrics = tableMetrics;
    this.userAggregate = userAggregate;
  }

  /**
   * Creates an instance of {@link RegionServerTableMetrics} only if the feature is enabled.
   */
  static RegionServerTableMetrics createTableMetrics(Configuration conf) {
    if (conf.getBoolean(RS_ENABLE_TABLE_METRICS_KEY, RS_ENABLE_TABLE_METRICS_DEFAULT)) {
      return new RegionServerTableMetrics();
    }
    return null;
  }

  @VisibleForTesting
  public MetricsRegionServerSource getMetricsSource() {
    return serverSource;
  }

  @VisibleForTesting
  public org.apache.hadoop.hbase.regionserver.MetricsUserAggregate getMetricsUserAggregate() {
    return userAggregate;
  }

  public MetricsRegionServerWrapper getRegionServerWrapper() {
    return regionServerWrapper;
  }

  public void updatePutBatch(TableName tn, long t) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updatePutBatch(tn, t);
    }
    if (t > 1000) {
      serverSource.incrSlowPut();
    }
    serverSource.updatePutBatch(t);
  }

  public void updatePut(TableName tn, long t) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updatePut(tn, t);
    }
    serverSource.updatePut(t);
    userAggregate.updatePut(t);
  }

  public void updateDelete(TableName tn, long t) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateDelete(tn, t);
    }
    serverSource.updateDelete(t);
    userAggregate.updateDelete(t);
  }

  public void updateDeleteBatch(TableName tn, long t) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateDeleteBatch(tn, t);
    }
    if (t > 1000) {
      serverSource.incrSlowDelete();
    }
    serverSource.updateDeleteBatch(t);
  }

  public void updateCheckAndDelete(long t) {
    serverSource.updateCheckAndDelete(t);
  }

  public void updateCheckAndPut(long t) {
    serverSource.updateCheckAndPut(t);
  }

  public void updateGet(TableName tn, long t) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateGet(tn, t);
    }
    if (t > 1000) {
      serverSource.incrSlowGet();
    }
    serverSource.updateGet(t);
    userAggregate.updateGet(t);
  }

  public void updateIncrement(TableName tn, long t) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateIncrement(tn, t);
    }
    if (t > 1000) {
      serverSource.incrSlowIncrement();
    }
    serverSource.updateIncrement(t);
    userAggregate.updateIncrement(t);
  }

  public void updateAppend(TableName tn, long t) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateAppend(tn, t);
    }
    if (t > 1000) {
      serverSource.incrSlowAppend();
    }
    serverSource.updateAppend(t);
    userAggregate.updateAppend(t);
  }

  public void updateReplay(long t){
    serverSource.updateReplay(t);
    userAggregate.updateReplay(t);
  }

  public void updateScanSize(TableName tn, long scanSize){
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateScanSize(tn, scanSize);
    }
    serverSource.updateScanSize(scanSize);
  }

  public void updateScanTime(TableName tn, long t) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateScanTime(tn, t);
    }
    serverSource.updateScanTime(t);
    userAggregate.updateScanTime(t);
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

  public void updateFlush(long t, long memstoreSize, long fileSize) {
    serverSource.updateFlushTime(t);
    serverSource.updateFlushMemstoreSize(memstoreSize);
    serverSource.updateFlushOutputSize(fileSize);
  }

  public void updateCompaction(boolean isMajor, long t, int inputFileCount, int outputFileCount,
      long inputBytes, long outputBytes) {
    serverSource.updateCompactionTime(isMajor, t);
    serverSource.updateCompactionInputFileCount(isMajor, inputFileCount);
    serverSource.updateCompactionOutputFileCount(isMajor, outputFileCount);
    serverSource.updateCompactionInputSize(isMajor, inputBytes);
    serverSource.updateCompactionOutputSize(isMajor, outputBytes);
  }

  public void updateBulkLoad(long millis) {
    this.bulkLoadTimer.updateMillis(millis);
  }


  public void updateReadQueryMeter(TableName tn, long count) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateTableReadQueryMeter(tn, count);
    }
    this.serverReadQueryMeter.mark(count);
  }

  public void updateReadQueryMeter(TableName tn) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateTableReadQueryMeter(tn);
    }
    this.serverReadQueryMeter.mark();
  }

  public void updateWriteQueryMeter(TableName tn, long count) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateTableWriteQueryMeter(tn, count);
    }
    this.serverWriteQueryMeter.mark(count);
  }

  public void updateWriteQueryMeter(TableName tn) {
    if (tableMetrics != null && tn != null) {
      tableMetrics.updateTableWriteQueryMeter(tn);
    }
    this.serverWriteQueryMeter.mark();
  }
}
