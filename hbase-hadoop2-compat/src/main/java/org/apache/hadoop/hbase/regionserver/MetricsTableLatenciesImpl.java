/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableSizeHistogram;
import org.apache.hadoop.metrics2.lib.MutableTimeHistogram;
import org.apache.yetus.audience.InterfaceAudience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;import org.slf4j.Logger;import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link MetricsTableLatencies} to track latencies for one table in a
 * RegionServer.
 */
@InterfaceAudience.Private
public class MetricsTableLatenciesImpl extends BaseSourceImpl implements MetricsTableLatencies {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsTableLatenciesImpl.class);

  private final ConcurrentMap<TableName, TableHistograms> histogramsByTable =
    Maps.newConcurrentMap();
  private Configuration conf;
  private boolean useTags;

  @VisibleForTesting
  public static class TableHistograms {
    final MetricHistogram getTimeHisto;
    final MetricHistogram incrementTimeHisto;
    final MetricHistogram appendTimeHisto;
    final MetricHistogram putTimeHisto;
    final MetricHistogram putBatchTimeHisto;
    final MetricHistogram deleteTimeHisto;
    final MetricHistogram deleteBatchTimeHisto;
    final MetricHistogram scanTimeHisto;
    final MetricHistogram scanSizeHisto;

    // TODO: this doesn't appear to remove metrics like the other impl
    TableHistograms(DynamicMetricsRegistry registry, TableName tn) {
      getTimeHisto = newTimeHistogram(registry, tn, GET_TIME);
      incrementTimeHisto = newTimeHistogram(registry, tn, INCREMENT_TIME);
      appendTimeHisto = newTimeHistogram(registry, tn, APPEND_TIME);
      putTimeHisto = newTimeHistogram(registry, tn, PUT_TIME);
      putBatchTimeHisto = newTimeHistogram(registry, tn, PUT_BATCH_TIME);
      deleteTimeHisto = newTimeHistogram(registry, tn, DELETE_TIME);
      deleteBatchTimeHisto = newTimeHistogram(registry, tn, DELETE_BATCH_TIME);
      scanTimeHisto = newTimeHistogram(registry, tn, SCAN_TIME);
      scanSizeHisto = newSizeHistogram(registry, tn, SCAN_SIZE);
    }

    protected MutableTimeHistogram newTimeHistogram(
        DynamicMetricsRegistry registry, TableName tn, String name) {
      return registry.newTimeHistogram(qualifyMetricsName(tn, name));
    }

    protected MutableSizeHistogram newSizeHistogram(
        DynamicMetricsRegistry registry, TableName tn, String name) {
      return registry.newSizeHistogram(qualifyMetricsName(tn, name));
    }

    public void updatePut(long time) {
      putTimeHisto.add(time);
    }

    public void updatePutBatch(long time) {
      putBatchTimeHisto.add(time);
    }

    public void updateDelete(long t) {
      deleteTimeHisto.add(t);
    }

    public void updateDeleteBatch(long t) {
      deleteBatchTimeHisto.add(t);
    }

    public void updateGet(long t) {
      getTimeHisto.add(t);
    }

    public void updateIncrement(long t) {
      incrementTimeHisto.add(t);
    }

    public void updateAppend(long t) {
      appendTimeHisto.add(t);
    }

    public void updateScanSize(long scanSize) {
      scanSizeHisto.add(scanSize);
    }

    public void updateScanTime(long t) {
      scanTimeHisto.add(t);
    }
  }

  private static class TableHistogramsWithTags extends TableHistograms {
    TableHistogramsWithTags(DynamicMetricsRegistry registry, TableName tn) {
      super(registry, tn);
    }

    @Override
    protected MutableSizeHistogram newSizeHistogram(
        DynamicMetricsRegistry registry, TableName tn, String name) {
      return registry.newScopedSizeHistogram(tn.getNameAsString(), name, "");
    }

    @Override
    protected MutableTimeHistogram newTimeHistogram(
        DynamicMetricsRegistry registry, TableName tn, String name) {
      return registry.newScopedTimeHistogram(tn.getNameAsString(), name, "");
    }
  }

  @VisibleForTesting
  public static String qualifyMetricsName(TableName tableName, String metric) {
    StringBuilder sb = new StringBuilder();
    sb.append("Namespace_").append(tableName.getNamespaceAsString());
    sb.append("_table_").append(tableName.getQualifierAsString());
    sb.append("_metric_").append(metric);
    return sb.toString();
  }

  @VisibleForTesting
  public TableHistograms getOrCreateTableHistogram(String tableName) {
    // TODO Java8's ConcurrentHashMap#computeIfAbsent would be stellar instead
    final TableName tn = TableName.valueOf(tableName);
    TableHistograms latency = histogramsByTable.get(tn);
    if (latency == null) {
      DynamicMetricsRegistry reg = getMetricsRegistry();
      latency = useTags ? new TableHistogramsWithTags(reg, tn) : new TableHistograms(reg, tn);
      histogramsByTable.put(tn, latency);
    }
    return latency;
  }

  public MetricsTableLatenciesImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsTableLatenciesImpl(String metricsName, String metricsDescription,
      String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  @Override
  public void updatePut(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updatePut(t);
  }

  @Override
  public void updatePutBatch(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updatePutBatch(t);
  }

  @Override
  public void updateDelete(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updateDelete(t);
  }

  @Override
  public void updateDeleteBatch(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updateDeleteBatch(t);
  }

  @Override
  public void updateGet(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updateGet(t);
  }

  @Override
  public void updateIncrement(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updateIncrement(t);
  }

  @Override
  public void updateAppend(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updateAppend(t);
  }

  @Override
  public void updateScanSize(String tableName, long scanSize) {
    getOrCreateTableHistogram(tableName).updateScanSize(scanSize);
  }

  @Override
  public void updateScanTime(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updateScanTime(t);
  }

  public void setConf(Configuration conf) {
    if (this.conf != null) {
      LOG.warn("The object was already initialized with {}", this.useTags);
      return;
    }
    this.conf = conf;
    this.useTags = MetricsTableAggregateSourceImpl.areTablesViaTags(conf);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    if (!useTags) {
      super.getMetrics(metricsCollector, all);
    } else {
      for (TableName tn : histogramsByTable.keySet()) {
        String scope = tn.getNameAsString();
        String recName = metricsRegistry.info().name() + "." + scope;
        MetricsRecordBuilder mrb = metricsCollector.addRecord(recName);
        mrb.add(new MetricsTag(MetricsTableSourceImplWithTags.TABLE_TAG_INFO, scope));
        metricsRegistry.snapshotScoped(scope, mrb, all);
      }
      // There are no metrics here not scoped to the table, so don't snapshot().
    }
  }

  @Override
  public boolean isScoped() {
    return useTags;
  }
}
