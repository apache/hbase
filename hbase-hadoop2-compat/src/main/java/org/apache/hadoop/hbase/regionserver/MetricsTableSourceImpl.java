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

import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.COMPACTED_INPUT_BYTES;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.COMPACTED_INPUT_BYTES_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.COMPACTED_OUTPUT_BYTES;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.COMPACTED_OUTPUT_BYTES_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.COMPACTION_INPUT_FILE_COUNT;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.COMPACTION_INPUT_FILE_COUNT_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.COMPACTION_INPUT_SIZE;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.COMPACTION_INPUT_SIZE_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.COMPACTION_OUTPUT_FILE_COUNT;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.COMPACTION_OUTPUT_FILE_COUNT_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.COMPACTION_OUTPUT_SIZE;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.COMPACTION_OUTPUT_SIZE_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.COMPACTION_TIME;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.COMPACTION_TIME_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.FLUSHED_MEMSTORE_BYTES;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.FLUSHED_MEMSTORE_BYTES_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.FLUSHED_OUTPUT_BYTES;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.FLUSHED_OUTPUT_BYTES_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.FLUSH_MEMSTORE_SIZE;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.FLUSH_MEMSTORE_SIZE_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.FLUSH_OUTPUT_SIZE;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.FLUSH_OUTPUT_SIZE_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.FLUSH_TIME;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.FLUSH_TIME_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.MAJOR_COMPACTED_INPUT_BYTES;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.MAJOR_COMPACTED_INPUT_BYTES_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.MAJOR_COMPACTED_OUTPUT_BYTES;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.MAJOR_COMPACTED_OUTPUT_BYTES_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.MAJOR_COMPACTION_INPUT_FILE_COUNT;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.MAJOR_COMPACTION_INPUT_FILE_COUNT_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.MAJOR_COMPACTION_INPUT_SIZE;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.MAJOR_COMPACTION_INPUT_SIZE_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.MAJOR_COMPACTION_OUTPUT_FILE_COUNT;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.MAJOR_COMPACTION_OUTPUT_FILE_COUNT_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.MAJOR_COMPACTION_OUTPUT_SIZE;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.MAJOR_COMPACTION_OUTPUT_SIZE_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.MAJOR_COMPACTION_TIME;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.MAJOR_COMPACTION_TIME_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.SPLIT_KEY;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.SPLIT_REQUEST_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.SPLIT_REQUEST_KEY;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.SPLIT_SUCCESS_DESC;
import static org.apache.hadoop.hbase.regionserver.MetricsRegionServerSource.SPLIT_SUCCESS_KEY;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MetricsTableSourceImpl implements MetricsTableSource {

  private static final String _COLUMNFAMILY = "_columnfamily_";

  private static final Logger LOG = LoggerFactory.getLogger(MetricsTableSourceImpl.class);

  private AtomicBoolean closed = new AtomicBoolean(false);

  // Non-final so that we can null out the wrapper
  // This is just paranoia. We really really don't want to
  // leak a whole table by way of keeping the
  // tableWrapper around too long.
  private MetricsTableWrapperAggregate tableWrapperAgg;
  private final MetricsTableAggregateSourceImpl agg;
  private final DynamicMetricsRegistry registry;
  private final String tableNamePrefix;
  private final String tableNamePrefixPart1;
  private final String tableNamePrefixPart2;
  private final TableName tableName;
  private final int hashCode;

  // split related metrics
  private MutableFastCounter splitRequest;
  private MutableFastCounter splitSuccess;
  private MetricHistogram splitTimeHisto;

  // flush related metrics
  private MetricHistogram flushTimeHisto;
  private MetricHistogram flushMemstoreSizeHisto;
  private MetricHistogram flushOutputSizeHisto;
  private MutableFastCounter flushedMemstoreBytes;
  private MutableFastCounter flushedOutputBytes;

  // compaction related metrics
  private MetricHistogram compactionTimeHisto;
  private MetricHistogram compactionInputFileCountHisto;
  private MetricHistogram compactionInputSizeHisto;
  private MetricHistogram compactionOutputFileCountHisto;
  private MetricHistogram compactionOutputSizeHisto;
  private MutableFastCounter compactedInputBytes;
  private MutableFastCounter compactedOutputBytes;

  private MetricHistogram majorCompactionTimeHisto;
  private MetricHistogram majorCompactionInputFileCountHisto;
  private MetricHistogram majorCompactionInputSizeHisto;
  private MetricHistogram majorCompactionOutputFileCountHisto;
  private MetricHistogram majorCompactionOutputSizeHisto;
  private MutableFastCounter majorCompactedInputBytes;
  private MutableFastCounter majorCompactedOutputBytes;

  public MetricsTableSourceImpl(String tblName,
      MetricsTableAggregateSourceImpl aggregate, MetricsTableWrapperAggregate tblWrapperAgg) {
    LOG.debug("Creating new MetricsTableSourceImpl for table '{}'", tblName);
    this.tableName = TableName.valueOf(tblName);
    this.agg = aggregate;

    this.tableWrapperAgg = tblWrapperAgg;
    this.registry = agg.getMetricsRegistry();
    this.tableNamePrefixPart1 = "Namespace_" + this.tableName.getNamespaceAsString() +
            "_table_" + this.tableName.getQualifierAsString();
    this.tableNamePrefixPart2 = "_metric_";
    this.tableNamePrefix = tableNamePrefixPart1 +
        tableNamePrefixPart2;
    this.hashCode = this.tableName.hashCode();
  }

  @Override
  public synchronized void registerMetrics() {
    flushTimeHisto = registry.newTimeHistogram(tableNamePrefix + FLUSH_TIME, FLUSH_TIME_DESC);
    flushMemstoreSizeHisto =
        registry.newSizeHistogram(tableNamePrefix + FLUSH_MEMSTORE_SIZE, FLUSH_MEMSTORE_SIZE_DESC);
    flushOutputSizeHisto =
        registry.newSizeHistogram(tableNamePrefix + FLUSH_OUTPUT_SIZE, FLUSH_OUTPUT_SIZE_DESC);
    flushedOutputBytes =
        registry.newCounter(tableNamePrefix + FLUSHED_OUTPUT_BYTES, FLUSHED_OUTPUT_BYTES_DESC, 0L);
    flushedMemstoreBytes = registry.newCounter(tableNamePrefix + FLUSHED_MEMSTORE_BYTES,
      FLUSHED_MEMSTORE_BYTES_DESC, 0L);

    compactionTimeHisto =
        registry.newTimeHistogram(tableNamePrefix + COMPACTION_TIME, COMPACTION_TIME_DESC);
    compactionInputFileCountHisto = registry.newHistogram(
      tableNamePrefix + COMPACTION_INPUT_FILE_COUNT, COMPACTION_INPUT_FILE_COUNT_DESC);
    compactionInputSizeHisto = registry.newSizeHistogram(tableNamePrefix + COMPACTION_INPUT_SIZE,
      COMPACTION_INPUT_SIZE_DESC);
    compactionOutputFileCountHisto = registry.newHistogram(
      tableNamePrefix + COMPACTION_OUTPUT_FILE_COUNT, COMPACTION_OUTPUT_FILE_COUNT_DESC);
    compactionOutputSizeHisto = registry.newSizeHistogram(tableNamePrefix + COMPACTION_OUTPUT_SIZE,
      COMPACTION_OUTPUT_SIZE_DESC);
    compactedInputBytes = registry.newCounter(tableNamePrefix + COMPACTED_INPUT_BYTES,
      COMPACTED_INPUT_BYTES_DESC, 0L);
    compactedOutputBytes = registry.newCounter(tableNamePrefix + COMPACTED_OUTPUT_BYTES,
      COMPACTED_OUTPUT_BYTES_DESC, 0L);

    majorCompactionTimeHisto = registry.newTimeHistogram(tableNamePrefix + MAJOR_COMPACTION_TIME,
      MAJOR_COMPACTION_TIME_DESC);
    majorCompactionInputFileCountHisto = registry.newHistogram(
      tableNamePrefix + MAJOR_COMPACTION_INPUT_FILE_COUNT, MAJOR_COMPACTION_INPUT_FILE_COUNT_DESC);
    majorCompactionInputSizeHisto = registry.newSizeHistogram(
      tableNamePrefix + MAJOR_COMPACTION_INPUT_SIZE, MAJOR_COMPACTION_INPUT_SIZE_DESC);
    majorCompactionOutputFileCountHisto =
        registry.newHistogram(tableNamePrefix + MAJOR_COMPACTION_OUTPUT_FILE_COUNT,
          MAJOR_COMPACTION_OUTPUT_FILE_COUNT_DESC);
    majorCompactionOutputSizeHisto = registry.newSizeHistogram(
      tableNamePrefix + MAJOR_COMPACTION_OUTPUT_SIZE, MAJOR_COMPACTION_OUTPUT_SIZE_DESC);
    majorCompactedInputBytes = registry.newCounter(tableNamePrefix + MAJOR_COMPACTED_INPUT_BYTES,
      MAJOR_COMPACTED_INPUT_BYTES_DESC, 0L);
    majorCompactedOutputBytes = registry.newCounter(tableNamePrefix + MAJOR_COMPACTED_OUTPUT_BYTES,
      MAJOR_COMPACTED_OUTPUT_BYTES_DESC, 0L);

    splitTimeHisto = registry.newTimeHistogram(tableNamePrefix + SPLIT_KEY);
    splitRequest = registry.newCounter(tableNamePrefix + SPLIT_REQUEST_KEY, SPLIT_REQUEST_DESC, 0L);
    splitSuccess = registry.newCounter(tableNamePrefix + SPLIT_SUCCESS_KEY, SPLIT_SUCCESS_DESC, 0L);
  }

  private void deregisterMetrics() {
    registry.removeHistogramMetrics(tableNamePrefix + FLUSH_TIME);
    registry.removeHistogramMetrics(tableNamePrefix + FLUSH_MEMSTORE_SIZE);
    registry.removeHistogramMetrics(tableNamePrefix + FLUSH_OUTPUT_SIZE);
    registry.removeMetric(tableNamePrefix + FLUSHED_OUTPUT_BYTES);
    registry.removeMetric(tableNamePrefix + FLUSHED_MEMSTORE_BYTES);
    registry.removeHistogramMetrics(tableNamePrefix + COMPACTION_TIME);
    registry.removeHistogramMetrics(tableNamePrefix + COMPACTION_INPUT_FILE_COUNT);
    registry.removeHistogramMetrics(tableNamePrefix + COMPACTION_INPUT_SIZE);
    registry.removeHistogramMetrics(tableNamePrefix + COMPACTION_OUTPUT_FILE_COUNT);
    registry.removeHistogramMetrics(tableNamePrefix + COMPACTION_OUTPUT_SIZE);
    registry.removeMetric(tableNamePrefix + COMPACTED_INPUT_BYTES);
    registry.removeMetric(tableNamePrefix + COMPACTED_OUTPUT_BYTES);
    registry.removeHistogramMetrics(tableNamePrefix + MAJOR_COMPACTION_TIME);
    registry.removeHistogramMetrics(tableNamePrefix + MAJOR_COMPACTION_INPUT_FILE_COUNT);
    registry.removeHistogramMetrics(tableNamePrefix + MAJOR_COMPACTION_INPUT_SIZE);
    registry.removeHistogramMetrics(tableNamePrefix + MAJOR_COMPACTION_OUTPUT_FILE_COUNT);
    registry.removeHistogramMetrics(tableNamePrefix + MAJOR_COMPACTION_OUTPUT_SIZE);
    registry.removeMetric(tableNamePrefix + MAJOR_COMPACTED_INPUT_BYTES);
    registry.removeMetric(tableNamePrefix + MAJOR_COMPACTED_OUTPUT_BYTES);
    registry.removeHistogramMetrics(tableNamePrefix + SPLIT_KEY);
    registry.removeMetric(tableNamePrefix + SPLIT_REQUEST_KEY);
    registry.removeMetric(tableNamePrefix + SPLIT_SUCCESS_KEY);
  }

  @Override
  public void close() {
    boolean wasClosed = closed.getAndSet(true);

    // Has someone else already closed this for us?
    if (wasClosed) {
      return;
    }

    // Before removing the metrics remove this table from the aggregate table bean.
    // This should mean that it's unlikely that snapshot and close happen at the same time.
    agg.deleteTableSource(tableName.getNameAsString());

    // While it's un-likely that snapshot and close happen at the same time it's still possible.
    // So grab the lock to ensure that all calls to snapshot are done before we remove the metrics
    synchronized (this) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Removing table Metrics for table ");
      }
      deregisterMetrics();
      tableWrapperAgg = null;
    }
  }
  @Override
  public MetricsTableAggregateSource getAggregateSource() {
    return agg;
  }

  @Override
  public int compareTo(MetricsTableSource source) {
    if (!(source instanceof MetricsTableSourceImpl)) {
      return -1;
    }
    MetricsTableSourceImpl impl = (MetricsTableSourceImpl) source;
    return Long.compare(hashCode, impl.hashCode);
  }

  void snapshot(MetricsRecordBuilder mrb, boolean ignored) {
    // If there is a close that started be double extra sure
    // that we're not getting any locks and not putting data
    // into the metrics that should be removed. So early out
    // before even getting the lock.
    if (closed.get()) {
      return;
    }

    // Grab the read
    // This ensures that removes of the metrics
    // can't happen while we are putting them back in.
    synchronized (this) {
      // It's possible that a close happened between checking
      // the closed variable and getting the lock.
      if (closed.get()) {
        return;
      }

      if (this.tableWrapperAgg != null) {
        mrb.addCounter(Interns.info(tableNamePrefix + MetricsRegionServerSource.READ_REQUEST_COUNT,
            MetricsRegionServerSource.READ_REQUEST_COUNT_DESC),
            tableWrapperAgg.getReadRequestCount(tableName.getNameAsString()));
        mrb.addCounter(
            Interns.info(tableNamePrefix + MetricsRegionServerSource.FILTERED_READ_REQUEST_COUNT,
                MetricsRegionServerSource.FILTERED_READ_REQUEST_COUNT_DESC),
            tableWrapperAgg.getFilteredReadRequestCount(tableName.getNameAsString()));
        mrb.addCounter(Interns.info(tableNamePrefix + MetricsRegionServerSource.WRITE_REQUEST_COUNT,
            MetricsRegionServerSource.WRITE_REQUEST_COUNT_DESC),
            tableWrapperAgg.getWriteRequestCount(tableName.getNameAsString()));
        mrb.addCounter(Interns.info(tableNamePrefix + MetricsRegionServerSource.TOTAL_REQUEST_COUNT,
            MetricsRegionServerSource.TOTAL_REQUEST_COUNT_DESC),
            tableWrapperAgg.getTotalRequestsCount(tableName.getNameAsString()));
        mrb.addGauge(Interns.info(tableNamePrefix + MetricsRegionServerSource.MEMSTORE_SIZE,
            MetricsRegionServerSource.MEMSTORE_SIZE_DESC),
            tableWrapperAgg.getMemStoreSize(tableName.getNameAsString()));
        mrb.addGauge(Interns.info(tableNamePrefix + MetricsRegionServerSource.STOREFILE_COUNT,
            MetricsRegionServerSource.STOREFILE_COUNT_DESC),
            tableWrapperAgg.getNumStoreFiles(tableName.getNameAsString()));
        mrb.addGauge(Interns.info(tableNamePrefix + MetricsRegionServerSource.STOREFILE_SIZE,
            MetricsRegionServerSource.STOREFILE_SIZE_DESC),
            tableWrapperAgg.getStoreFileSize(tableName.getNameAsString()));
        mrb.addGauge(Interns.info(tableNamePrefix + MetricsTableSource.TABLE_SIZE,
          MetricsTableSource.TABLE_SIZE_DESC),
          tableWrapperAgg.getTableSize(tableName.getNameAsString()));
        mrb.addGauge(Interns.info(tableNamePrefix + MetricsRegionServerSource.AVERAGE_REGION_SIZE,
            MetricsRegionServerSource.AVERAGE_REGION_SIZE_DESC),
            tableWrapperAgg.getAvgRegionSize(tableName.getNameAsString()));
        mrb.addGauge(Interns.info(tableNamePrefix + MetricsRegionServerSource.REGION_COUNT,
            MetricsRegionServerSource.REGION_COUNT_DESC),
            tableWrapperAgg.getNumRegions(tableName.getNameAsString()));
        mrb.addGauge(Interns.info(tableNamePrefix + MetricsRegionServerSource.STORE_COUNT,
            MetricsRegionServerSource.STORE_COUNT_DESC),
            tableWrapperAgg.getNumStores(tableName.getNameAsString()));
        mrb.addGauge(Interns.info(tableNamePrefix + MetricsRegionServerSource.MAX_STORE_FILE_AGE,
            MetricsRegionServerSource.MAX_STORE_FILE_AGE_DESC),
            tableWrapperAgg.getMaxStoreFileAge(tableName.getNameAsString()));
        mrb.addGauge(Interns.info(tableNamePrefix + MetricsRegionServerSource.MIN_STORE_FILE_AGE,
            MetricsRegionServerSource.MIN_STORE_FILE_AGE_DESC),
            tableWrapperAgg.getMinStoreFileAge(tableName.getNameAsString()));
        mrb.addGauge(Interns.info(tableNamePrefix + MetricsRegionServerSource.AVG_STORE_FILE_AGE,
            MetricsRegionServerSource.AVG_STORE_FILE_AGE_DESC),
            tableWrapperAgg.getAvgStoreFileAge(tableName.getNameAsString()));
        mrb.addGauge(Interns.info(tableNamePrefix + MetricsRegionServerSource.NUM_REFERENCE_FILES,
            MetricsRegionServerSource.NUM_REFERENCE_FILES_DESC),
            tableWrapperAgg.getNumReferenceFiles(tableName.getNameAsString()));
        addGauge(mrb, tableWrapperAgg.getMemstoreOnlyRowReadsCount(tableName.getNameAsString()),
          MetricsRegionSource.ROW_READS_ONLY_ON_MEMSTORE,
          MetricsRegionSource.ROW_READS_ONLY_ON_MEMSTORE_DESC);
        addGauge(mrb, tableWrapperAgg.getMixedRowReadsCount(tableName.getNameAsString()),
          MetricsRegionSource.MIXED_ROW_READS,
          MetricsRegionSource.MIXED_ROW_READS_ON_STORE_DESC);
      }
    }
  }

  private void addGauge(MetricsRecordBuilder mrb, Map<String, Long> metricMap, String metricName,
      String metricDesc) {
    if (metricMap != null) {
      for (Entry<String, Long> entry : metricMap.entrySet()) {
        // append 'store' and its name to the metric
        mrb.addGauge(Interns.info(this.tableNamePrefixPart1 + _COLUMNFAMILY
            + entry.getKey().split(MetricsTableWrapperAggregate.HASH)[1]
            + this.tableNamePrefixPart2 + metricName,
          metricDesc), entry.getValue());
      }
    }
  }

  @Override
  public String getTableName() {
    return tableName.getNameAsString();
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return (compareTo((MetricsTableSourceImpl) o) == 0);
  }

  public MetricsTableWrapperAggregate getTableWrapper() {
    return tableWrapperAgg;
  }

  public String getTableNamePrefix() {
    return tableNamePrefix;
  }

  @Override
  public void incrSplitRequest() {
    splitRequest.incr();
  }

  @Override
  public void incrSplitSuccess() {
    splitSuccess.incr();
  }

  @Override
  public void updateSplitTime(long t) {
    splitTimeHisto.add(t);
  }

  @Override
  public void updateFlushTime(long t) {
    flushTimeHisto.add(t);
  }

  @Override
  public synchronized void updateFlushMemstoreSize(long bytes) {
    flushMemstoreSizeHisto.add(bytes);
    flushedMemstoreBytes.incr(bytes);
  }

  @Override
  public synchronized void updateFlushOutputSize(long bytes) {
    flushOutputSizeHisto.add(bytes);
    flushedOutputBytes.incr(bytes);
  }

  @Override
  public synchronized void updateCompactionTime(boolean isMajor, long t) {
    compactionTimeHisto.add(t);
    if (isMajor) {
      majorCompactionTimeHisto.add(t);
    }
  }

  @Override
  public synchronized void updateCompactionInputFileCount(boolean isMajor, long c) {
    compactionInputFileCountHisto.add(c);
    if (isMajor) {
      majorCompactionInputFileCountHisto.add(c);
    }
  }

  @Override
  public synchronized void updateCompactionInputSize(boolean isMajor, long bytes) {
    compactionInputSizeHisto.add(bytes);
    compactedInputBytes.incr(bytes);
    if (isMajor) {
      majorCompactionInputSizeHisto.add(bytes);
      majorCompactedInputBytes.incr(bytes);
    }
  }

  @Override
  public synchronized void updateCompactionOutputFileCount(boolean isMajor, long c) {
    compactionOutputFileCountHisto.add(c);
    if (isMajor) {
      majorCompactionOutputFileCountHisto.add(c);
    }
  }

  @Override
  public synchronized void updateCompactionOutputSize(boolean isMajor, long bytes) {
    compactionOutputSizeHisto.add(bytes);
    compactedOutputBytes.incr(bytes);
    if (isMajor) {
      majorCompactionOutputSizeHisto.add(bytes);
      majorCompactedOutputBytes.incr(bytes);
    }
  }
}
