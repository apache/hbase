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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.hadoop.metrics2.lib.MutableSizeHistogram;
import org.apache.hadoop.metrics2.lib.MutableTimeHistogram;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MetricsTableSourceImpl implements MetricsTableSource {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsTableSourceImpl.class);


  public static final String RS_ENABLE_TABLE_METRICS_KEY =
      "hbase.regionserver.enable.table.latencies";
  public static final boolean RS_ENABLE_TABLE_METRICS_DEFAULT = true;

  private AtomicBoolean closed = new AtomicBoolean(false);

  // Non-final so that we can null out the wrapper
  // This is just paranoia. We really really don't want to
  // leak a whole table by way of keeping the
  // tableWrapper around too long.
  private MetricsTableWrapperAggregate tableWrapperAgg;
  private final MetricsTableAggregateSourceImpl agg;
  protected final DynamicMetricsRegistry registry;
  private final String tableNamePrefix;
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
    this.tableNamePrefix = "Namespace_" + this.tableName.getNamespaceAsString() +
        "_table_" + this.tableName.getQualifierAsString() + "_metric_";
    this.hashCode = this.tableName.hashCode();
  }


  public static boolean areTableLatenciesEnabled(Configuration conf) {
    return conf.getBoolean(RS_ENABLE_TABLE_METRICS_KEY, RS_ENABLE_TABLE_METRICS_DEFAULT);
  }


  @Override
  public synchronized void registerMetrics() {
    flushTimeHisto = newTimeHisto(FLUSH_TIME, FLUSH_TIME_DESC);
    flushMemstoreSizeHisto = newSizeHisto(FLUSH_MEMSTORE_SIZE, FLUSH_MEMSTORE_SIZE_DESC);
    flushOutputSizeHisto = newSizeHisto(FLUSH_OUTPUT_SIZE, FLUSH_OUTPUT_SIZE_DESC);
    flushedOutputBytes = newCounter(FLUSHED_OUTPUT_BYTES, FLUSHED_OUTPUT_BYTES_DESC);
    flushedMemstoreBytes = newCounter(FLUSHED_MEMSTORE_BYTES, FLUSHED_MEMSTORE_BYTES_DESC);

    compactionTimeHisto = newTimeHisto(COMPACTION_TIME, COMPACTION_TIME_DESC);
    compactionInputFileCountHisto = newHistogram(COMPACTION_INPUT_FILE_COUNT, COMPACTION_INPUT_FILE_COUNT_DESC);
    compactionInputSizeHisto = newSizeHisto(COMPACTION_INPUT_SIZE, COMPACTION_INPUT_SIZE_DESC);
    compactionOutputFileCountHisto = newHistogram(COMPACTION_OUTPUT_FILE_COUNT, COMPACTION_OUTPUT_FILE_COUNT_DESC);
    compactionOutputSizeHisto = newSizeHisto(COMPACTION_OUTPUT_SIZE, COMPACTION_OUTPUT_SIZE_DESC);
    compactedInputBytes = newCounter(COMPACTED_INPUT_BYTES, COMPACTED_INPUT_BYTES_DESC);
    compactedOutputBytes = newCounter(COMPACTED_OUTPUT_BYTES, COMPACTED_OUTPUT_BYTES_DESC);

    majorCompactionTimeHisto = newTimeHisto(MAJOR_COMPACTION_TIME, MAJOR_COMPACTION_TIME_DESC);
    majorCompactionInputFileCountHisto = newHistogram(MAJOR_COMPACTION_INPUT_FILE_COUNT, MAJOR_COMPACTION_INPUT_FILE_COUNT_DESC);
    majorCompactionInputSizeHisto = newSizeHisto(MAJOR_COMPACTION_INPUT_SIZE, MAJOR_COMPACTION_INPUT_SIZE_DESC);
    majorCompactionOutputFileCountHisto =
        newHistogram(MAJOR_COMPACTION_OUTPUT_FILE_COUNT, MAJOR_COMPACTION_OUTPUT_FILE_COUNT_DESC);
    majorCompactionOutputSizeHisto = newSizeHisto(MAJOR_COMPACTION_OUTPUT_SIZE, MAJOR_COMPACTION_OUTPUT_SIZE_DESC);
    majorCompactedInputBytes = newCounter(MAJOR_COMPACTED_INPUT_BYTES, MAJOR_COMPACTED_INPUT_BYTES_DESC);
    majorCompactedOutputBytes = newCounter(MAJOR_COMPACTED_OUTPUT_BYTES, MAJOR_COMPACTED_OUTPUT_BYTES_DESC);

    splitTimeHisto = newTimeHisto(tableNamePrefix + SPLIT_KEY, "");
    splitRequest = newCounter(SPLIT_REQUEST_KEY, SPLIT_REQUEST_DESC);
    splitSuccess = newCounter(SPLIT_SUCCESS_KEY, SPLIT_SUCCESS_DESC);
  }

  protected MutableHistogram newHistogram(String name, String desc) {
    return registry.newHistogram(tableNamePrefix + name, desc);
  }

  protected MutableFastCounter newCounter(String name, String desc) {
    return registry.newCounter(tableNamePrefix + name, desc, 0L);
  }

  protected MutableSizeHistogram newSizeHisto(String name, String desc) {
    return registry.newSizeHistogram(tableNamePrefix + name, desc);
  }

  protected MutableTimeHistogram newTimeHisto(String name, String desc) {
    return registry.newTimeHistogram(tableNamePrefix + name, desc);
  }

  protected void removeNonHistogram(String name) {
    registry.removeMetric(tableNamePrefix + name);
  }

  protected void removeHistogram(String name) {
    registry.removeHistogramMetrics(tableNamePrefix + name);
  }

  protected MetricsInfo createMetricsInfo(String name, String desc) {
    return Interns.info(tableNamePrefix + name, desc);
  }

  private void deregisterMetrics() {
    removeHistogram(FLUSH_TIME);
    removeHistogram(FLUSH_MEMSTORE_SIZE);
    removeHistogram(FLUSH_OUTPUT_SIZE);
    removeNonHistogram(FLUSHED_OUTPUT_BYTES);
    removeNonHistogram(FLUSHED_MEMSTORE_BYTES);
    removeHistogram(COMPACTION_TIME);
    removeHistogram(COMPACTION_INPUT_FILE_COUNT);
    removeHistogram(COMPACTION_INPUT_SIZE);
    removeHistogram(COMPACTION_OUTPUT_FILE_COUNT);
    removeHistogram(COMPACTION_OUTPUT_SIZE);
    removeNonHistogram(COMPACTED_INPUT_BYTES);
    removeNonHistogram(COMPACTED_OUTPUT_BYTES);
    removeHistogram(MAJOR_COMPACTION_TIME);
    removeHistogram(MAJOR_COMPACTION_INPUT_FILE_COUNT);
    removeHistogram(MAJOR_COMPACTION_INPUT_SIZE);
    removeHistogram(MAJOR_COMPACTION_OUTPUT_FILE_COUNT);
    removeHistogram(MAJOR_COMPACTION_OUTPUT_SIZE);
    removeNonHistogram(MAJOR_COMPACTED_INPUT_BYTES);
    removeNonHistogram(MAJOR_COMPACTED_OUTPUT_BYTES);
    removeHistogram(SPLIT_KEY);
    removeNonHistogram(SPLIT_REQUEST_KEY);
    removeNonHistogram(SPLIT_SUCCESS_KEY);
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
      // Make sure that these metrics are skipped in MetricsRegionServerSourceImpl;
      // see skipTableMetrics and the comment there.
      if (this.tableWrapperAgg != null) {
        mrb.addCounter(createMetricsInfo(MetricsRegionServerSource.CP_REQUEST_COUNT,
            MetricsRegionServerSource.CP_REQUEST_COUNT_DESC),
          tableWrapperAgg.getCpRequestsCount(tableName.getNameAsString()));
        mrb.addCounter(createMetricsInfo(MetricsRegionServerSource.READ_REQUEST_COUNT,
            MetricsRegionServerSource.READ_REQUEST_COUNT_DESC),
          tableWrapperAgg.getReadRequestCount(tableName.getNameAsString()));
        mrb.addCounter(createMetricsInfo(MetricsRegionServerSource.FILTERED_READ_REQUEST_COUNT,
            MetricsRegionServerSource.FILTERED_READ_REQUEST_COUNT_DESC),
          tableWrapperAgg.getFilteredReadRequestCount(tableName.getNameAsString()));
        mrb.addCounter(createMetricsInfo(MetricsRegionServerSource.WRITE_REQUEST_COUNT,
            MetricsRegionServerSource.WRITE_REQUEST_COUNT_DESC),
            tableWrapperAgg.getWriteRequestCount(tableName.getNameAsString()));
        mrb.addCounter(createMetricsInfo(MetricsRegionServerSource.TOTAL_REQUEST_COUNT,
            MetricsRegionServerSource.TOTAL_REQUEST_COUNT_DESC),
            tableWrapperAgg.getTotalRequestsCount(tableName.getNameAsString()));
        mrb.addGauge(createMetricsInfo(MetricsRegionServerSource.MEMSTORE_SIZE,
            MetricsRegionServerSource.MEMSTORE_SIZE_DESC),
            tableWrapperAgg.getMemStoreSize(tableName.getNameAsString()));
        mrb.addGauge(createMetricsInfo(MetricsRegionServerSource.STOREFILE_COUNT,
            MetricsRegionServerSource.STOREFILE_COUNT_DESC),
            tableWrapperAgg.getNumStoreFiles(tableName.getNameAsString()));
        mrb.addGauge(createMetricsInfo(MetricsRegionServerSource.STOREFILE_SIZE,
            MetricsRegionServerSource.STOREFILE_SIZE_DESC),
            tableWrapperAgg.getStoreFileSize(tableName.getNameAsString()));
        mrb.addGauge(createMetricsInfo(MetricsTableSource.TABLE_SIZE,
            MetricsTableSource.TABLE_SIZE_DESC),
          tableWrapperAgg.getTableSize(tableName.getNameAsString()));
        mrb.addGauge(createMetricsInfo(MetricsRegionServerSource.AVERAGE_REGION_SIZE,
            MetricsRegionServerSource.AVERAGE_REGION_SIZE_DESC),
            tableWrapperAgg.getAvgRegionSize(tableName.getNameAsString()));
        mrb.addGauge(createMetricsInfo(MetricsRegionServerSource.REGION_COUNT,
            MetricsRegionServerSource.REGION_COUNT_DESC),
            tableWrapperAgg.getNumRegions(tableName.getNameAsString()));
        mrb.addGauge(createMetricsInfo(MetricsRegionServerSource.STORE_COUNT,
            MetricsRegionServerSource.STORE_COUNT_DESC),
            tableWrapperAgg.getNumStores(tableName.getNameAsString()));
        mrb.addGauge(createMetricsInfo(MetricsRegionServerSource.MAX_STORE_FILE_AGE,
            MetricsRegionServerSource.MAX_STORE_FILE_AGE_DESC),
            tableWrapperAgg.getMaxStoreFileAge(tableName.getNameAsString()));
        mrb.addGauge(createMetricsInfo(MetricsRegionServerSource.MIN_STORE_FILE_AGE,
            MetricsRegionServerSource.MIN_STORE_FILE_AGE_DESC),
            tableWrapperAgg.getMinStoreFileAge(tableName.getNameAsString()));
        mrb.addGauge(createMetricsInfo(MetricsRegionServerSource.AVG_STORE_FILE_AGE,
            MetricsRegionServerSource.AVG_STORE_FILE_AGE_DESC),
            tableWrapperAgg.getAvgStoreFileAge(tableName.getNameAsString()));
        mrb.addGauge(createMetricsInfo(MetricsRegionServerSource.NUM_REFERENCE_FILES,
            MetricsRegionServerSource.NUM_REFERENCE_FILES_DESC),
            tableWrapperAgg.getNumReferenceFiles(tableName.getNameAsString()));

        addTags(mrb);
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

  public String getScope() {
    return null;
  }

  protected void addTags(MetricsRecordBuilder mrb) {
  }
}
