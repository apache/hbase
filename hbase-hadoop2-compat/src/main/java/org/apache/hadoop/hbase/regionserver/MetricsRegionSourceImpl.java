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

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MetricsRegionSourceImpl implements MetricsRegionSource {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsRegionSourceImpl.class);

  private static final String _STORE = "_store_";

  private AtomicBoolean closed = new AtomicBoolean(false);

  // Non-final so that we can null out the wrapper
  // This is just paranoia. We really really don't want to
  // leak a whole region by way of keeping the
  // regionWrapper around too long.
  private MetricsRegionWrapper regionWrapper;

  private final MetricsRegionAggregateSourceImpl agg;
  private final DynamicMetricsRegistry registry;

  private final String regionNamePrefix;
  private final String regionNamePrefix1;
  private final String regionNamePrefix2;
  private final String regionPutKey;
  private final String regionDeleteKey;
  private final String regionGetKey;
  private final String regionIncrementKey;
  private final String regionAppendKey;
  private final String regionScanKey;

  /*
   * Implementation note: Do not put histograms per region. With hundreds of regions in a server
   * histograms allocate too many counters. See HBASE-17016.
   */
  private final MutableFastCounter regionPut;
  private final MutableFastCounter regionDelete;
  private final MutableFastCounter regionIncrement;
  private final MutableFastCounter regionAppend;
  private final MutableFastCounter regionGet;
  private final MutableFastCounter regionScan;

  private final int hashCode;

  public MetricsRegionSourceImpl(MetricsRegionWrapper regionWrapper,
                                 MetricsRegionAggregateSourceImpl aggregate) {
    this.regionWrapper = regionWrapper;
    agg = aggregate;
    hashCode = regionWrapper.getRegionHashCode();
    agg.register(this);

    LOG.debug("Creating new MetricsRegionSourceImpl for table " +
        regionWrapper.getTableName() + " " + regionWrapper.getRegionName());

    registry = agg.getMetricsRegistry();

    regionNamePrefix1 = "Namespace_" + regionWrapper.getNamespace() + "_table_"
        + regionWrapper.getTableName() + "_region_" + regionWrapper.getRegionName();
    regionNamePrefix2 = "_metric_";
    regionNamePrefix = regionNamePrefix1 + regionNamePrefix2;

    String suffix = "Count";

    regionPutKey = regionNamePrefix + MetricsRegionServerSource.PUT_KEY + suffix;
    regionPut = registry.getCounter(regionPutKey, 0L);

    regionDeleteKey = regionNamePrefix + MetricsRegionServerSource.DELETE_KEY + suffix;
    regionDelete = registry.getCounter(regionDeleteKey, 0L);

    regionIncrementKey = regionNamePrefix + MetricsRegionServerSource.INCREMENT_KEY + suffix;
    regionIncrement = registry.getCounter(regionIncrementKey, 0L);

    regionAppendKey = regionNamePrefix + MetricsRegionServerSource.APPEND_KEY + suffix;
    regionAppend = registry.getCounter(regionAppendKey, 0L);

    regionGetKey = regionNamePrefix + MetricsRegionServerSource.GET_KEY + suffix;
    regionGet = registry.getCounter(regionGetKey, 0L);

    regionScanKey = regionNamePrefix + MetricsRegionServerSource.SCAN_KEY + suffix;
    regionScan = registry.getCounter(regionScanKey, 0L);
  }

  @Override
  public void close() {
    boolean wasClosed = closed.getAndSet(true);

    // Has someone else already closed this for us?
    if (wasClosed) {
      return;
    }

    // Before removing the metrics remove this region from the aggregate region bean.
    // This should mean that it's unlikely that snapshot and close happen at the same time.
    agg.deregister(this);

    // While it's un-likely that snapshot and close happen at the same time it's still possible.
    // So grab the lock to ensure that all calls to snapshot are done before we remove the metrics
    synchronized (this) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Removing region Metrics: " + regionWrapper.getRegionName());
      }

      registry.removeMetric(regionPutKey);
      registry.removeMetric(regionDeleteKey);
      registry.removeMetric(regionIncrementKey);
      registry.removeMetric(regionAppendKey);
      registry.removeMetric(regionGetKey);
      registry.removeMetric(regionScanKey);

      regionWrapper = null;
    }
  }

  @Override
  public void updatePut() {
    regionPut.incr();
  }

  @Override
  public void updateDelete() {
    regionDelete.incr();
  }

  @Override
  public void updateGet(long mills) {
    regionGet.incr();
  }

  @Override
  public void updateScanTime(long mills) {
    regionScan.incr();
  }

  @Override
  public void updateIncrement() {
    regionIncrement.incr();
  }

  @Override
  public void updateAppend() {
    regionAppend.incr();
  }

  @Override
  public MetricsRegionAggregateSource getAggregateSource() {
    return agg;
  }

  @Override
  public int compareTo(MetricsRegionSource source) {
    if (!(source instanceof MetricsRegionSourceImpl)) {
      return -1;
    }
    return Long.compare(hashCode, ((MetricsRegionSourceImpl) source).hashCode);
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

      mrb.addGauge(
          Interns.info(
              regionNamePrefix + MetricsRegionServerSource.STORE_COUNT,
              MetricsRegionServerSource.STORE_COUNT_DESC),
          this.regionWrapper.getNumStores());
      mrb.addGauge(Interns.info(
              regionNamePrefix + MetricsRegionServerSource.STOREFILE_COUNT,
              MetricsRegionServerSource.STOREFILE_COUNT_DESC),
          this.regionWrapper.getNumStoreFiles());
      mrb.addGauge(Interns.info(
              regionNamePrefix + MetricsRegionServerSource.STORE_REF_COUNT,
              MetricsRegionServerSource.STORE_REF_COUNT),
          this.regionWrapper.getStoreRefCount());
      mrb.addGauge(Interns.info(
        regionNamePrefix + MetricsRegionServerSource.MAX_COMPACTED_STORE_FILE_REF_COUNT,
        MetricsRegionServerSource.MAX_COMPACTED_STORE_FILE_REF_COUNT),
        this.regionWrapper.getMaxCompactedStoreFileRefCount()
      );
      mrb.addGauge(Interns.info(
              regionNamePrefix + MetricsRegionServerSource.MEMSTORE_SIZE,
              MetricsRegionServerSource.MEMSTORE_SIZE_DESC),
          this.regionWrapper.getMemStoreSize());
      mrb.addGauge(Interns.info(
        regionNamePrefix + MetricsRegionServerSource.MAX_STORE_FILE_AGE,
        MetricsRegionServerSource.MAX_STORE_FILE_AGE_DESC),
        this.regionWrapper.getMaxStoreFileAge());
      mrb.addGauge(Interns.info(
        regionNamePrefix + MetricsRegionServerSource.MIN_STORE_FILE_AGE,
        MetricsRegionServerSource.MIN_STORE_FILE_AGE_DESC),
        this.regionWrapper.getMinStoreFileAge());
      mrb.addGauge(Interns.info(
        regionNamePrefix + MetricsRegionServerSource.AVG_STORE_FILE_AGE,
        MetricsRegionServerSource.AVG_STORE_FILE_AGE_DESC),
        this.regionWrapper.getAvgStoreFileAge());
      mrb.addGauge(Interns.info(
        regionNamePrefix + MetricsRegionServerSource.NUM_REFERENCE_FILES,
        MetricsRegionServerSource.NUM_REFERENCE_FILES_DESC),
        this.regionWrapper.getNumReferenceFiles());
      mrb.addGauge(Interns.info(
              regionNamePrefix + MetricsRegionServerSource.STOREFILE_SIZE,
              MetricsRegionServerSource.STOREFILE_SIZE_DESC),
          this.regionWrapper.getStoreFileSize());
      mrb.addCounter(Interns.info(
              regionNamePrefix + MetricsRegionSource.COMPACTIONS_COMPLETED_COUNT,
              MetricsRegionSource.COMPACTIONS_COMPLETED_DESC),
          this.regionWrapper.getNumCompactionsCompleted());
      mrb.addCounter(Interns.info(
          regionNamePrefix + MetricsRegionSource.COMPACTIONS_FAILED_COUNT,
          MetricsRegionSource.COMPACTIONS_FAILED_DESC),
          this.regionWrapper.getNumCompactionsFailed());
      mrb.addCounter(Interns.info(
              regionNamePrefix + MetricsRegionSource.LAST_MAJOR_COMPACTION_AGE,
              MetricsRegionSource.LAST_MAJOR_COMPACTION_DESC),
          this.regionWrapper.getLastMajorCompactionAge());
      mrb.addCounter(Interns.info(
              regionNamePrefix + MetricsRegionSource.NUM_BYTES_COMPACTED_COUNT,
              MetricsRegionSource.NUM_BYTES_COMPACTED_DESC),
          this.regionWrapper.getNumBytesCompacted());
      mrb.addCounter(Interns.info(
              regionNamePrefix + MetricsRegionSource.NUM_FILES_COMPACTED_COUNT,
              MetricsRegionSource.NUM_FILES_COMPACTED_DESC),
          this.regionWrapper.getNumFilesCompacted());
      mrb.addCounter(Interns.info(
              regionNamePrefix + MetricsRegionServerSource.READ_REQUEST_COUNT,
              MetricsRegionServerSource.READ_REQUEST_COUNT_DESC),
          this.regionWrapper.getReadRequestCount());
      mrb.addCounter(Interns.info(
              regionNamePrefix + MetricsRegionServerSource.FILTERED_READ_REQUEST_COUNT,
              MetricsRegionServerSource.FILTERED_READ_REQUEST_COUNT_DESC),
          this.regionWrapper.getFilteredReadRequestCount());
      mrb.addCounter(Interns.info(
              regionNamePrefix + MetricsRegionServerSource.WRITE_REQUEST_COUNT,
              MetricsRegionServerSource.WRITE_REQUEST_COUNT_DESC),
          this.regionWrapper.getWriteRequestCount());
      mrb.addCounter(Interns.info(
              regionNamePrefix + MetricsRegionSource.REPLICA_ID,
              MetricsRegionSource.REPLICA_ID_DESC),
          this.regionWrapper.getReplicaId());
      mrb.addCounter(Interns.info(
              regionNamePrefix + MetricsRegionSource.COMPACTIONS_QUEUED_COUNT,
              MetricsRegionSource.COMPACTIONS_QUEUED_DESC),
          this.regionWrapper.getNumCompactionsQueued());
      mrb.addCounter(Interns.info(
              regionNamePrefix + MetricsRegionSource.FLUSHES_QUEUED_COUNT,
              MetricsRegionSource.FLUSHES_QUEUED_DESC),
          this.regionWrapper.getNumFlushesQueued());
      mrb.addCounter(Interns.info(
              regionNamePrefix + MetricsRegionSource.MAX_COMPACTION_QUEUE_SIZE,
              MetricsRegionSource.MAX_COMPACTION_QUEUE_DESC),
          this.regionWrapper.getMaxCompactionQueueSize());
      mrb.addCounter(Interns.info(
              regionNamePrefix + MetricsRegionSource.MAX_FLUSH_QUEUE_SIZE,
              MetricsRegionSource.MAX_FLUSH_QUEUE_DESC),
          this.regionWrapper.getMaxFlushQueueSize());
      addCounter(mrb, this.regionWrapper.getMemstoreOnlyRowReadsCount(),
        MetricsRegionSource.ROW_READS_ONLY_ON_MEMSTORE,
        MetricsRegionSource.ROW_READS_ONLY_ON_MEMSTORE_DESC);
      addCounter(mrb, this.regionWrapper.getMixedRowReadsCount(),
        MetricsRegionSource.MIXED_ROW_READS,
        MetricsRegionSource.MIXED_ROW_READS_ON_STORE_DESC);
    }
  }

  private void addCounter(MetricsRecordBuilder mrb, Map<String, Long> metricMap, String metricName,
      String metricDesc) {
    if (metricMap != null) {
      for (Entry<String, Long> entry : metricMap.entrySet()) {
        // append 'store' and its name to the metric
        mrb.addCounter(Interns.info(
          this.regionNamePrefix1 + _STORE + entry.getKey() + this.regionNamePrefix2 + metricName,
          metricDesc), entry.getValue());
      }
    }
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this ||
        (obj instanceof MetricsRegionSourceImpl && compareTo((MetricsRegionSourceImpl) obj) == 0);
  }
}
