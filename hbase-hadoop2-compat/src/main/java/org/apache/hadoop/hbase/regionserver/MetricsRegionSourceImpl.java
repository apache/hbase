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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

@InterfaceAudience.Private
public class MetricsRegionSourceImpl implements MetricsRegionSource {

  private static final Log LOG = LogFactory.getLog(MetricsRegionSourceImpl.class);

  private AtomicBoolean closed = new AtomicBoolean(false);

  // Non-final so that we can null out the wrapper
  // This is just paranoia. We really really don't want to
  // leak a whole region by way of keeping the
  // regionWrapper around too long.
  private MetricsRegionWrapper regionWrapper;

  private final MetricsRegionAggregateSourceImpl agg;
  private final DynamicMetricsRegistry registry;

  private final String regionNamePrefix;
  private final String regionPutKey;
  private final String regionDeleteKey;
  private final String regionGetKey;
  private final String regionIncrementKey;
  private final String regionAppendKey;
  private final String regionScanSizeKey;
  private final String regionScanTimeKey;

  private final MutableFastCounter regionPut;
  private final MutableFastCounter regionDelete;
  private final MutableFastCounter regionIncrement;
  private final MutableFastCounter regionAppend;
  private final MetricHistogram regionGet;
  private final MetricHistogram regionScanSize;
  private final MetricHistogram regionScanTime;
  private final int hashCode;

  public MetricsRegionSourceImpl(MetricsRegionWrapper regionWrapper,
                                 MetricsRegionAggregateSourceImpl aggregate) {
    this.regionWrapper = regionWrapper;
    agg = aggregate;
    agg.register(this);

    LOG.debug("Creating new MetricsRegionSourceImpl for table " +
        regionWrapper.getTableName() + " " + regionWrapper.getRegionName());

    registry = agg.getMetricsRegistry();

    regionNamePrefix = "Namespace_" + regionWrapper.getNamespace() +
        "_table_" + regionWrapper.getTableName() +
        "_region_" + regionWrapper.getRegionName()  +
        "_metric_";

    String suffix = "Count";

    regionPutKey = regionNamePrefix + MetricsRegionServerSource.MUTATE_KEY + suffix;
    regionPut = registry.getCounter(regionPutKey, 0L);

    regionDeleteKey = regionNamePrefix + MetricsRegionServerSource.DELETE_KEY + suffix;
    regionDelete = registry.getCounter(regionDeleteKey, 0L);

    regionIncrementKey = regionNamePrefix + MetricsRegionServerSource.INCREMENT_KEY + suffix;
    regionIncrement = registry.getCounter(regionIncrementKey, 0L);

    regionAppendKey = regionNamePrefix + MetricsRegionServerSource.APPEND_KEY + suffix;
    regionAppend = registry.getCounter(regionAppendKey, 0L);

    regionGetKey = regionNamePrefix + MetricsRegionServerSource.GET_KEY;
    regionGet = registry.newTimeHistogram(regionGetKey);

    regionScanSizeKey = regionNamePrefix + MetricsRegionServerSource.SCAN_SIZE_KEY;
    regionScanSize = registry.newSizeHistogram(regionScanSizeKey);

    regionScanTimeKey = regionNamePrefix + MetricsRegionServerSource.SCAN_TIME_KEY;
    regionScanTime = registry.newTimeHistogram(regionScanTimeKey);

    hashCode = regionWrapper.getRegionHashCode();
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
      registry.removeMetric(regionScanSizeKey);
      registry.removeMetric(regionScanTimeKey);
      registry.removeHistogramMetrics(regionGetKey);
      registry.removeHistogramMetrics(regionScanSizeKey);
      registry.removeHistogramMetrics(regionScanTimeKey);

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
  public void updateGet(long getSize) {
    regionGet.add(getSize);
  }

  @Override
  public void updateScanSize(long scanSize) {
    regionScanSize.add(scanSize);
  }

  @Override
  public void updateScanTime(long mills) {
    regionScanTime.add(mills);
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

    MetricsRegionSourceImpl impl = (MetricsRegionSourceImpl) source;
    if (impl == null) {
      return -1;
    }

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
              regionNamePrefix + MetricsRegionServerSource.MEMSTORE_SIZE,
              MetricsRegionServerSource.MEMSTORE_SIZE_DESC),
          this.regionWrapper.getMemstoreSize());
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
              regionNamePrefix + MetricsRegionServerSource.WRITE_REQUEST_COUNT,
              MetricsRegionServerSource.WRITE_REQUEST_COUNT_DESC),
          this.regionWrapper.getWriteRequestCount());
      mrb.addCounter(Interns.info(regionNamePrefix + MetricsRegionSource.REPLICA_ID,
              MetricsRegionSource.REPLICA_ID_DESC),
          this.regionWrapper.getReplicaId());
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
