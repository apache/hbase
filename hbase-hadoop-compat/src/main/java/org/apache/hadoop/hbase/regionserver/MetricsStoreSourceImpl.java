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

import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MetricsStoreSourceImpl implements MetricsStoreSource {

  private MetricsStoreWrapper storeWrapper;
  private MetricsStoreAggregateSourceImpl aggreagate;
  private AtomicBoolean closed = new AtomicBoolean(false);

  private String storeNamePrefix;
  private static final Logger LOG = LoggerFactory.getLogger(MetricsStoreSourceImpl.class);

  public MetricsStoreSourceImpl(MetricsStoreWrapper storeWrapper,
      MetricsStoreAggregateSourceImpl aggreagate) {
    this.storeWrapper = storeWrapper;
    this.aggreagate = aggreagate;
    aggreagate.register(this);

    LOG.info("Creating new MetricsStoreSourceImpl for store " + storeWrapper.getRegionName() + " "
        + storeWrapper.getStoreName());

    storeNamePrefix = "Namespace_" + storeWrapper.getNamespace() + "_table_"
        + storeWrapper.getTableName() + "_region_" + storeWrapper.getRegionName() + "_store_"
        + storeWrapper.getStoreName() + "_metric_";
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
    aggreagate.deregister(this);

    // While it's un-likely that snapshot and close happen at the same time it's still possible.
    // So grab the lock to ensure that all calls to snapshot are done before we remove the metrics
    synchronized (this) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Removing store Metrics: " + storeWrapper.getStoreName());
      }
      storeWrapper = null;
    }
  }

  @Override
  public int compareTo(MetricsStoreSource source) {
    if (!(source instanceof MetricsStoreSourceImpl)) {
      return -1;
    }

    MetricsStoreSourceImpl impl = (MetricsStoreSourceImpl) source;
    if (impl == null) {
      return -1;
    }

    return Long.compare(this.hashCode(), impl.hashCode());
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this
        || (obj instanceof MetricsStoreSourceImpl && compareTo((MetricsStoreSourceImpl) obj) == 0);
  }

  @Override
  public int hashCode() {
    return (this.storeWrapper.getRegionName() + this.storeWrapper.getStoreName()).hashCode();
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
      mrb.addGauge(Interns.info(storeNamePrefix + MetricsRegionServerSource.STOREFILE_COUNT,
        MetricsRegionServerSource.STOREFILE_COUNT_DESC), this.storeWrapper.getNumStoreFiles());
      mrb.addGauge(Interns.info(storeNamePrefix + MetricsRegionServerSource.STORE_REF_COUNT,
        MetricsRegionServerSource.STORE_REF_COUNT), this.storeWrapper.getStoreRefCount());
      mrb.addGauge(Interns.info(storeNamePrefix + MetricsRegionServerSource.MEMSTORE_SIZE,
        MetricsRegionServerSource.MEMSTORE_SIZE_DESC), this.storeWrapper.getMemStoreSize());
      mrb.addGauge(
        Interns.info(storeNamePrefix + MetricsRegionServerSource.MAX_STORE_FILE_AGE,
          MetricsRegionServerSource.MAX_STORE_FILE_AGE_DESC),
        this.storeWrapper.getMaxStoreFileAge());
      mrb.addGauge(
        Interns.info(storeNamePrefix + MetricsRegionServerSource.MIN_STORE_FILE_AGE,
          MetricsRegionServerSource.MIN_STORE_FILE_AGE_DESC),
        this.storeWrapper.getMinStoreFileAge());
      mrb.addGauge(
        Interns.info(storeNamePrefix + MetricsRegionServerSource.AVG_STORE_FILE_AGE,
          MetricsRegionServerSource.AVG_STORE_FILE_AGE_DESC),
        this.storeWrapper.getAvgStoreFileAge());
      mrb.addGauge(
        Interns.info(storeNamePrefix + MetricsRegionServerSource.NUM_REFERENCE_FILES,
          MetricsRegionServerSource.NUM_REFERENCE_FILES_DESC),
        this.storeWrapper.getNumReferenceFiles());
      mrb.addGauge(Interns.info(storeNamePrefix + MetricsRegionServerSource.STOREFILE_SIZE,
        MetricsRegionServerSource.STOREFILE_SIZE_DESC), this.storeWrapper.getStoreFileSize());
      mrb.addCounter(
        Interns.info(storeNamePrefix + MetricsRegionSource.READ_REQUEST_ON_MEMSTORE,
          MetricsRegionSource.READ_REQUEST_ON_MEMSTORE_DESC),
        this.storeWrapper.getMemstoreReadRequestsCount());
      mrb.addCounter(
        Interns.info(storeNamePrefix + MetricsRegionSource.READ_REQUEST_ON_FILE,
          MetricsRegionSource.READ_REQUEST_ON_FILE_DESC),
        this.storeWrapper.getFileReadRequestsCount());
    }
  }

  @Override
  public MetricsStoreAggregateSource getAggregateSource() {
    return aggreagate;
  }
}