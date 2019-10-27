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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.metrics2.MetricsExecutor;

@InterfaceAudience.Private
public class MetricsRegionWrapperImpl implements MetricsRegionWrapper, Closeable {

  private static final Log LOG = LogFactory.getLog(MetricsRegionWrapperImpl.class);

  public static final int PERIOD = 45;
  public static final String UNKNOWN = "unknown";

  private final HRegion region;
  private ScheduledExecutorService executor;
  private Runnable runnable;
  private long numStoreFiles;
  private long storeRefCount;
  private long memstoreSize;
  private long storeFileSize;
  private long maxStoreFileAge;
  private long minStoreFileAge;
  private long avgStoreFileAge;
  private long numReferenceFiles;
  private long maxFlushQueueSize;
  private long maxCompactionQueueSize;
  private int maxStoreFileRefCount;

  private ScheduledFuture<?> regionMetricsUpdateTask;

  public MetricsRegionWrapperImpl(HRegion region) {
    this.region = region;
    this.executor = CompatibilitySingletonFactory.getInstance(MetricsExecutor.class).getExecutor();
    this.runnable = new HRegionMetricsWrapperRunnable();
    this.regionMetricsUpdateTask = this.executor.scheduleWithFixedDelay(this.runnable, PERIOD,
      PERIOD, TimeUnit.SECONDS);
  }

  @Override
  public String getTableName() {
    HTableDescriptor tableDesc = this.region.getTableDesc();
    if (tableDesc == null) {
      return UNKNOWN;
    }
    return tableDesc.getTableName().getQualifierAsString();
  }

  @Override
  public String getNamespace() {
    HTableDescriptor tableDesc = this.region.getTableDesc();
    if (tableDesc == null) {
      return UNKNOWN;
    }
    return tableDesc.getTableName().getNamespaceAsString();
  }


  @Override
  public String getRegionName() {
    HRegionInfo regionInfo = this.region.getRegionInfo();
    if (regionInfo == null) {
      return UNKNOWN;
    }
    return regionInfo.getEncodedName();
  }

  @Override
  public long getNumStores() {
    Map<byte[],Store> stores = this.region.stores;
    if (stores == null) {
      return 0;
    }
    return stores.size();
  }

  @Override
  public long getNumStoreFiles() {
    return numStoreFiles;
  }

  @Override
  public long getMemstoreSize() {
    return memstoreSize;
  }

  @Override
  public long getStoreFileSize() {
    return storeFileSize;
  }

  @Override
  public long getStoreRefCount() {
    return storeRefCount;
  }

  @Override
  public int getMaxStoreFileRefCount() {
    return maxStoreFileRefCount;
  }

  @Override
  public long getReadRequestCount() {
    return this.region.getReadRequestsCount();
  }

  @Override
  public long getWriteRequestCount() {
    return this.region.getWriteRequestsCount();
  }

  @Override
  public long getNumFilesCompacted() {
    return this.region.compactionNumFilesCompacted.get();
  }

  @Override
  public long getNumBytesCompacted() {
    return this.region.compactionNumBytesCompacted.get();
  }

  @Override
  public long getNumCompactionsCompleted() {
    return this.region.compactionsFinished.get();
  }

  @Override
  public long getLastMajorCompactionAge() {
    long lastMajorCompactionTs = 0L;
    try {
      lastMajorCompactionTs = this.region.getOldestHfileTs(true);
    } catch (IOException ioe) {
      LOG.error("Could not load HFile info ", ioe);
    }
    long now = EnvironmentEdgeManager.currentTime();
    return now - lastMajorCompactionTs;
  }

  @Override
  public long getNumCompactionsFailed() {
    return this.region.compactionsFailed.get();
  }

  @Override
  public long getNumCompactionsQueued() {
    return this.region.compactionsQueued.get();
  }

  @Override
  public long getNumFlushesQueued() {
    return this.region.flushesQueued.get();
  }

  @Override
  public long getMaxCompactionQueueSize() {
    return maxCompactionQueueSize;
  }

  @Override
  public long getMaxFlushQueueSize() {
    return maxFlushQueueSize;
  }

  @Override
  public long getMaxStoreFileAge() {
    return maxStoreFileAge;
  }

  @Override
  public long getMinStoreFileAge() {
    return minStoreFileAge;
  }

  @Override
  public long getAvgStoreFileAge() {
    return avgStoreFileAge;
  }

  @Override
  public long getNumReferenceFiles() {
    return numReferenceFiles;
  }

  @Override
  public int getRegionHashCode() {
    return this.region.hashCode();
  }

  public class HRegionMetricsWrapperRunnable implements Runnable {

    @Override
    public void run() {
      long tempNumStoreFiles = 0;
      int tempStoreRefCount = 0;
      int tempMaxStoreFileRefCount = 0;
      long tempMemstoreSize = 0;
      long tempStoreFileSize = 0;
      long tempMaxStoreFileAge = 0;
      long tempMinStoreFileAge = Long.MAX_VALUE;
      long tempNumReferenceFiles = 0;
      long tempMaxCompactionQueueSize = 0;
      long tempMaxFlushQueueSize = 0;

      long avgAgeNumerator = 0;
      long numHFiles = 0;
      if (region.stores != null) {
        for (Store store : region.stores.values()) {
          tempNumStoreFiles += store.getStorefilesCount();
          tempMemstoreSize += store.getMemStoreSize();
          tempStoreFileSize += store.getStorefilesSize();

          long storeMaxStoreFileAge = store.getMaxStoreFileAge();
          tempMaxStoreFileAge = (storeMaxStoreFileAge > tempMaxStoreFileAge) ?
            storeMaxStoreFileAge : tempMaxStoreFileAge;

          long storeMinStoreFileAge = store.getMinStoreFileAge();
          tempMinStoreFileAge = (storeMinStoreFileAge < tempMinStoreFileAge) ?
            storeMinStoreFileAge : tempMinStoreFileAge;

          long storeHFiles = store.getNumHFiles();
          avgAgeNumerator += store.getAvgStoreFileAge() * storeHFiles;
          numHFiles += storeHFiles;
          tempNumReferenceFiles += store.getNumReferenceFiles();

          if (store instanceof HStore) {
            // Cast here to avoid interface changes to Store
            HStore hStore = ((HStore) store);
            tempStoreRefCount += hStore.getStoreRefCount();
            int currentMaxStoreFileRefCount = hStore.getMaxStoreFileRefCount();
            tempMaxStoreFileRefCount = Math.max(tempMaxStoreFileRefCount,
              currentMaxStoreFileRefCount);
          }
        }
      }

      numStoreFiles = tempNumStoreFiles;
      storeRefCount = tempStoreRefCount;
      maxStoreFileRefCount = tempMaxStoreFileRefCount;
      memstoreSize = tempMemstoreSize;
      storeFileSize = tempStoreFileSize;
      maxStoreFileAge = tempMaxStoreFileAge;
      if (tempMinStoreFileAge != Long.MAX_VALUE) {
        minStoreFileAge = tempMinStoreFileAge;
      }

      if (numHFiles != 0) {
        avgStoreFileAge = avgAgeNumerator / numHFiles;
      }

      numReferenceFiles = tempNumReferenceFiles;
      tempMaxCompactionQueueSize = getNumCompactionsQueued();
      tempMaxFlushQueueSize = getNumFlushesQueued();
      if (tempMaxCompactionQueueSize > maxCompactionQueueSize) {
        maxCompactionQueueSize = tempMaxCompactionQueueSize;
      }
      if (tempMaxFlushQueueSize > maxFlushQueueSize) {
        maxFlushQueueSize = tempMaxFlushQueueSize;
      }
    }
  }

  @Override
  public void close() throws IOException {
    regionMetricsUpdateTask.cancel(true);
  }

  /**
   * Get the replica id of this region.
   */
  @Override
  public int getReplicaId() {
    return region.getRegionInfo().getReplicaId();
  }

}
