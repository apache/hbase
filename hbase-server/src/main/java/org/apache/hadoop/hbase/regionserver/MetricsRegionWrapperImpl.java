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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.metrics2.MetricsExecutor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MetricsRegionWrapperImpl implements MetricsRegionWrapper, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsRegionWrapperImpl.class);

  public static final int PERIOD = 45;
  public static final String UNKNOWN = "unknown";

  private final HRegion region;
  private ScheduledExecutorService executor;
  private Runnable runnable;
  private long numStoreFiles;
  private long storeRefCount;
  private long maxCompactedStoreFileRefCount;
  private long memstoreSize;
  private long storeFileSize;
  private long maxStoreFileAge;
  private long minStoreFileAge;
  private long avgStoreFileAge;
  private long numReferenceFiles;
  private long maxFlushQueueSize;
  private long maxCompactionQueueSize;
  private Map<String, Long> readsOnlyFromMemstore;
  private Map<String, Long> mixedReadsOnStore;
  private Map<Integer, Long> storeFilesAccessedDaysAndSize;

  private int[] storeFilesAccessedDaysThresholds;
  private ScheduledFuture<?> regionMetricsUpdateTask;

  // Count the size of the region's storefiles according to the access time,
  // and set the size to the accessed-days interval to expose it to metrics.
  // the accessed-days interval default value is "7,30,90", we can change it through
  // 'hbase.region.hfile.accessed.days.thresholds'. see HBASE-27483 for details.
  public static final long ONE_DAY_MS = 24 * 60 * 60 * 1000;
  public static final String STOREFILES_ACCESSED_DAYS_THRESHOLDS =
    "hbase.region.hfile.accessed.days.thresholds";
  public static final int[] STOREFILES_ACCESSED_DAYS_THRESHOLDS_DEFAULT = { 7, 30, 90 };

  public MetricsRegionWrapperImpl(HRegion region) {
    this.region = region;
    this.executor = CompatibilitySingletonFactory.getInstance(MetricsExecutor.class).getExecutor();
    this.runnable = new HRegionMetricsWrapperRunnable();
    this.regionMetricsUpdateTask =
      this.executor.scheduleWithFixedDelay(this.runnable, PERIOD, PERIOD, TimeUnit.SECONDS);

    storeFilesAccessedDaysThresholds =
      region.getReadOnlyConfiguration().getInts(STOREFILES_ACCESSED_DAYS_THRESHOLDS);
    if (storeFilesAccessedDaysThresholds == null || storeFilesAccessedDaysThresholds.length == 0) {
      storeFilesAccessedDaysThresholds = STOREFILES_ACCESSED_DAYS_THRESHOLDS_DEFAULT;
    }
  }

  @Override
  public String getTableName() {
    TableDescriptor tableDesc = this.region.getTableDescriptor();
    if (tableDesc == null) {
      return UNKNOWN;
    }
    return tableDesc.getTableName().getQualifierAsString();
  }

  @Override
  public String getNamespace() {
    TableDescriptor tableDesc = this.region.getTableDescriptor();
    if (tableDesc == null) {
      return UNKNOWN;
    }
    return tableDesc.getTableName().getNamespaceAsString();
  }

  @Override
  public String getRegionName() {
    RegionInfo regionInfo = this.region.getRegionInfo();
    if (regionInfo == null) {
      return UNKNOWN;
    }
    return regionInfo.getEncodedName();
  }

  @Override
  public long getNumStores() {
    Map<byte[], HStore> stores = this.region.stores;
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
  public long getMemStoreSize() {
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
  public long getMaxCompactedStoreFileRefCount() {
    return maxCompactedStoreFileRefCount;
  }

  @Override
  public long getReadRequestCount() {
    return this.region.getReadRequestsCount();
  }

  @Override
  public long getCpRequestCount() {
    return this.region.getCpRequestsCount();
  }

  @Override
  public long getFilteredReadRequestCount() {
    return this.region.getFilteredReadRequestsCount();
  }

  @Override
  public long getWriteRequestCount() {
    return this.region.getWriteRequestsCount();
  }

  @Override
  public long getNumFilesCompacted() {
    return this.region.compactionNumFilesCompacted.sum();
  }

  @Override
  public long getNumBytesCompacted() {
    return this.region.compactionNumBytesCompacted.sum();
  }

  @Override
  public long getNumCompactionsCompleted() {
    return this.region.compactionsFinished.sum();
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
  public long getTotalRequestCount() {
    return getReadRequestCount() + getWriteRequestCount();
  }

  @Override
  public long getNumCompactionsFailed() {
    return this.region.compactionsFailed.sum();
  }

  @Override
  public long getNumCompactionsQueued() {
    return this.region.compactionsQueued.sum();
  }

  @Override
  public long getNumFlushesQueued() {
    return this.region.flushesQueued.sum();
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

  @Override
  public Map<String, Long> getMemstoreOnlyRowReadsCount() {
    return readsOnlyFromMemstore;
  }

  @Override
  public Map<String, Long> getMixedRowReadsCount() {
    return mixedReadsOnStore;
  }

  @Override
  public Map<Integer, Long> getStoreFilesAccessedDaysAndSize() {
    return storeFilesAccessedDaysAndSize;
  }

  public class HRegionMetricsWrapperRunnable implements Runnable {

    @Override
    public void run() {
      long tempNumStoreFiles = 0;
      int tempStoreRefCount = 0;
      int tempMaxCompactedStoreFileRefCount = 0;
      long tempMemstoreSize = 0;
      long tempStoreFileSize = 0;
      long tempMaxStoreFileAge = 0;
      long tempMinStoreFileAge = Long.MAX_VALUE;
      long tempNumReferenceFiles = 0;
      long tempMaxCompactionQueueSize = 0;
      long tempMaxFlushQueueSize = 0;
      long avgAgeNumerator = 0;
      long numHFiles = 0;
      Map<Integer, Long> tmpStoreFileAccessDaysAndSize = new TreeMap<>();
      for (int threshold : storeFilesAccessedDaysThresholds) {
        tmpStoreFileAccessDaysAndSize.put(threshold, 0L);
      }
      if (region.getStores() != null) {
        for (HStore store : region.getStores()) {
          tempNumStoreFiles += store.getStorefilesCount();
          int currentStoreRefCount = store.getStoreRefCount();
          tempStoreRefCount += currentStoreRefCount;
          int currentMaxCompactedStoreFileRefCount = store.getMaxCompactedStoreFileRefCount();
          tempMaxCompactedStoreFileRefCount =
            Math.max(tempMaxCompactedStoreFileRefCount, currentMaxCompactedStoreFileRefCount);
          tempMemstoreSize += store.getMemStoreSize().getDataSize();
          tempStoreFileSize += store.getStorefilesSize();
          OptionalLong storeMaxStoreFileAge = store.getMaxStoreFileAge();
          if (
            storeMaxStoreFileAge.isPresent()
              && storeMaxStoreFileAge.getAsLong() > tempMaxStoreFileAge
          ) {
            tempMaxStoreFileAge = storeMaxStoreFileAge.getAsLong();
          }

          OptionalLong storeMinStoreFileAge = store.getMinStoreFileAge();
          if (
            storeMinStoreFileAge.isPresent()
              && storeMinStoreFileAge.getAsLong() < tempMinStoreFileAge
          ) {
            tempMinStoreFileAge = storeMinStoreFileAge.getAsLong();
          }

          long storeHFiles = store.getNumHFiles();
          numHFiles += storeHFiles;
          tempNumReferenceFiles += store.getNumReferenceFiles();

          OptionalDouble storeAvgStoreFileAge = store.getAvgStoreFileAge();
          if (storeAvgStoreFileAge.isPresent()) {
            avgAgeNumerator += (long) storeAvgStoreFileAge.getAsDouble() * storeHFiles;
          }
          if (mixedReadsOnStore == null) {
            mixedReadsOnStore = new HashMap<String, Long>();
          }
          Long tempVal = mixedReadsOnStore.get(store.getColumnFamilyName());
          if (tempVal == null) {
            tempVal = 0L;
          } else {
            tempVal += store.getMixedRowReadsCount();
          }
          mixedReadsOnStore.put(store.getColumnFamilyName(), tempVal);
          if (readsOnlyFromMemstore == null) {
            readsOnlyFromMemstore = new HashMap<String, Long>();
          }
          tempVal = readsOnlyFromMemstore.get(store.getColumnFamilyName());
          if (tempVal == null) {
            tempVal = 0L;
          } else {
            tempVal += store.getMemstoreOnlyRowReadsCount();
          }
          readsOnlyFromMemstore.put(store.getColumnFamilyName(), tempVal);

          Map<String, Pair<Long, Long>> sfAccessTimeAndSizeMap =
            store.getStoreFilesAccessTimeAndSize();
          long now = EnvironmentEdgeManager.currentTime();
          for (Pair<Long, Long> pair : sfAccessTimeAndSizeMap.values()) {
            long accessTime = pair.getFirst();
            long size = pair.getSecond();
            for (int threshold : storeFilesAccessedDaysThresholds) {
              long sumSize = tmpStoreFileAccessDaysAndSize.get(threshold);
              if ((now - accessTime) >= threshold * ONE_DAY_MS) {
                sumSize = sumSize + size;
                tmpStoreFileAccessDaysAndSize.put(threshold, sumSize);
              }
            }
          }
        }
      }
      storeFilesAccessedDaysAndSize = tmpStoreFileAccessDaysAndSize;
      numStoreFiles = tempNumStoreFiles;
      storeRefCount = tempStoreRefCount;
      maxCompactedStoreFileRefCount = tempMaxCompactedStoreFileRefCount;
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
