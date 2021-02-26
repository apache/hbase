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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.CombinedBlockCache;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWALSource;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hdfs.DFSHedgedReadMetrics;
import org.apache.hadoop.metrics2.MetricsExecutor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Impl for exposing HRegionServer Information through Hadoop's metrics 2 system.
 */
@InterfaceAudience.Private
class MetricsRegionServerWrapperImpl
    implements MetricsRegionServerWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsRegionServerWrapperImpl.class);

  private final HRegionServer regionServer;
  private final MetricsWALSource metricsWALSource;
  private final ByteBuffAllocator allocator;

  private BlockCache blockCache;
  private MobFileCache mobFileCache;
  private CacheStats cacheStats;
  private CacheStats l1Stats = null;
  private CacheStats l2Stats = null;

  private volatile long numStores = 0;
  private volatile long numWALFiles = 0;
  private volatile long walFileSize = 0;
  private volatile long numStoreFiles = 0;
  private volatile long memstoreSize = 0;
  private volatile long storeFileSize = 0;
  private volatile long maxStoreFileAge = 0;
  private volatile long minStoreFileAge = 0;
  private volatile long avgStoreFileAge = 0;
  private volatile long numReferenceFiles = 0;
  private volatile double requestsPerSecond = 0.0;
  private volatile long readRequestsCount = 0;
  private volatile double readRequestsRatePerSecond = 0;
  private volatile long filteredReadRequestsCount = 0;
  private volatile long writeRequestsCount = 0;
  private volatile double writeRequestsRatePerSecond = 0;
  private volatile long checkAndMutateChecksFailed = 0;
  private volatile long checkAndMutateChecksPassed = 0;
  private volatile long storefileIndexSize = 0;
  private volatile long totalStaticIndexSize = 0;
  private volatile long totalStaticBloomSize = 0;
  private volatile long numMutationsWithoutWAL = 0;
  private volatile long dataInMemoryWithoutWAL = 0;
  private volatile double percentFileLocal = 0;
  private volatile double percentFileLocalSecondaryRegions = 0;
  private volatile long flushedCellsCount = 0;
  private volatile long compactedCellsCount = 0;
  private volatile long majorCompactedCellsCount = 0;
  private volatile long flushedCellsSize = 0;
  private volatile long compactedCellsSize = 0;
  private volatile long majorCompactedCellsSize = 0;
  private volatile long cellsCountCompactedToMob = 0;
  private volatile long cellsCountCompactedFromMob = 0;
  private volatile long cellsSizeCompactedToMob = 0;
  private volatile long cellsSizeCompactedFromMob = 0;
  private volatile long mobFlushCount = 0;
  private volatile long mobFlushedCellsCount = 0;
  private volatile long mobFlushedCellsSize = 0;
  private volatile long mobScanCellsCount = 0;
  private volatile long mobScanCellsSize = 0;
  private volatile long mobFileCacheAccessCount = 0;
  private volatile long mobFileCacheMissCount = 0;
  private volatile double mobFileCacheHitRatio = 0;
  private volatile long mobFileCacheEvictedCount = 0;
  private volatile long mobFileCacheCount = 0;
  private volatile long blockedRequestsCount = 0L;
  private volatile long averageRegionSize = 0L;
  protected final Map<String, ArrayList<Long>>
      requestsCountCache = new ConcurrentHashMap<String, ArrayList<Long>>();

  private ScheduledExecutorService executor;
  private Runnable runnable;
  private long period;

  /**
   * Can be null if not on hdfs.
   */
  private DFSHedgedReadMetrics dfsHedgedReadMetrics;

  public MetricsRegionServerWrapperImpl(final HRegionServer regionServer) {
    this.regionServer = regionServer;
    initBlockCache();
    initMobFileCache();

    this.period = regionServer.getConfiguration().getLong(HConstants.REGIONSERVER_METRICS_PERIOD,
      HConstants.DEFAULT_REGIONSERVER_METRICS_PERIOD);

    this.executor = CompatibilitySingletonFactory.getInstance(MetricsExecutor.class).getExecutor();
    this.runnable = new RegionServerMetricsWrapperRunnable();
    this.executor.scheduleWithFixedDelay(this.runnable, this.period, this.period,
      TimeUnit.MILLISECONDS);
    this.metricsWALSource = CompatibilitySingletonFactory.getInstance(MetricsWALSource.class);
    this.allocator = regionServer.getRpcServer().getByteBuffAllocator();

    try {
      this.dfsHedgedReadMetrics = FSUtils.getDFSHedgedReadMetrics(regionServer.getConfiguration());
    } catch (IOException e) {
      LOG.warn("Failed to get hedged metrics", e);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Computing regionserver metrics every " + this.period + " milliseconds");
    }
  }

  private void initBlockCache() {
    this.blockCache = this.regionServer.getBlockCache().orElse(null);
    this.cacheStats = this.blockCache != null ? this.blockCache.getStats() : null;
    if (this.cacheStats != null) {
      if (this.cacheStats instanceof CombinedBlockCache.CombinedCacheStats) {
        l1Stats = ((CombinedBlockCache.CombinedCacheStats) this.cacheStats)
          .getLruCacheStats();
        l2Stats = ((CombinedBlockCache.CombinedCacheStats) this.cacheStats)
          .getBucketCacheStats();
      } else {
        l1Stats = this.cacheStats;
      }
    }
  }

  /**
   * Initializes the mob file cache.
   */
  private void initMobFileCache() {
    this.mobFileCache = this.regionServer.getMobFileCache().orElse(null);
  }

  @Override
  public String getClusterId() {
    return regionServer.getClusterId();
  }

  @Override
  public long getStartCode() {
    return regionServer.getStartcode();
  }

  @Override
  public String getZookeeperQuorum() {
    ZKWatcher zk = regionServer.getZooKeeper();
    if (zk == null) {
      return "";
    }
    return zk.getQuorum();
  }

  @Override
  public String getCoprocessors() {
    String[] coprocessors = regionServer.getRegionServerCoprocessors();
    if (coprocessors == null || coprocessors.length == 0) {
      return "";
    }
    return StringUtils.join(coprocessors, ", ");
  }

  @Override
  public String getServerName() {
    ServerName serverName = regionServer.getServerName();
    if (serverName == null) {
      return "";
    }
    return serverName.getServerName();
  }

  @Override
  public long getNumOnlineRegions() {
    Collection<HRegion> onlineRegionsLocalContext = regionServer.getOnlineRegionsLocalContext();
    if (onlineRegionsLocalContext == null) {
      return 0;
    }
    return onlineRegionsLocalContext.size();
  }

  @Override
  public long getTotalRequestCount() {
    return regionServer.rpcServices.requestCount.sum();
  }

  @Override
  public long getTotalRowActionRequestCount() {
    return readRequestsCount + writeRequestsCount;
  }

  @Override
  public int getSplitQueueSize() {
    if (this.regionServer.compactSplitThread == null) {
      return 0;
    }
    return this.regionServer.compactSplitThread.getSplitQueueSize();
  }

  @Override
  public int getCompactionQueueSize() {
    //The thread could be zero.  if so assume there is no queue.
    if (this.regionServer.compactSplitThread == null) {
      return 0;
    }
    return this.regionServer.compactSplitThread.getCompactionQueueSize();
  }

  @Override
  public int getSmallCompactionQueueSize() {
    //The thread could be zero.  if so assume there is no queue.
    if (this.regionServer.compactSplitThread == null) {
      return 0;
    }
    return this.regionServer.compactSplitThread.getSmallCompactionQueueSize();
  }

  @Override
  public int getLargeCompactionQueueSize() {
    //The thread could be zero.  if so assume there is no queue.
    if (this.regionServer.compactSplitThread == null) {
      return 0;
    }
    return this.regionServer.compactSplitThread.getLargeCompactionQueueSize();
  }

  @Override
  public int getFlushQueueSize() {
    //If there is no flusher there should be no queue.
    if (this.regionServer.getMemStoreFlusher() == null) {
      return 0;
    }
    return this.regionServer.getMemStoreFlusher().getFlushQueueSize();
  }

  @Override
  public long getBlockCacheCount() {
    return this.blockCache != null ? this.blockCache.getBlockCount() : 0L;
  }

  @Override
  public long getMemStoreLimit() {
    return this.regionServer.getRegionServerAccounting().getGlobalMemStoreLimit();
  }

  @Override
  public long getBlockCacheSize() {
    return this.blockCache != null ? this.blockCache.getCurrentSize() : 0L;
  }

  @Override
  public long getBlockCacheFreeSize() {
    return this.blockCache != null ? this.blockCache.getFreeSize() : 0L;
  }

  @Override
  public long getBlockCacheHitCount() {
    return this.cacheStats != null ? this.cacheStats.getHitCount() : 0L;
  }

  @Override
  public long getBlockCachePrimaryHitCount() {
    return this.cacheStats != null ? this.cacheStats.getPrimaryHitCount() : 0L;
  }

  @Override
  public long getBlockCacheMissCount() {
    return this.cacheStats != null ? this.cacheStats.getMissCount() : 0L;
  }

  @Override
  public long getBlockCachePrimaryMissCount() {
    return this.cacheStats != null ? this.cacheStats.getPrimaryMissCount() : 0L;
  }

  @Override
  public long getBlockCacheEvictedCount() {
    return this.cacheStats != null ? this.cacheStats.getEvictedCount() : 0L;
  }

  @Override
  public long getBlockCachePrimaryEvictedCount() {
    return this.cacheStats != null ? this.cacheStats.getPrimaryEvictedCount() : 0L;
  }

  @Override
  public double getBlockCacheHitPercent() {
    double ratio = this.cacheStats != null ? this.cacheStats.getHitRatio() : 0.0;
    if (Double.isNaN(ratio)) {
      ratio = 0;
    }
    return (ratio * 100);
  }

  @Override
  public double getBlockCacheHitCachingPercent() {
    double ratio = this.cacheStats != null ? this.cacheStats.getHitCachingRatio() : 0.0;
    if (Double.isNaN(ratio)) {
      ratio = 0;
    }
    return (ratio * 100);
  }

  @Override
  public long getBlockCacheFailedInsertions() {
    return this.cacheStats != null ? this.cacheStats.getFailedInserts() : 0L;
  }

  @Override
  public long getL1CacheHitCount() {
    return this.l1Stats != null ? this.l1Stats.getHitCount() : 0L;
  }

  @Override
  public long getL1CacheMissCount() {
    return this.l1Stats != null ? this.l1Stats.getMissCount() : 0L;
  }

  @Override
  public double getL1CacheHitRatio() {
    return this.l1Stats != null ? this.l1Stats.getHitRatio() : 0.0;
  }

  @Override
  public double getL1CacheMissRatio() {
    return this.l1Stats != null ? this.l1Stats.getMissRatio() : 0.0;
  }

  @Override
  public long getL2CacheHitCount() {
    return this.l2Stats != null ? this.l2Stats.getHitCount() : 0L;
  }

  @Override
  public long getL2CacheMissCount() {
    return this.l2Stats != null ? this.l2Stats.getMissCount() : 0L;
  }

  @Override
  public double getL2CacheHitRatio() {
    return this.l2Stats != null ? this.l2Stats.getHitRatio() : 0.0;
  }

  @Override
  public double getL2CacheMissRatio() {
    return this.l2Stats != null ? this.l2Stats.getMissRatio() : 0.0;
  }

  @Override public void forceRecompute() {
    this.runnable.run();
  }

  @Override
  public long getNumStores() {
    return numStores;
  }

  @Override
  public long getNumWALFiles() {
    return numWALFiles;
  }

  @Override
  public long getWALFileSize() {
    return walFileSize;
  }

  @Override
  public long getNumWALSlowAppend() {
    return metricsWALSource.getSlowAppendCount();
  }

  @Override
  public long getNumStoreFiles() {
    return numStoreFiles;
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
  public long getMemStoreSize() {
    return memstoreSize;
  }

  @Override
  public long getStoreFileSize() {
    return storeFileSize;
  }

  @Override public double getRequestsPerSecond() {
    return requestsPerSecond;
  }

  @Override
  public long getReadRequestsCount() {
    return readRequestsCount;
  }

  @Override
  public double getReadRequestsRatePerSecond() {
    return readRequestsRatePerSecond;
  }

  @Override
  public long getFilteredReadRequestsCount() {
    return filteredReadRequestsCount;
  }

  @Override
  public long getWriteRequestsCount() {
    return writeRequestsCount;
  }

  @Override
  public double getWriteRequestsRatePerSecond() {
    return writeRequestsRatePerSecond;
  }

  @Override
  public long getRpcGetRequestsCount() {
    return regionServer.rpcServices.rpcGetRequestCount.sum();
  }

  @Override
  public long getRpcScanRequestsCount() {
    return regionServer.rpcServices.rpcScanRequestCount.sum();
  }

  @Override
  public long getRpcFullScanRequestsCount() {
    return regionServer.rpcServices.rpcFullScanRequestCount.sum();
  }

  @Override
  public long getRpcMultiRequestsCount() {
    return regionServer.rpcServices.rpcMultiRequestCount.sum();
  }

  @Override
  public long getRpcMutateRequestsCount() {
    return regionServer.rpcServices.rpcMutateRequestCount.sum();
  }

  @Override
  public long getCheckAndMutateChecksFailed() {
    return checkAndMutateChecksFailed;
  }

  @Override
  public long getCheckAndMutateChecksPassed() {
    return checkAndMutateChecksPassed;
  }

  @Override
  public long getStoreFileIndexSize() {
    return storefileIndexSize;
  }

  @Override
  public long getTotalStaticIndexSize() {
    return totalStaticIndexSize;
  }

  @Override
  public long getTotalStaticBloomSize() {
    return totalStaticBloomSize;
  }

  @Override
  public long getNumMutationsWithoutWAL() {
    return numMutationsWithoutWAL;
  }

  @Override
  public long getDataInMemoryWithoutWAL() {
    return dataInMemoryWithoutWAL;
  }

  @Override
  public double getPercentFileLocal() {
    return percentFileLocal;
  }

  @Override
  public double getPercentFileLocalSecondaryRegions() {
    return percentFileLocalSecondaryRegions;
  }

  @Override
  public long getUpdatesBlockedTime() {
    if (this.regionServer.getMemStoreFlusher() == null) {
      return 0;
    }
    return this.regionServer.getMemStoreFlusher().getUpdatesBlockedMsHighWater().sum();
  }

  @Override
  public long getFlushedCellsCount() {
    return flushedCellsCount;
  }

  @Override
  public long getCompactedCellsCount() {
    return compactedCellsCount;
  }

  @Override
  public long getMajorCompactedCellsCount() {
    return majorCompactedCellsCount;
  }

  @Override
  public long getFlushedCellsSize() {
    return flushedCellsSize;
  }

  @Override
  public long getCompactedCellsSize() {
    return compactedCellsSize;
  }

  @Override
  public long getMajorCompactedCellsSize() {
    return majorCompactedCellsSize;
  }

  @Override
  public long getCellsCountCompactedFromMob() {
    return cellsCountCompactedFromMob;
  }

  @Override
  public long getCellsCountCompactedToMob() {
    return cellsCountCompactedToMob;
  }

  @Override
  public long getCellsSizeCompactedFromMob() {
    return cellsSizeCompactedFromMob;
  }

  @Override
  public long getCellsSizeCompactedToMob() {
    return cellsSizeCompactedToMob;
  }

  @Override
  public long getMobFlushCount() {
    return mobFlushCount;
  }

  @Override
  public long getMobFlushedCellsCount() {
    return mobFlushedCellsCount;
  }

  @Override
  public long getMobFlushedCellsSize() {
    return mobFlushedCellsSize;
  }

  @Override
  public long getMobScanCellsCount() {
    return mobScanCellsCount;
  }

  @Override
  public long getMobScanCellsSize() {
    return mobScanCellsSize;
  }

  @Override
  public long getMobFileCacheAccessCount() {
    return mobFileCacheAccessCount;
  }

  @Override
  public long getMobFileCacheMissCount() {
    return mobFileCacheMissCount;
  }

  @Override
  public long getMobFileCacheCount() {
    return mobFileCacheCount;
  }

  @Override
  public long getMobFileCacheEvictedCount() {
    return mobFileCacheEvictedCount;
  }

  @Override
  public double getMobFileCacheHitPercent() {
    return mobFileCacheHitRatio * 100;
  }

  /**
   * This is the runnable that will be executed on the executor every PERIOD number of seconds
   * It will take metrics/numbers from all of the regions and use them to compute point in
   * time metrics.
   */
  public class RegionServerMetricsWrapperRunnable implements Runnable {

    private long lastRan = 0;

    @Override
    synchronized public void run() {
      try {
        HDFSBlocksDistribution hdfsBlocksDistribution =
            new HDFSBlocksDistribution();
        HDFSBlocksDistribution hdfsBlocksDistributionSecondaryRegions =
            new HDFSBlocksDistribution();

        long tempNumStores = 0, tempNumStoreFiles = 0, tempMemstoreSize = 0, tempStoreFileSize = 0;
        long tempMaxStoreFileAge = 0, tempNumReferenceFiles = 0;
        long avgAgeNumerator = 0, numHFiles = 0;
        long tempMinStoreFileAge = Long.MAX_VALUE;
        long tempReadRequestsCount = 0, tempFilteredReadRequestsCount = 0,
          tempWriteRequestsCount = 0;
        long tempCheckAndMutateChecksFailed = 0;
        long tempCheckAndMutateChecksPassed = 0;
        long tempStorefileIndexSize = 0;
        long tempTotalStaticIndexSize = 0;
        long tempTotalStaticBloomSize = 0;
        long tempNumMutationsWithoutWAL = 0;
        long tempDataInMemoryWithoutWAL = 0;
        double tempPercentFileLocal = 0;
        double tempPercentFileLocalSecondaryRegions = 0;
        long tempFlushedCellsCount = 0;
        long tempCompactedCellsCount = 0;
        long tempMajorCompactedCellsCount = 0;
        long tempFlushedCellsSize = 0;
        long tempCompactedCellsSize = 0;
        long tempMajorCompactedCellsSize = 0;
        long tempCellsCountCompactedToMob = 0;
        long tempCellsCountCompactedFromMob = 0;
        long tempCellsSizeCompactedToMob = 0;
        long tempCellsSizeCompactedFromMob = 0;
        long tempMobFlushCount = 0;
        long tempMobFlushedCellsCount = 0;
        long tempMobFlushedCellsSize = 0;
        long tempMobScanCellsCount = 0;
        long tempMobScanCellsSize = 0;
        long tempBlockedRequestsCount = 0;
        int regionCount = 0;

        long currentReadRequestsCount = 0;
        long currentWriteRequestsCount = 0;
        long lastReadRequestsCount = 0;
        long lastWriteRequestsCount = 0;
        long readRequestsDelta = 0;
        long writeRequestsDelta = 0;
        long totalReadRequestsDelta = 0;
        long totalWriteRequestsDelta = 0;
        String encodedRegionName;
        for (HRegion r : regionServer.getOnlineRegionsLocalContext()) {
          encodedRegionName = r.getRegionInfo().getEncodedName();
          currentReadRequestsCount = r.getReadRequestsCount();
          currentWriteRequestsCount = r.getWriteRequestsCount();
          if (requestsCountCache.containsKey(encodedRegionName)) {
            lastReadRequestsCount = requestsCountCache.get(encodedRegionName).get(0);
            lastWriteRequestsCount = requestsCountCache.get(encodedRegionName).get(1);
            readRequestsDelta = currentReadRequestsCount - lastReadRequestsCount;
            writeRequestsDelta = currentWriteRequestsCount - lastWriteRequestsCount;
            totalReadRequestsDelta += readRequestsDelta;
            totalWriteRequestsDelta += writeRequestsDelta;
            //Update cache for our next comparision
            requestsCountCache.get(encodedRegionName).set(0,currentReadRequestsCount);
            requestsCountCache.get(encodedRegionName).set(1,currentWriteRequestsCount);
          } else {
            // List[0] -> readRequestCount
            // List[1] -> writeRequestCount
            ArrayList<Long> requests = new ArrayList<Long>(2);
            requests.add(currentReadRequestsCount);
            requests.add(currentWriteRequestsCount);
            requestsCountCache.put(encodedRegionName, requests);
            totalReadRequestsDelta += currentReadRequestsCount;
            totalWriteRequestsDelta += currentWriteRequestsCount;
          }
          tempNumMutationsWithoutWAL += r.getNumMutationsWithoutWAL();
          tempDataInMemoryWithoutWAL += r.getDataInMemoryWithoutWAL();
          tempReadRequestsCount += r.getReadRequestsCount();
          tempFilteredReadRequestsCount += r.getFilteredReadRequestsCount();
          tempWriteRequestsCount += r.getWriteRequestsCount();
          tempCheckAndMutateChecksFailed += r.getCheckAndMutateChecksFailed();
          tempCheckAndMutateChecksPassed += r.getCheckAndMutateChecksPassed();
          tempBlockedRequestsCount += r.getBlockedRequestsCount();
          List<? extends Store> storeList = r.getStores();
          tempNumStores += storeList.size();
          for (Store store : storeList) {
            tempNumStoreFiles += store.getStorefilesCount();
            tempMemstoreSize += store.getMemStoreSize().getDataSize();
            tempStoreFileSize += store.getStorefilesSize();

            OptionalLong storeMaxStoreFileAge = store.getMaxStoreFileAge();
            if (storeMaxStoreFileAge.isPresent() &&
                storeMaxStoreFileAge.getAsLong() > tempMaxStoreFileAge) {
              tempMaxStoreFileAge = storeMaxStoreFileAge.getAsLong();
            }

            OptionalLong storeMinStoreFileAge = store.getMinStoreFileAge();
            if (storeMinStoreFileAge.isPresent() &&
                storeMinStoreFileAge.getAsLong() < tempMinStoreFileAge) {
              tempMinStoreFileAge = storeMinStoreFileAge.getAsLong();
            }

            long storeHFiles = store.getNumHFiles();
            numHFiles += storeHFiles;
            tempNumReferenceFiles += store.getNumReferenceFiles();

            OptionalDouble storeAvgStoreFileAge = store.getAvgStoreFileAge();
            if (storeAvgStoreFileAge.isPresent()) {
              avgAgeNumerator =
                  (long) (avgAgeNumerator + storeAvgStoreFileAge.getAsDouble() * storeHFiles);
            }

            tempStorefileIndexSize += store.getStorefilesRootLevelIndexSize();
            tempTotalStaticBloomSize += store.getTotalStaticBloomSize();
            tempTotalStaticIndexSize += store.getTotalStaticIndexSize();
            tempFlushedCellsCount += store.getFlushedCellsCount();
            tempCompactedCellsCount += store.getCompactedCellsCount();
            tempMajorCompactedCellsCount += store.getMajorCompactedCellsCount();
            tempFlushedCellsSize += store.getFlushedCellsSize();
            tempCompactedCellsSize += store.getCompactedCellsSize();
            tempMajorCompactedCellsSize += store.getMajorCompactedCellsSize();
            if (store instanceof HMobStore) {
              HMobStore mobStore = (HMobStore) store;
              tempCellsCountCompactedToMob += mobStore.getCellsCountCompactedToMob();
              tempCellsCountCompactedFromMob += mobStore.getCellsCountCompactedFromMob();
              tempCellsSizeCompactedToMob += mobStore.getCellsSizeCompactedToMob();
              tempCellsSizeCompactedFromMob += mobStore.getCellsSizeCompactedFromMob();
              tempMobFlushCount += mobStore.getMobFlushCount();
              tempMobFlushedCellsCount += mobStore.getMobFlushedCellsCount();
              tempMobFlushedCellsSize += mobStore.getMobFlushedCellsSize();
              tempMobScanCellsCount += mobStore.getMobScanCellsCount();
              tempMobScanCellsSize += mobStore.getMobScanCellsSize();
            }
          }

          HDFSBlocksDistribution distro = r.getHDFSBlocksDistribution();
          hdfsBlocksDistribution.add(distro);
          if (r.getRegionInfo().getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
            hdfsBlocksDistributionSecondaryRegions.add(distro);
          }
          regionCount++;
        }
        float localityIndex = hdfsBlocksDistribution.getBlockLocalityIndex(
            regionServer.getServerName().getHostname());
        tempPercentFileLocal = Double.isNaN(tempBlockedRequestsCount) ? 0 : (localityIndex * 100);

        float localityIndexSecondaryRegions = hdfsBlocksDistributionSecondaryRegions
            .getBlockLocalityIndex(regionServer.getServerName().getHostname());
        tempPercentFileLocalSecondaryRegions = Double.
            isNaN(localityIndexSecondaryRegions) ? 0 : (localityIndexSecondaryRegions * 100);

        // Compute the number of requests per second
        long currentTime = EnvironmentEdgeManager.currentTime();

        // assume that it took PERIOD seconds to start the executor.
        // this is a guess but it's a pretty good one.
        if (lastRan == 0) {
          lastRan = currentTime - period;
        }
        // If we've time traveled keep the last requests per second.
        if ((currentTime - lastRan) > 0) {
          requestsPerSecond = (totalReadRequestsDelta + totalWriteRequestsDelta) /
              ((currentTime - lastRan) / 1000.0);

          double readRequestsRatePerMilliSecond = (double)totalReadRequestsDelta / period;
          double writeRequestsRatePerMilliSecond = (double)totalWriteRequestsDelta / period;

          readRequestsRatePerSecond = readRequestsRatePerMilliSecond * 1000.0;
          writeRequestsRatePerSecond = writeRequestsRatePerMilliSecond * 1000.0;
        }
        lastRan = currentTime;

        final WALProvider provider = regionServer.getWalFactory().getWALProvider();
        final WALProvider metaProvider = regionServer.getWalFactory().getMetaWALProvider();
        numWALFiles = (provider == null ? 0 : provider.getNumLogFiles()) +
            (metaProvider == null ? 0 : metaProvider.getNumLogFiles());
        walFileSize = (provider == null ? 0 : provider.getLogFileSize()) +
          (metaProvider == null ? 0 : metaProvider.getLogFileSize());
        // Copy over computed values so that no thread sees half computed values.
        numStores = tempNumStores;
        numStoreFiles = tempNumStoreFiles;
        memstoreSize = tempMemstoreSize;
        storeFileSize = tempStoreFileSize;
        maxStoreFileAge = tempMaxStoreFileAge;
        if (regionCount > 0) {
          averageRegionSize = (memstoreSize + storeFileSize) / regionCount;
        }
        if (tempMinStoreFileAge != Long.MAX_VALUE) {
          minStoreFileAge = tempMinStoreFileAge;
        }

        if (numHFiles != 0) {
          avgStoreFileAge = avgAgeNumerator / numHFiles;
        }

        numReferenceFiles= tempNumReferenceFiles;
        readRequestsCount = tempReadRequestsCount;
        filteredReadRequestsCount = tempFilteredReadRequestsCount;
        writeRequestsCount = tempWriteRequestsCount;
        checkAndMutateChecksFailed = tempCheckAndMutateChecksFailed;
        checkAndMutateChecksPassed = tempCheckAndMutateChecksPassed;
        storefileIndexSize = tempStorefileIndexSize;
        totalStaticIndexSize = tempTotalStaticIndexSize;
        totalStaticBloomSize = tempTotalStaticBloomSize;
        numMutationsWithoutWAL = tempNumMutationsWithoutWAL;
        dataInMemoryWithoutWAL = tempDataInMemoryWithoutWAL;
        percentFileLocal = tempPercentFileLocal;
        percentFileLocalSecondaryRegions = tempPercentFileLocalSecondaryRegions;
        flushedCellsCount = tempFlushedCellsCount;
        compactedCellsCount = tempCompactedCellsCount;
        majorCompactedCellsCount = tempMajorCompactedCellsCount;
        flushedCellsSize = tempFlushedCellsSize;
        compactedCellsSize = tempCompactedCellsSize;
        majorCompactedCellsSize = tempMajorCompactedCellsSize;
        cellsCountCompactedToMob = tempCellsCountCompactedToMob;
        cellsCountCompactedFromMob = tempCellsCountCompactedFromMob;
        cellsSizeCompactedToMob = tempCellsSizeCompactedToMob;
        cellsSizeCompactedFromMob = tempCellsSizeCompactedFromMob;
        mobFlushCount = tempMobFlushCount;
        mobFlushedCellsCount = tempMobFlushedCellsCount;
        mobFlushedCellsSize = tempMobFlushedCellsSize;
        mobScanCellsCount = tempMobScanCellsCount;
        mobScanCellsSize = tempMobScanCellsSize;
        mobFileCacheAccessCount = mobFileCache != null ? mobFileCache.getAccessCount() : 0L;
        mobFileCacheMissCount = mobFileCache != null ? mobFileCache.getMissCount() : 0L;
        mobFileCacheHitRatio = mobFileCache != null ? mobFileCache.getHitRatio() : 0.0;
        if (Double.isNaN(mobFileCacheHitRatio)) {
          mobFileCacheHitRatio = 0.0;
        }
        mobFileCacheEvictedCount = mobFileCache != null ? mobFileCache.getEvictedFileCount() : 0L;
        mobFileCacheCount = mobFileCache != null ? mobFileCache.getCacheSize() : 0;
        blockedRequestsCount = tempBlockedRequestsCount;
      } catch (Throwable e) {
        LOG.warn("Caught exception! Will suppress and retry.", e);
      }
    }
  }

  @Override
  public long getHedgedReadOps() {
    return this.dfsHedgedReadMetrics == null? 0: this.dfsHedgedReadMetrics.getHedgedReadOps();
  }

  @Override
  public long getHedgedReadWins() {
    return this.dfsHedgedReadMetrics == null? 0: this.dfsHedgedReadMetrics.getHedgedReadWins();
  }

  @Override
  public long getHedgedReadOpsInCurThread() {
    return this.dfsHedgedReadMetrics == null ? 0 : this.dfsHedgedReadMetrics.getHedgedReadOpsInCurThread();
  }

  @Override
  public long getTotalBytesRead() {
    return FSDataInputStreamWrapper.getTotalBytesRead();
  }

  @Override
  public long getLocalBytesRead() {
    return FSDataInputStreamWrapper.getLocalBytesRead();
  }

  @Override
  public long getShortCircuitBytesRead() {
    return FSDataInputStreamWrapper.getShortCircuitBytesRead();
  }

  @Override
  public long getZeroCopyBytesRead() {
    return FSDataInputStreamWrapper.getZeroCopyBytesRead();
  }

  @Override
  public long getBlockedRequestsCount() {
    return blockedRequestsCount;
  }

  @Override
  public long getAverageRegionSize() {
    return averageRegionSize;
  }

  @Override
  public long getDataMissCount() {
    return this.cacheStats != null ? this.cacheStats.getDataMissCount() : 0L;
  }

  @Override
  public long getLeafIndexMissCount() {
    return this.cacheStats != null ? this.cacheStats.getLeafIndexMissCount() : 0L;
  }

  @Override
  public long getBloomChunkMissCount() {
    return this.cacheStats != null ? this.cacheStats.getBloomChunkMissCount() : 0L;
  }

  @Override
  public long getMetaMissCount() {
    return this.cacheStats != null ? this.cacheStats.getMetaMissCount() : 0L;
  }

  @Override
  public long getRootIndexMissCount() {
    return this.cacheStats != null ? this.cacheStats.getRootIndexMissCount() : 0L;
  }

  @Override
  public long getIntermediateIndexMissCount() {
    return this.cacheStats != null ? this.cacheStats.getIntermediateIndexMissCount() : 0L;
  }

  @Override
  public long getFileInfoMissCount() {
    return this.cacheStats != null ? this.cacheStats.getFileInfoMissCount() : 0L;
  }

  @Override
  public long getGeneralBloomMetaMissCount() {
    return this.cacheStats != null ? this.cacheStats.getGeneralBloomMetaMissCount() : 0L;
  }

  @Override
  public long getDeleteFamilyBloomMissCount() {
    return this.cacheStats != null ? this.cacheStats.getDeleteFamilyBloomMissCount() : 0L;
  }

  @Override
  public long getTrailerMissCount() {
    return this.cacheStats != null ? this.cacheStats.getTrailerMissCount() : 0L;
  }

  @Override
  public long getDataHitCount() {
    return this.cacheStats != null ? this.cacheStats.getDataHitCount() : 0L;
  }

  @Override
  public long getLeafIndexHitCount() {
    return this.cacheStats != null ? this.cacheStats.getLeafIndexHitCount() : 0L;
  }

  @Override
  public long getBloomChunkHitCount() {
    return this.cacheStats != null ? this.cacheStats.getBloomChunkHitCount() : 0L;
  }

  @Override
  public long getMetaHitCount() {
    return this.cacheStats != null ? this.cacheStats.getMetaHitCount() : 0L;
  }

  @Override
  public long getRootIndexHitCount() {
    return this.cacheStats != null ? this.cacheStats.getRootIndexHitCount() : 0L;
  }

  @Override
  public long getIntermediateIndexHitCount() {
    return this.cacheStats != null ? this.cacheStats.getIntermediateIndexHitCount() : 0L;
  }

  @Override
  public long getFileInfoHitCount() {
    return this.cacheStats != null ? this.cacheStats.getFileInfoHitCount() : 0L;
  }

  @Override
  public long getGeneralBloomMetaHitCount() {
    return this.cacheStats != null ? this.cacheStats.getGeneralBloomMetaHitCount() : 0L;
  }

  @Override
  public long getDeleteFamilyBloomHitCount() {
    return this.cacheStats != null ? this.cacheStats.getDeleteFamilyBloomHitCount() : 0L;
  }

  @Override
  public long getTrailerHitCount() {
    return this.cacheStats != null ? this.cacheStats.getTrailerHitCount() : 0L;
  }

  @Override
  public long getByteBuffAllocatorHeapAllocationBytes() {
    return ByteBuffAllocator.getHeapAllocationBytes(allocator, ByteBuffAllocator.HEAP);
  }

  @Override
  public long getByteBuffAllocatorPoolAllocationBytes() {
    return this.allocator.getPoolAllocationBytes();
  }

  @Override
  public double getByteBuffAllocatorHeapAllocRatio() {
    return ByteBuffAllocator.getHeapAllocationRatio(allocator, ByteBuffAllocator.HEAP);
  }

  @Override
  public long getByteBuffAllocatorTotalBufferCount() {
    return this.allocator.getTotalBufferCount();
  }

  @Override
  public long getByteBuffAllocatorUsedBufferCount() {
    return this.allocator.getUsedBufferCount();
  }
}
