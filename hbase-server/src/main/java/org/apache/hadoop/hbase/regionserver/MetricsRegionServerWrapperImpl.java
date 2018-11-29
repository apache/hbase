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
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
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

  private Optional<BlockCache> blockCache;
  private Optional<MobFileCache> mobFileCache;
  private Optional<CacheStats> cacheStats;
  private Optional<CacheStats> l1Stats = Optional.empty();
  private Optional<CacheStats> l2Stats = Optional.empty();

  private volatile long numStores = 0;
  private volatile long numWALFiles = 0;
  private volatile long walFileSize = 0;
  private volatile long numStoreFiles = 0;
  private volatile long memstoreSize = 0;
  private volatile long storeFileSize = 0;
  private volatile double storeFileSizeGrowthRate = 0;
  private volatile long maxStoreFileAge = 0;
  private volatile long minStoreFileAge = 0;
  private volatile long avgStoreFileAge = 0;
  private volatile long numReferenceFiles = 0;
  private volatile double requestsPerSecond = 0.0;
  private volatile long readRequestsCount = 0;
  private volatile double readRequestsRatePerSecond = 0;
  private volatile long cpRequestsCount = 0;
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

    this.period =
        regionServer.conf.getLong(HConstants.REGIONSERVER_METRICS_PERIOD,
          HConstants.DEFAULT_REGIONSERVER_METRICS_PERIOD);

    this.executor = CompatibilitySingletonFactory.getInstance(MetricsExecutor.class).getExecutor();
    this.runnable = new RegionServerMetricsWrapperRunnable();
    this.executor.scheduleWithFixedDelay(this.runnable, this.period, this.period,
      TimeUnit.MILLISECONDS);
    this.metricsWALSource = CompatibilitySingletonFactory.getInstance(MetricsWALSource.class);

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
    this.blockCache = this.regionServer.getBlockCache();
    this.cacheStats = this.blockCache.map(BlockCache::getStats);
    if (this.cacheStats.isPresent()) {
      if (this.cacheStats.get() instanceof CombinedBlockCache.CombinedCacheStats) {
        l1Stats = Optional
            .of(((CombinedBlockCache.CombinedCacheStats) this.cacheStats.get()).getLruCacheStats());
        l2Stats = Optional.of(((CombinedBlockCache.CombinedCacheStats) this.cacheStats.get())
            .getBucketCacheStats());
      } else {
        l1Stats = this.cacheStats;
      }
    }
  }

  /**
   * Initializes the mob file cache.
   */
  private void initMobFileCache() {
    this.mobFileCache = this.regionServer.getMobFileCache();
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
    if (this.regionServer.cacheFlusher == null) {
      return 0;
    }
    return this.regionServer.cacheFlusher.getFlushQueueSize();
  }

  @Override
  public long getBlockCacheCount() {
    return this.blockCache.map(BlockCache::getBlockCount).orElse(0L);
  }

  @Override
  public long getMemStoreLimit() {
    return this.regionServer.getRegionServerAccounting().getGlobalMemStoreLimit();
  }

  @Override
  public long getBlockCacheSize() {
    return this.blockCache.map(BlockCache::getCurrentSize).orElse(0L);
  }

  @Override
  public long getBlockCacheFreeSize() {
    return this.blockCache.map(BlockCache::getFreeSize).orElse(0L);
  }

  @Override
  public long getBlockCacheHitCount() {
    return this.cacheStats.map(CacheStats::getHitCount).orElse(0L);
  }

  @Override
  public long getBlockCachePrimaryHitCount() {
    return this.cacheStats.map(CacheStats::getPrimaryHitCount).orElse(0L);
  }

  @Override
  public long getBlockCacheMissCount() {
    return this.cacheStats.map(CacheStats::getMissCount).orElse(0L);
  }

  @Override
  public long getBlockCachePrimaryMissCount() {
    return this.cacheStats.map(CacheStats::getPrimaryMissCount).orElse(0L);
  }

  @Override
  public long getBlockCacheEvictedCount() {
    return this.cacheStats.map(CacheStats::getEvictedCount).orElse(0L);
  }

  @Override
  public long getBlockCachePrimaryEvictedCount() {
    return this.cacheStats.map(CacheStats::getPrimaryEvictedCount).orElse(0L);
  }

  @Override
  public double getBlockCacheHitPercent() {
    double ratio = this.cacheStats.map(CacheStats::getHitRatio).orElse(0.0);
    if (Double.isNaN(ratio)) {
      ratio = 0;
    }
    return (ratio * 100);
  }

  @Override
  public double getBlockCacheHitCachingPercent() {
    double ratio = this.cacheStats.map(CacheStats::getHitCachingRatio).orElse(0.0);
    if (Double.isNaN(ratio)) {
      ratio = 0;
    }
    return (ratio * 100);
  }

  @Override
  public long getBlockCacheFailedInsertions() {
    return this.cacheStats.map(CacheStats::getFailedInserts).orElse(0L);
  }

  @Override
  public long getL1CacheHitCount() {
    return this.l1Stats.map(CacheStats::getHitCount).orElse(0L);
  }

  @Override
  public long getL1CacheMissCount() {
    return this.l1Stats.map(CacheStats::getMissCount).orElse(0L);
  }

  @Override
  public double getL1CacheHitRatio() {
    return this.l1Stats.map(CacheStats::getHitRatio).orElse(0.0);
  }

  @Override
  public double getL1CacheMissRatio() {
    return this.l1Stats.map(CacheStats::getMissRatio).orElse(0.0);
  }

  @Override
  public long getL2CacheHitCount() {
    return this.l2Stats.map(CacheStats::getHitCount).orElse(0L);
  }

  @Override
  public long getL2CacheMissCount() {
    return this.l2Stats.map(CacheStats::getMissCount).orElse(0L);
  }

  @Override
  public double getL2CacheHitRatio() {
    return this.l2Stats.map(CacheStats::getHitRatio).orElse(0.0);
  }

  @Override
  public double getL2CacheMissRatio() {
    return this.l2Stats.map(CacheStats::getMissRatio).orElse(0.0);
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

  @Override
  public double getStoreFileSizeGrowthRate() {
    return storeFileSizeGrowthRate;
  }

  @Override public double getRequestsPerSecond() {
    return requestsPerSecond;
  }

  @Override
  public long getReadRequestsCount() {
    return readRequestsCount;
  }

  @Override
  public long getCpRequestsCount() {
    return cpRequestsCount;
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
    if (this.regionServer.cacheFlusher == null) {
      return 0;
    }
    return this.regionServer.cacheFlusher.getUpdatesBlockedMsHighWater().sum();
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
    private long lastRequestCount = 0;
    private long lastReadRequestsCount = 0;
    private long lastWriteRequestsCount = 0;
    private long lastStoreFileSize = 0;

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
          tempWriteRequestsCount = 0, tempCpRequestsCount = 0;
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
        for (HRegion r : regionServer.getOnlineRegionsLocalContext()) {
          tempNumMutationsWithoutWAL += r.getNumMutationsWithoutWAL();
          tempDataInMemoryWithoutWAL += r.getDataInMemoryWithoutWAL();
          tempReadRequestsCount += r.getReadRequestsCount();
          tempCpRequestsCount += r.getCpRequestsCount();
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
          long currentRequestCount = getTotalRequestCount();
          requestsPerSecond = (currentRequestCount - lastRequestCount) /
              ((currentTime - lastRan) / 1000.0);
          lastRequestCount = currentRequestCount;

          long intervalReadRequestsCount = tempReadRequestsCount - lastReadRequestsCount;
          long intervalWriteRequestsCount = tempWriteRequestsCount - lastWriteRequestsCount;

          double readRequestsRatePerMilliSecond = (double)intervalReadRequestsCount / period;
          double writeRequestsRatePerMilliSecond = (double)intervalWriteRequestsCount / period;

          readRequestsRatePerSecond = readRequestsRatePerMilliSecond * 1000.0;
          writeRequestsRatePerSecond = writeRequestsRatePerMilliSecond * 1000.0;

          long intervalStoreFileSize = tempStoreFileSize - lastStoreFileSize;
          storeFileSizeGrowthRate = (double)intervalStoreFileSize * 1000.0 / period;

          lastReadRequestsCount = tempReadRequestsCount;
          lastWriteRequestsCount = tempWriteRequestsCount;
          lastStoreFileSize = tempStoreFileSize;
        }

        lastRan = currentTime;

        WALProvider provider = regionServer.walFactory.getWALProvider();
        WALProvider metaProvider = regionServer.walFactory.getMetaWALProvider();
        numWALFiles = (provider == null ? 0 : provider.getNumLogFiles()) +
            (metaProvider == null ? 0 : metaProvider.getNumLogFiles());
        walFileSize = (provider == null ? 0 : provider.getLogFileSize()) +
            (provider == null ? 0 : provider.getLogFileSize());
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
        cpRequestsCount = tempCpRequestsCount;
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
        mobFileCacheAccessCount = mobFileCache.map(MobFileCache::getAccessCount).orElse(0L);
        mobFileCacheMissCount = mobFileCache.map(MobFileCache::getMissCount).orElse(0L);
        mobFileCacheHitRatio = mobFileCache.map(MobFileCache::getHitRatio).orElse(0.0);
        if (Double.isNaN(mobFileCacheHitRatio)) {
          mobFileCacheHitRatio = 0.0;
        }
        mobFileCacheEvictedCount = mobFileCache.map(MobFileCache::getEvictedFileCount).orElse(0L);
        mobFileCacheCount = mobFileCache.map(MobFileCache::getCacheSize).orElse(0);
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
  public long getBlockedRequestsCount() {
    return blockedRequestsCount;
  }

  @Override
  public long getAverageRegionSize() {
    return averageRegionSize;
  }

  @Override
  public long getDataMissCount() {
    return this.cacheStats.map(CacheStats::getDataMissCount).orElse(0L);
  }

  @Override
  public long getLeafIndexMissCount() {
    return this.cacheStats.map(CacheStats::getLeafIndexMissCount).orElse(0L);
  }

  @Override
  public long getBloomChunkMissCount() {
    return this.cacheStats.map(CacheStats::getBloomChunkMissCount).orElse(0L);
  }

  @Override
  public long getMetaMissCount() {
    return this.cacheStats.map(CacheStats::getMetaMissCount).orElse(0L);
  }

  @Override
  public long getRootIndexMissCount() {
    return this.cacheStats.map(CacheStats::getRootIndexMissCount).orElse(0L);
  }

  @Override
  public long getIntermediateIndexMissCount() {
    return this.cacheStats.map(CacheStats::getIntermediateIndexMissCount).orElse(0L);
  }

  @Override
  public long getFileInfoMissCount() {
    return this.cacheStats.map(CacheStats::getFileInfoMissCount).orElse(0L);
  }

  @Override
  public long getGeneralBloomMetaMissCount() {
    return this.cacheStats.map(CacheStats::getGeneralBloomMetaMissCount).orElse(0L);
  }

  @Override
  public long getDeleteFamilyBloomMissCount() {
    return this.cacheStats.map(CacheStats::getDeleteFamilyBloomMissCount).orElse(0L);
  }

  @Override
  public long getTrailerMissCount() {
    return this.cacheStats.map(CacheStats::getTrailerMissCount).orElse(0L);
  }

  @Override
  public long getDataHitCount() {
    return this.cacheStats.map(CacheStats::getDataHitCount).orElse(0L);
  }

  @Override
  public long getLeafIndexHitCount() {
    return this.cacheStats.map(CacheStats::getLeafIndexHitCount).orElse(0L);
  }

  @Override
  public long getBloomChunkHitCount() {
    return this.cacheStats.map(CacheStats::getBloomChunkHitCount).orElse(0L);
  }

  @Override
  public long getMetaHitCount() {
    return this.cacheStats.map(CacheStats::getMetaHitCount).orElse(0L);
  }

  @Override
  public long getRootIndexHitCount() {
    return this.cacheStats.map(CacheStats::getRootIndexHitCount).orElse(0L);
  }

  @Override
  public long getIntermediateIndexHitCount() {
    return this.cacheStats.map(CacheStats::getIntermediateIndexHitCount).orElse(0L);
  }

  @Override
  public long getFileInfoHitCount() {
    return this.cacheStats.map(CacheStats::getFileInfoHitCount).orElse(0L);
  }

  @Override
  public long getGeneralBloomMetaHitCount() {
    return this.cacheStats.map(CacheStats::getGeneralBloomMetaHitCount).orElse(0L);
  }

  @Override
  public long getDeleteFamilyBloomHitCount() {
    return this.cacheStats.map(CacheStats::getDeleteFamilyBloomHitCount).orElse(0L);
  }

  @Override
  public long getTrailerHitCount() {
    return this.cacheStats.map(CacheStats::getTrailerHitCount).orElse(0L);
  }
}
