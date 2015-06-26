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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.wal.BoundedRegionGroupingProvider;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.metrics2.MetricsExecutor;

/**
 * Impl for exposing HRegionServer Information through Hadoop's metrics 2 system.
 */
@InterfaceAudience.Private
class MetricsRegionServerWrapperImpl
    implements MetricsRegionServerWrapper {

  private static final Log LOG = LogFactory.getLog(MetricsRegionServerWrapperImpl.class);

  private final HRegionServer regionServer;

  private BlockCache blockCache;

  private volatile long numStores = 0;
  private volatile long numWALFiles = 0;
  private volatile long walFileSize = 0;
  private volatile long numStoreFiles = 0;
  private volatile long memstoreSize = 0;
  private volatile long storeFileSize = 0;
  private volatile double requestsPerSecond = 0.0;
  private volatile long readRequestsCount = 0;
  private volatile long writeRequestsCount = 0;
  private volatile long checkAndMutateChecksFailed = 0;
  private volatile long checkAndMutateChecksPassed = 0;
  private volatile long storefileIndexSize = 0;
  private volatile long totalStaticIndexSize = 0;
  private volatile long totalStaticBloomSize = 0;
  private volatile long numMutationsWithoutWAL = 0;
  private volatile long dataInMemoryWithoutWAL = 0;
  private volatile int percentFileLocal = 0;
  private volatile int percentFileLocalSecondaryRegions = 0;
  private volatile long flushedCellsCount = 0;
  private volatile long compactedCellsCount = 0;
  private volatile long majorCompactedCellsCount = 0;
  private volatile long flushedCellsSize = 0;
  private volatile long compactedCellsSize = 0;
  private volatile long majorCompactedCellsSize = 0;
  private volatile long blockedRequestsCount = 0L;

  private CacheStats cacheStats;
  private ScheduledExecutorService executor;
  private Runnable runnable;
  private long period;

  public MetricsRegionServerWrapperImpl(final HRegionServer regionServer) {
    this.regionServer = regionServer;
    initBlockCache();

    this.period =
        regionServer.conf.getLong(HConstants.REGIONSERVER_METRICS_PERIOD,
          HConstants.DEFAULT_REGIONSERVER_METRICS_PERIOD);

    this.executor = CompatibilitySingletonFactory.getInstance(MetricsExecutor.class).getExecutor();
    this.runnable = new RegionServerMetricsWrapperRunnable();
    this.executor.scheduleWithFixedDelay(this.runnable, this.period, this.period,
      TimeUnit.MILLISECONDS);

    if (LOG.isInfoEnabled()) {
      LOG.info("Computing regionserver metrics every " + this.period + " milliseconds");
    }
  }

  /**
   * It's possible that due to threading the block cache could not be initialized
   * yet (testing multiple region servers in one jvm).  So we need to try and initialize
   * the blockCache and cacheStats reference multiple times until we succeed.
   */
  private synchronized  void initBlockCache() {
    CacheConfig cacheConfig = this.regionServer.cacheConfig;
    if (cacheConfig != null && this.blockCache == null) {
      this.blockCache = cacheConfig.getBlockCache();
    }

    if (this.blockCache != null && this.cacheStats == null) {
      this.cacheStats = blockCache.getStats();
    }
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
    ZooKeeperWatcher zk = regionServer.getZooKeeper();
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
    Collection<Region> onlineRegionsLocalContext = regionServer.getOnlineRegionsLocalContext();
    if (onlineRegionsLocalContext == null) {
      return 0;
    }
    return onlineRegionsLocalContext.size();
  }

  @Override
  public long getTotalRequestCount() {
    return regionServer.rpcServices.requestCount.get();
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
    if (this.blockCache == null) {
      return 0;
    }
    return this.blockCache.getBlockCount();
  }

  @Override
  public long getBlockCacheSize() {
    if (this.blockCache == null) {
      return 0;
    }
    return this.blockCache.getCurrentSize();
  }

  @Override
  public long getBlockCacheFreeSize() {
    if (this.blockCache == null) {
      return 0;
    }
    return this.blockCache.getFreeSize();
  }

  @Override
  public long getBlockCacheHitCount() {
    if (this.cacheStats == null) {
      return 0;
    }
    return this.cacheStats.getHitCount();
  }

  @Override
  public long getBlockCacheMissCount() {
    if (this.cacheStats == null) {
      return 0;
    }
    return this.cacheStats.getMissCount();
  }

  @Override
  public long getBlockCacheEvictedCount() {
    if (this.cacheStats == null) {
      return 0;
    }
    return this.cacheStats.getEvictedCount();
  }

  @Override
  public double getBlockCacheHitPercent() {
    if (this.cacheStats == null) {
      return 0;
    }
    return (int) (this.cacheStats.getHitRatio() * 100);
  }

  @Override
  public int getBlockCacheHitCachingPercent() {
    if (this.cacheStats == null) {
      return 0;
    }
    return (int) (this.cacheStats.getHitCachingRatio() * 100);
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

  @Override public double getRequestsPerSecond() {
    return requestsPerSecond;
  }

  @Override
  public long getReadRequestsCount() {
    return readRequestsCount;
  }

  @Override
  public long getWriteRequestsCount() {
    return writeRequestsCount;
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
  public int getPercentFileLocal() {
    return percentFileLocal;
  }

  @Override
  public int getPercentFileLocalSecondaryRegions() {
    return percentFileLocalSecondaryRegions;
  }

  @Override
  public long getUpdatesBlockedTime() {
    if (this.regionServer.cacheFlusher == null) {
      return 0;
    }
    return this.regionServer.cacheFlusher.getUpdatesBlockedMsHighWater().get();
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

  /**
   * This is the runnable that will be executed on the executor every PERIOD number of seconds
   * It will take metrics/numbers from all of the regions and use them to compute point in
   * time metrics.
   */
  public class RegionServerMetricsWrapperRunnable implements Runnable {

    private long lastRan = 0;
    private long lastRequestCount = 0;

    @Override
    synchronized public void run() {
      initBlockCache();
      cacheStats = blockCache.getStats();

      HDFSBlocksDistribution hdfsBlocksDistribution =
          new HDFSBlocksDistribution();
      HDFSBlocksDistribution hdfsBlocksDistributionSecondaryRegions =
          new HDFSBlocksDistribution();

      long tempNumStores = 0;
      long tempNumStoreFiles = 0;
      long tempMemstoreSize = 0;
      long tempStoreFileSize = 0;
      long tempReadRequestsCount = 0;
      long tempWriteRequestsCount = 0;
      long tempCheckAndMutateChecksFailed = 0;
      long tempCheckAndMutateChecksPassed = 0;
      long tempStorefileIndexSize = 0;
      long tempTotalStaticIndexSize = 0;
      long tempTotalStaticBloomSize = 0;
      long tempNumMutationsWithoutWAL = 0;
      long tempDataInMemoryWithoutWAL = 0;
      int tempPercentFileLocal = 0;
      int tempPercentFileLocalSecondaryRegions = 0;
      long tempFlushedCellsCount = 0;
      long tempCompactedCellsCount = 0;
      long tempMajorCompactedCellsCount = 0;
      long tempFlushedCellsSize = 0;
      long tempCompactedCellsSize = 0;
      long tempMajorCompactedCellsSize = 0;
      long tempBlockedRequestsCount = 0L;

      for (Region r : regionServer.getOnlineRegionsLocalContext()) {
        tempNumMutationsWithoutWAL += r.getNumMutationsWithoutWAL();
        tempDataInMemoryWithoutWAL += r.getDataInMemoryWithoutWAL();
        tempReadRequestsCount += r.getReadRequestsCount();
        tempWriteRequestsCount += r.getWriteRequestsCount();
        tempCheckAndMutateChecksFailed += r.getCheckAndMutateChecksFailed();
        tempCheckAndMutateChecksPassed += r.getCheckAndMutateChecksPassed();
        tempBlockedRequestsCount += r.getBlockedRequestsCount();
        List<Store> storeList = r.getStores();
        tempNumStores += storeList.size();
        for (Store store : storeList) {
          tempNumStoreFiles += store.getStorefilesCount();
          tempMemstoreSize += store.getMemStoreSize();
          tempStoreFileSize += store.getStorefilesSize();
          tempStorefileIndexSize += store.getStorefilesIndexSize();
          tempTotalStaticBloomSize += store.getTotalStaticBloomSize();
          tempTotalStaticIndexSize += store.getTotalStaticIndexSize();
          tempFlushedCellsCount += store.getFlushedCellsCount();
          tempCompactedCellsCount += store.getCompactedCellsCount();
          tempMajorCompactedCellsCount += store.getMajorCompactedCellsCount();
          tempFlushedCellsSize += store.getFlushedCellsSize();
          tempCompactedCellsSize += store.getCompactedCellsSize();
          tempMajorCompactedCellsSize += store.getMajorCompactedCellsSize();
        }

        HDFSBlocksDistribution distro = r.getHDFSBlocksDistribution();
        hdfsBlocksDistribution.add(distro);
        if (r.getRegionInfo().getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
          hdfsBlocksDistributionSecondaryRegions.add(distro);
        }
      }

      float localityIndex = hdfsBlocksDistribution.getBlockLocalityIndex(
          regionServer.getServerName().getHostname());
      tempPercentFileLocal = (int) (localityIndex * 100);

      float localityIndexSecondaryRegions = hdfsBlocksDistributionSecondaryRegions
          .getBlockLocalityIndex(regionServer.getServerName().getHostname());
      tempPercentFileLocalSecondaryRegions = (int) (localityIndexSecondaryRegions * 100);

      //Compute the number of requests per second
      long currentTime = EnvironmentEdgeManager.currentTime();

      // assume that it took PERIOD seconds to start the executor.
      // this is a guess but it's a pretty good one.
      if (lastRan == 0) {
        lastRan = currentTime - period;
      }


      //If we've time traveled keep the last requests per second.
      if ((currentTime - lastRan) > 0) {
        long currentRequestCount = getTotalRequestCount();
        requestsPerSecond = (currentRequestCount - lastRequestCount) /
            ((currentTime - lastRan) / 1000.0);
        lastRequestCount = currentRequestCount;
      }
      lastRan = currentTime;

      numWALFiles = DefaultWALProvider.getNumLogFiles(regionServer.walFactory) +
          BoundedRegionGroupingProvider.getNumLogFiles(regionServer.walFactory);
      walFileSize = DefaultWALProvider.getLogFileSize(regionServer.walFactory) +
          BoundedRegionGroupingProvider.getLogFileSize(regionServer.walFactory);
      //Copy over computed values so that no thread sees half computed values.
      numStores = tempNumStores;
      numStoreFiles = tempNumStoreFiles;
      memstoreSize = tempMemstoreSize;
      storeFileSize = tempStoreFileSize;
      readRequestsCount = tempReadRequestsCount;
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
      blockedRequestsCount = tempBlockedRequestsCount;
    }
  }

  @Override
  public long getBlockedRequestsCount() {
    return blockedRequestsCount;
  }
}
