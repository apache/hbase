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

public class MetricsRegionServerWrapperStub implements MetricsRegionServerWrapper {

  @Override
  public String getServerName() {
    return "test";
  }

  @Override
  public String getClusterId() {
    return "tClusterId";
  }

  @Override
  public String getZookeeperQuorum() {
    return "zk";
  }

  @Override
  public String getCoprocessors() {
    return "co-process";
  }

  @Override
  public long getStartCode() {
    return 100;
  }

  @Override
  public long getNumOnlineRegions() {
    return 101;
  }

  @Override
  public long getNumStores() {
    return 2;
  }

  @Override
  public long getNumStoreFiles() {
    return 300;
  }

  @Override
  public long getMemStoreSize() {
    return 1025;
  }

  @Override
  public long getStoreFileSize() {
    return 1900;
  }

  @Override
  public double getStoreFileSizeGrowthRate() {
    return 50.0;
  }

  @Override
  public long getMaxStoreFileAge() {
    return 2;
  }

  @Override
  public long getMinStoreFileAge() {
    return 2;
  }

  @Override
  public long getAvgStoreFileAge() {
    return 2;
  }

  @Override
  public long getNumReferenceFiles() {
    return 2;
  }

  @Override
  public double getRequestsPerSecond() {
    return 0;
  }

  @Override
  public long getTotalRequestCount() {
    return 899;
  }

  @Override
  public double getReadRequestsRatePerSecond() {
    return 10.0;
  }

  @Override
  public long getTotalRowActionRequestCount() {
    return getReadRequestsCount() + getWriteRequestsCount();
  }

  @Override
  public long getReadRequestsCount() {
    return 997;
  }

  @Override
  public long getCpRequestsCount() {
    return 998;
  }

  @Override
  public long getFilteredReadRequestsCount() {
    return 1997;
  }

  @Override
  public long getWriteRequestsCount() {
    return 707;
  }

  @Override
  public double getWriteRequestsRatePerSecond() {
    return 10.0;
  }

  @Override
  public long getRpcGetRequestsCount() {
    return 521;
  }

  @Override
  public long getRpcScanRequestsCount() {
    return 101;
  }

  @Override
  public long getRpcMultiRequestsCount() {
    return 486;
  }

  @Override
  public long getRpcMutateRequestsCount() {
    return 606;
  }

  @Override
  public long getCheckAndMutateChecksFailed() {
    return 401;
  }

  @Override
  public long getCheckAndMutateChecksPassed() {
    return 405;
  }

  @Override
  public long getStoreFileIndexSize() {
    return 406;
  }

  @Override
  public long getTotalStaticIndexSize() {
    return 407;
  }

  @Override
  public long getTotalStaticBloomSize() {
    return 408;
  }

  @Override
  public long getNumMutationsWithoutWAL() {
    return 409;
  }

  @Override
  public long getDataInMemoryWithoutWAL() {
    return 410;
  }

  @Override
  public double getPercentFileLocal() {
    return 99;
  }

  @Override
  public double getPercentFileLocalSecondaryRegions() {
    return 99;
  }

  @Override
  public int getCompactionQueueSize() {
    return 411;
  }

  @Override
  public int getSmallCompactionQueueSize() {
    return 0;
  }

  @Override
  public int getLargeCompactionQueueSize() {
    return 0;
  }

  @Override
  public int getFlushQueueSize() {
    return 412;
  }

  @Override
  public long getMemStoreLimit() {
    return 419;
  }

  @Override
  public long getBlockCacheFreeSize() {
    return 413;
  }

  @Override
  public long getBlockCacheCount() {
    return 414;
  }

  @Override
  public long getBlockCacheSize() {
    return 415;
  }

  @Override
  public long getBlockCacheHitCount() {
    return 416;
  }

  @Override
  public long getBlockCachePrimaryHitCount() {
    return 422;
  }

  @Override
  public long getBlockCacheMissCount() {
    return 417;
  }

  @Override
  public long getBlockCachePrimaryMissCount() {
    return 421;
  }

  @Override
  public long getBlockCacheEvictedCount() {
    return 418;
  }

  @Override
   public long getBlockCachePrimaryEvictedCount() {
    return 420;
  }

  @Override
  public double getBlockCacheHitPercent() {
    return 98;
  }

  @Override
  public double getBlockCacheHitCachingPercent() {
    return 97;
  }

  @Override
  public long getBlockCacheFailedInsertions() {
    return 36;
  }

  @Override
  public long getL1CacheHitCount() {
    return 200;
  }

  @Override
  public long getL1CacheMissCount() {
    return 100;
  }

  @Override
  public double getL1CacheHitRatio() {
    return 80;
  }

  @Override
  public double getL1CacheMissRatio() {
    return 20;
  }

  @Override
  public long getL2CacheHitCount() {
    return 800;
  }

  @Override
  public long getL2CacheMissCount() {
    return 200;
  }

  @Override
  public double getL2CacheHitRatio() {
    return 90;
  }

  @Override
  public double getL2CacheMissRatio() {
    return 10;
  }

  @Override
  public long getUpdatesBlockedTime() {
    return 419;
  }

  @Override
  public void forceRecompute() {
    //IGNORED.
  }

  @Override
  public long getNumWALFiles() {
    return 10;
  }

  @Override
  public long getWALFileSize() {
    return 1024000;
  }

  @Override
  public long getNumWALSlowAppend() {
    return 0;
  }

  @Override
  public long getFlushedCellsCount() {
    return 100000000;
  }

  @Override
  public long getCompactedCellsCount() {
    return 10000000;
  }

  @Override
  public long getMajorCompactedCellsCount() {
    return 1000000;
  }

  @Override
  public long getFlushedCellsSize() {
    return 1024000000;
  }

  @Override
  public long getCompactedCellsSize() {
    return 102400000;
  }

  @Override
  public long getMajorCompactedCellsSize() {
    return 10240000;
  }

  @Override
  public long getHedgedReadOps() {
    return 100;
  }

  @Override
  public long getHedgedReadWins() {
    return 10;
  }

  @Override
  public long getBlockedRequestsCount() {
    return 0;
  }

  @Override
  public long getDataMissCount() {
    return 0;
  }

  @Override
  public long getLeafIndexMissCount() {
    return 0;
  }

  @Override
  public long getBloomChunkMissCount() {
    return 0;
  }

  @Override
  public long getMetaMissCount() {
    return 0;
  }

  @Override
  public long getRootIndexMissCount() {
    return 0;
  }

  @Override
  public long getIntermediateIndexMissCount() {
    return 0;
  }

  @Override
  public long getFileInfoMissCount() {
    return 0;
  }

  @Override
  public long getGeneralBloomMetaMissCount() {
    return 0;
  }

  @Override
  public long getDeleteFamilyBloomMissCount() {
    return 0;
  }

  @Override
  public long getTrailerMissCount() {
    return 0;
  }

  @Override
  public long getDataHitCount() {
    return 0;
  }

  @Override
  public long getLeafIndexHitCount() {
    return 0;
  }

  @Override
  public long getBloomChunkHitCount() {
    return 0;
  }

  @Override
  public long getMetaHitCount() {
    return 0;
  }

  @Override
  public long getRootIndexHitCount() {
    return 0;
  }

  @Override
  public long getIntermediateIndexHitCount() {
    return 0;
  }

  @Override
  public long getFileInfoHitCount() {
    return 0;
  }

  @Override
  public long getGeneralBloomMetaHitCount() {
    return 0;
  }

  @Override
  public long getDeleteFamilyBloomHitCount() {
    return 0;
  }

  @Override
  public long getTrailerHitCount() {
    return 0;
  }

  @Override
  public int getSplitQueueSize() {
    return 0;
  }

  @Override
  public long getCellsCountCompactedToMob() {
    return 20;
  }

  @Override
  public long getCellsCountCompactedFromMob() {
    return 10;
  }

  @Override
  public long getCellsSizeCompactedToMob() {
    return 200;
  }

  @Override
  public long getCellsSizeCompactedFromMob() {
    return 100;
  }

  @Override
  public long getMobFlushCount() {
    return 1;
  }

  @Override
  public long getMobFlushedCellsCount() {
    return 10;
  }

  @Override
  public long getMobFlushedCellsSize() {
    return 1000;
  }

  @Override
  public long getMobScanCellsCount() {
    return 10;
  }

  @Override
  public long getMobScanCellsSize() {
    return 1000;
  }

  @Override
  public long getMobFileCacheAccessCount() {
    return 100;
  }

  @Override
  public long getMobFileCacheMissCount() {
    return 50;
  }

  @Override
  public long getMobFileCacheEvictedCount() {
    return 0;
  }

  @Override
  public long getMobFileCacheCount() {
    return 100;
  }

  @Override
  public double getMobFileCacheHitPercent() {
    return 50;
  }

  @Override
  public long getAverageRegionSize() {
    return 10000000;
  }
}
