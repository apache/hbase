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

/**
 * This is the interface that will expose RegionServer information to hadoop1/hadoop2
 * implementations of the MetricsRegionServerSource.
 */
public interface MetricsRegionServerWrapper {

  /**
   * Get ServerName
   */
  String getServerName();

  /**
   * Get the Cluster ID
   *
   * @return Cluster ID
   */
  String getClusterId();

  /**
   * Get the Zookeeper Quorum Info
   *
   * @return Zookeeper Quorum Info
   */
  String getZookeeperQuorum();

  /**
   * Get the co-processors
   *
   * @return Co-processors
   */
  String getCoprocessors();

  /**
   * Get HRegionServer start time
   *
   * @return Start time of RegionServer in milliseconds
   */
  long getStartCode();

  /**
   * The number of online regions
   */
  long getNumOnlineRegions();

  /**
   * Get the number of stores hosted on this region server.
   */
  long getNumStores();

  /**
   * Get the number of WAL files of this region server.
   */
  public long getNumWALFiles();
  
  /**
   * Get the size of WAL files of this region server.
   */
  public long getWALFileSize();
  
  /**
   * Get the number of store files hosted on this region server.
   */
  long getNumStoreFiles();

  /**
   * Get the size of the memstore on this region server.
   */
  long getMemstoreSize();

  /**
   * Get the total size of the store files this region server is serving from.
   */
  long getStoreFileSize();

  /**
   * Get the number of requests per second.
   */
  double getRequestsPerSecond();

  /**
   * Get the total number of requests per second.
   */
  long getTotalRequestCount();

  /**
   * Get the number of read requests to regions hosted on this region server.
   */
  long getReadRequestsCount();

  /**
   * Get the number of write requests to regions hosted on this region server.
   */
  long getWriteRequestsCount();

  /**
   * Get the number of CAS operations that failed.
   */
  long getCheckAndMutateChecksFailed();

  /**
   * Get the number of CAS operations that passed.
   */
  long getCheckAndMutateChecksPassed();

  /**
   * Get the Size (in bytes) of indexes in storefiles on disk.
   */
  long getStoreFileIndexSize();

  /**
   * Get the size (in bytes) of of the static indexes including the roots.
   */
  long getTotalStaticIndexSize();

  /**
   * Get the size (in bytes) of the static bloom filters.
   */
  long getTotalStaticBloomSize();

  /**
   * Number of mutations received with WAL explicitly turned off.
   */
  long getNumMutationsWithoutWAL();

  /**
   * Ammount of data in the memstore but not in the WAL because mutations explicitly had their
   * WAL turned off.
   */
  long getDataInMemoryWithoutWAL();

  /**
   * Get the percent of HFiles' that are local.
   */
  int getPercentFileLocal();

  /**
   * Get the percent of HFiles' that are local for secondary region replicas.
   */
  int getPercentFileLocalSecondaryRegions();

  /**
   * Get the size of the split queue
   */
  int getSplitQueueSize();

  /**
   * Get the size of the compaction queue
   */
  int getCompactionQueueSize();

  int getSmallCompactionQueueSize();

  int getLargeCompactionQueueSize();

  /**
   * Get the size of the flush queue.
   */
  int getFlushQueueSize();

  /**
   * Get the size (in bytes) of the block cache that is free.
   */
  long getBlockCacheFreeSize();

  /**
   * Get the number of items in the block cache.
   */
  long getBlockCacheCount();

  /**
   * Get the total size (in bytes) of the block cache.
   */
  long getBlockCacheSize();

  /**
   * Get the count of hits to the block cache
   */
  long getBlockCacheHitCount();

  /**
   * Get the count of misses to the block cache.
   */
  long getBlockCacheMissCount();

  /**
   * Get the number of items evicted from the block cache.
   */
  long getBlockCacheEvictedCount();

  /**
   * Get the percent of all requests that hit the block cache.
   */
  double getBlockCacheHitPercent();

  /**
   * Get the percent of requests with the block cache turned on that hit the block cache.
   */
  int getBlockCacheHitCachingPercent();

  /**
   * Force a re-computation of the metrics.
   */
  void forceRecompute();

  /**
   * Get the amount of time that updates were blocked.
   */
  long getUpdatesBlockedTime();

  /**
   * Get the number of cells flushed to disk.
   */
  long getFlushedCellsCount();

  /**
   * Get the number of cells processed during minor compactions.
   */
  long getCompactedCellsCount();

  /**
   * Get the number of cells processed during major compactions.
   */
  long getMajorCompactedCellsCount();

  /**
   * Get the total amount of data flushed to disk, in bytes.
   */
  long getFlushedCellsSize();

  /**
   * Get the total amount of data processed during minor compactions, in bytes.
   */
  long getCompactedCellsSize();

  /**
   * Get the total amount of data processed during major compactions, in bytes.
   */
  long getMajorCompactedCellsSize();

  /**
   * @return Count of hedged read operations
   */
  public long getHedgedReadOps();

  /**
   * @return Count of times a hedged read beat out the primary read.
   */
  public long getHedgedReadWins();

  /**
   * @return Count of requests blocked because the memstore size is larger than blockingMemStoreSize
   */
  public long getBlockedRequestsCount();
}
