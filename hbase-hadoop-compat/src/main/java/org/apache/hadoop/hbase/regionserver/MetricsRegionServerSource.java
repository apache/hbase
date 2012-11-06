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

import org.apache.hadoop.hbase.metrics.BaseSource;

/**
 * Interface for classes that expose metrics about the regionserver.
 */
public interface MetricsRegionServerSource extends BaseSource {

  /**
   * The name of the metrics
   */
  static final String METRICS_NAME = "Server";

  /**
   * The name of the metrics context that metrics will be under.
   */
  static final String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  static final String METRICS_DESCRIPTION = "Metrics about HBase RegionServer";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  static final String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  /**
   * Update the Put time histogram
   *
   * @param t time it took
   */
  void updatePut(long t);

  /**
   * Update the Delete time histogram
   *
   * @param t time it took
   */
  void updateDelete(long t);

  /**
   * Update the Get time histogram .
   *
   * @param t time it took
   */
  void updateGet(long t);

  /**
   * Update the Increment time histogram.
   *
   * @param t time it took
   */
  void updateIncrement(long t);

  /**
   * Update the Append time histogram.
   *
   * @param t time it took
   */
  void updateAppend(long t);

  // Strings used for exporting to metrics system.
  static final String REGION_COUNT = "regionCount";
  static final String REGION_COUNT_DESC = "Number of regions";
  static final String STORE_COUNT = "storeCount";
  static final String STORE_COUNT_DESC = "Number of Stores";
  static final String STOREFILE_COUNT = "storeFileCount";
  static final String STOREFILE_COUNT_DESC = "Number of Store Files";
  static final String MEMSTORE_SIZE = "memStoreSize";
  static final String MEMSTORE_SIZE_DESC = "Size of the memstore";
  static final String STOREFILE_SIZE = "storeFileSize";
  static final String STOREFILE_SIZE_DESC = "Size of storefiles being served.";
  static final String TOTAL_REQUEST_COUNT = "totalRequestCount";
  static final String TOTAL_REQUEST_COUNT_DESC =
      "Total number of requests this RegionServer has answered.";
  static final String READ_REQUEST_COUNT = "readRequestCount";
  static final String READ_REQUEST_COUNT_DESC =
      "Number of read requests this region server has answered.";
  static final String WRITE_REQUEST_COUNT = "writeRequestCount";
  static final String WRITE_REQUEST_COUNT_DESC =
      "Number of mutation requests this region server has answered.";
  static final String CHECK_MUTATE_FAILED_COUNT = "checkMutateFailedCount";
  static final String CHECK_MUTATE_FAILED_COUNT_DESC =
      "Number of Check and Mutate calls that failed the checks.";
  static final String CHECK_MUTATE_PASSED_COUNT = "checkMutatePassedCount";
  static final String CHECK_MUTATE_PASSED_COUNT_DESC =
      "Number of Check and Mutate calls that passed the checks.";
  static final String STOREFILE_INDEX_SIZE = "storeFileIndexSize";
  static final String STOREFILE_INDEX_SIZE_DESC = "Size of indexes in storefiles on disk.";
  static final String STATIC_INDEX_SIZE = "staticIndexSize";
  static final String STATIC_INDEX_SIZE_DESC = "Uncompressed size of the static indexes.";
  static final String STATIC_BLOOM_SIZE = "staticBloomSize";
  static final String STATIC_BLOOM_SIZE_DESC =
      "Uncompressed size of the static bloom filters.";
  static final String NUMBER_OF_PUTS_WITHOUT_WAL = "putsWithoutWALCount";
  static final String NUMBER_OF_PUTS_WITHOUT_WAL_DESC =
      "Number of mutations that have been sent by clients with the write ahead logging turned off.";
  static final String DATA_SIZE_WITHOUT_WAL = "putsWithoutWALSize";
  static final String DATA_SIZE_WITHOUT_WAL_DESC =
      "Size of data that has been sent by clients with the write ahead logging turned off.";
  static final String PERCENT_FILES_LOCAL = "percentFilesLocal";
  static final String PERCENT_FILES_LOCAL_DESC =
      "The percent of HFiles that are stored on the local hdfs data node.";
  static final String COMPACTION_QUEUE_LENGTH = "compactionQueueLength";
  static final String COMPACTION_QUEUE_LENGTH_DESC = "Length of the queue for compactions.";
  static final String FLUSH_QUEUE_LENGTH = "flushQueueLength";
  static final String FLUSH_QUEUE_LENGTH_DESC = "Length of the queue for region flushes";
  static final String BLOCK_CACHE_FREE_SIZE = "blockCacheFreeSize";
  static final String BLOCK_CACHE_FREE_DESC =
      "Size of the block cache that is not occupied.";
  static final String BLOCK_CACHE_COUNT = "blockCacheCount";
  static final String BLOCK_CACHE_COUNT_DESC = "Number of block in the block cache.";
  static final String BLOCK_CACHE_SIZE = "blockCacheSize";
  static final String BLOCK_CACHE_SIZE_DESC = "Size of the block cache.";
  static final String BLOCK_CACHE_HIT_COUNT = "blockCacheHitCount";
  static final String BLOCK_CACHE_HIT_COUNT_DESC = "Count of the hit on the block cache.";
  static final String BLOCK_CACHE_MISS_COUNT = "blockCacheMissCount";
  static final String BLOCK_COUNT_MISS_COUNT_DESC =
      "Number of requests for a block that missed the block cache.";
  static final String BLOCK_CACHE_EVICTION_COUNT = "blockCacheEvictionCount";
  static final String BLOCK_CACHE_EVICTION_COUNT_DESC =
      "Count of the number of blocks evicted from the block cache.";
  static final String BLOCK_CACHE_HIT_PERCENT = "blockCountHitPercent";
  static final String BLOCK_CACHE_HIT_PERCENT_DESC =
      "Percent of block cache requests that are hits";
  static final String BLOCK_CACHE_EXPRESS_HIT_PERCENT = "blockCacheExpressHitPercent";
  static final String BLOCK_CACHE_EXPRESS_HIT_PERCENT_DESC =
      "The percent of the time that requests with the cache turned on hit the cache.";
  static final String RS_START_TIME_NAME = "regionServerStartTime";
  static final String ZOOKEEPER_QUORUM_NAME = "zookeeperQuorum";
  static final String SERVER_NAME_NAME = "serverName";
  static final String CLUSTER_ID_NAME = "clusterId";
  static final String RS_START_TIME_DESC = "RegionServer Start Time";
  static final String ZOOKEEPER_QUORUM_DESC = "Zookeeper Quorum";
  static final String SERVER_NAME_DESC = "Server Name";
  static final String CLUSTER_ID_DESC = "Cluster Id";
  static final String UPDATES_BLOCKED_TIME = "updatesBlockedTime";
  static final String UPDATES_BLOCKED_DESC =
      "Number of MS updates have been blocked so that the memstore can be flushed.";
  static final String DELETE_KEY = "delete";
  static final String GET_KEY = "get";
  static final String INCREMENT_KEY = "increment";
  static final String PUT_KEY = "multiput";
  static final String APPEND_KEY = "append";
}
