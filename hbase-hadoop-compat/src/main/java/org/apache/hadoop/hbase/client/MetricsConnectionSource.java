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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.metrics.BaseSource;

public interface MetricsConnectionSource extends BaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "Connection";

  /**
   * The name of the metrics context that metrics will be under.
   */
  String METRICS_CONTEXT = "connection";

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HBase Connection";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "Client,sub=";

  /**
   * Increment number of meta cache hits
   */
  void incrMetaCacheHit();

  /**
   * Increment number of meta cache misses
   */
  void incrMetaCacheMiss();

  // Strings used for exporting to metrics system.
  String CONNECTION_ID_NAME = "connectionId";
  String CONNECTION_ID_DESC = "The connection's process-unique identifier.";
  String USER_NAME_NAME = "userName";
  String USER_NAME_DESC = "The user on behalf of whom the Connection is acting.";
  String CLUSTER_ID_NAME = "clusterId";
  String CLUSTER_ID_DESC = "Cluster Id";
  String ZOOKEEPER_QUORUM_NAME = "zookeeperQuorum";
  String ZOOKEEPER_QUORUM_DESC = "Zookeeper Quorum";
  String ZOOKEEPER_ZNODE_NAME = "zookeeperBaseZNode";
  String ZOOKEEPER_ZNODE_DESC = "Base ZNode for this cluster.";

  String META_CACHE_HIT_NAME = "metaCacheHit";
  String META_CACHE_HIT_DESC =
      "A counter on the number of times this connection's meta cache has a valid region location.";
  String META_CACHE_MISS_NAME = "metaCacheMiss";
  String META_CACHE_MISS_DESC =
      "A counter on the number of times this connection does not know where to find a region.";

  String META_LOOKUP_POOL_ACTIVE_THREAD_NAME = "metaLookupPoolActiveThreads";
  String META_LOOKUP_POOL_ACTIVE_THREAD_DESC =
      "The approximate number of threads actively resolving region locations from META.";
  String META_LOOKUP_POOL_LARGEST_SIZE_NAME = "metaLookupPoolLargestSize";
  String META_LOOKUP_POOL_LARGEST_SIZE_DESC =
      "The largest number of threads that have ever simultaneously been in the pool.";
  String BATCH_POOL_ID_NAME = "batchPoolId";
  String BATCH_POOL_ID_DESC = "The connection's batch pool's unique identifier.";
  String BATCH_POOL_ACTIVE_THREAD_NAME = "batchPoolActiveThreads";
  String BATCH_POOL_ACTIVE_THREAD_DESC =
      "The approximate number of threads executing table operations.";
  String BATCH_POOL_LARGEST_SIZE_NAME = "batchPoolLargestSize";
  String BATCH_POOL_LARGEST_SIZE_DESC =
      "The largest number of threads that have ever simultaneously been in the pool.";
}
