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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZK_SESSION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.ZK_SESSION_TIMEOUT;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;

/**
 * Cache the cluster registry data in memory and use zk watcher to update. The only exception is
 * {@link #getClusterId()}, it will fetch the data from zk directly.
 */
@InterfaceAudience.Private
class ZKClusterRegistry implements ClusterRegistry {

  private static final Log LOG = LogFactory.getLog(ZKClusterRegistry.class);

  private final RecoverableZooKeeper zk;

  private final ZNodePaths znodePaths;

  ZKClusterRegistry(Configuration conf) throws IOException {
    this.znodePaths = new ZNodePaths(conf);
    int zkSessionTimeout = conf.getInt(ZK_SESSION_TIMEOUT, DEFAULT_ZK_SESSION_TIMEOUT);
    int zkRetry = conf.getInt("zookeeper.recovery.retry", 3);
    int zkRetryIntervalMs = conf.getInt("zookeeper.recovery.retry.intervalmill", 1000);
    this.zk = new RecoverableZooKeeper(ZKConfig.getZKQuorumServersString(conf), zkSessionTimeout,
        null, zkRetry, zkRetryIntervalMs);
  }

  @Override
  public String getClusterId() {
    try {
      byte[] data = zk.getData(znodePaths.clusterIdZNode, false, null);
      if (data == null || data.length == 0) {
        return null;
      }
      return ClusterId.parseFrom(data).toString();
    } catch (Exception e) {
      LOG.warn("failed to get cluster id", e);
      return null;
    }
  }

  @Override
  public void close() {
    try {
      zk.close();
    } catch (InterruptedException e) {
      LOG.warn("close zookeeper failed", e);
    }
  }
}