/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

public class ClusterStatusTracker extends ZooKeeperNodeTracker {
  private static final Log LOG = LogFactory.getLog(ClusterStatusTracker.class);

  public static final byte [] upData = Bytes.toBytes("up");

  /**
   * Creates a cluster status tracker.
   *
   * <p>After construction, use {@link #start} to kick off tracking.
   *
   * @param watcher
   * @param abortable
   */
  public ClusterStatusTracker(ZooKeeperWatcher watcher, Abortable abortable) {
    super(watcher, watcher.rootServerZNode, abortable);
  }

  /**
   * Checks if the root region location is available.
   * @return true if root region location is available, false if not
   */
  public boolean isClusterUp() {
    return super.getData() != null;
  }

  /**
   * Sets the cluster as up.
   * @throws KeeperException unexpected zk exception
   */
  public void setClusterUp()
  throws KeeperException {
    try {
      ZKUtil.createAndWatch(watcher, watcher.clusterStateZNode, upData);
    } catch(KeeperException.NodeExistsException nee) {
      ZKUtil.setData(watcher, watcher.clusterStateZNode, upData);
    }
  }

  /**
   * Sets the cluster as down.
   * @throws KeeperException unexpected zk exception
   */
  public void setClusterDown()
  throws KeeperException {
    try {
      ZKUtil.deleteNode(watcher, watcher.clusterStateZNode);
    } catch(KeeperException.NoNodeException nne) {
      LOG.warn("Attempted to set cluster as down but already down, cluster " +
          "state node (" + watcher.clusterStateZNode + ") not found");
    }
  }
}
