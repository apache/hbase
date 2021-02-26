/*
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

import org.apache.hadoop.hbase.Abortable;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;

/**
 * Tracker on cluster settings up in zookeeper.
 * This is not related to {@link org.apache.hadoop.hbase.ClusterStatus}. That class
 * is a data structure that holds snapshot of current view on cluster. This class
 * is about tracking cluster attributes up in zookeeper.
 *
 */
@InterfaceAudience.Private
public class ClusterStatusTracker extends ZKNodeTracker {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterStatusTracker.class);

  /**
   * Creates a cluster status tracker.
   *
   * <p>After construction, use {@link #start} to kick off tracking.
   *
   * @param watcher reference to the {@link ZKWatcher} which also contains configuration and
   *                constants
   * @param abortable used to abort if a fatal error occurs
   */
  public ClusterStatusTracker(ZKWatcher watcher, Abortable abortable) {
    super(watcher, watcher.getZNodePaths().clusterStateZNode, abortable);
  }

  /**
   * Checks if cluster is up.
   * @return true if the cluster up ('shutdown' is its name up in zk) znode
   *         exists with data, false if not
   */
  public boolean isClusterUp() {
    return super.getData(false) != null;
  }

  /**
   * Sets the cluster as up.
   * @throws KeeperException unexpected zk exception
   */
  public void setClusterUp()
    throws KeeperException {
    byte [] upData = toByteArray();
    try {
      ZKUtil.createAndWatch(watcher, watcher.getZNodePaths().clusterStateZNode, upData);
    } catch(KeeperException.NodeExistsException nee) {
      ZKUtil.setData(watcher, watcher.getZNodePaths().clusterStateZNode, upData);
    }
  }

  /**
   * Sets the cluster as down by deleting the znode.
   * @throws KeeperException unexpected zk exception
   */
  public void setClusterDown()
    throws KeeperException {
    try {
      ZKUtil.deleteNode(watcher, watcher.getZNodePaths().clusterStateZNode);
    } catch(KeeperException.NoNodeException nne) {
      LOG.warn("Attempted to set cluster as down but already down, cluster " +
          "state node (" + watcher.getZNodePaths().clusterStateZNode + ") not found");
    }
  }

  /**
   * @return Content of the clusterup znode as a serialized pb with the pb
   *         magic as prefix.
   */
  static byte [] toByteArray() {
    ZooKeeperProtos.ClusterUp.Builder builder =
      ZooKeeperProtos.ClusterUp.newBuilder();
    builder.setStartDate(new java.util.Date().toString());
    return ProtobufUtil.prependPBMagic(builder.build().toByteArray());
  }
}
