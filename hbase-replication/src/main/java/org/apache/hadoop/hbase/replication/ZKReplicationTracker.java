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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;

/**
 * This class is a ZooKeeper implementation of the ReplicationTracker interface. This class is
 * responsible for handling replication events that are defined in the ReplicationListener
 * interface.
 */
@InterfaceAudience.Private
class ZKReplicationTracker extends ReplicationTrackerBase {

  private static final Logger LOG = LoggerFactory.getLogger(ZKReplicationTracker.class);

  // Zookeeper
  private final ZKWatcher zookeeper;
  // All about stopping
  private final Stoppable stopper;

  ZKReplicationTracker(ReplicationTrackerParams params) {
    this.zookeeper = params.zookeeper();
    this.stopper = params.stopptable();
    this.zookeeper.registerListener(new RegionServerWatcher(this.zookeeper));
  }

  /**
   * Watcher used to be notified of the other region server's death in the local cluster. It
   * initiates the process to transfer the queues if it is able to grab the lock.
   */
  public class RegionServerWatcher extends ZKListener {

    /**
     * Construct a ZooKeeper event listener.
     */
    public RegionServerWatcher(ZKWatcher watcher) {
      super(watcher);
    }

    /**
     * Called when an existing node has a child node added or removed.
     * @param path full path of the node whose children have changed
     */
    @Override
    public void nodeChildrenChanged(String path) {
      if (stopper.isStopped()) {
        return;
      }
      refreshListIfRightPath(path);
    }

    private void refreshListIfRightPath(String path) {
      if (!path.startsWith(this.watcher.getZNodePaths().rsZNode)) {
        return;
      }
      Set<ServerName> newRegionServers;
      try {
        newRegionServers = getRegisteredRegionServers();
      } catch (IOException e) {
        LOG.warn("failed to get registered region servers", e);
        return;
      }
      notifyListeners(newRegionServers);
    }
  }

  /**
   * Get a list of all the other region servers in this cluster and set a watch
   * @return a list of server names
   */
  private Set<ServerName> getRegisteredRegionServers() throws IOException {
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenAndWatchForNewChildren(this.zookeeper,
        this.zookeeper.getZNodePaths().rsZNode);
    } catch (KeeperException e) {
      throw new IOException("failed to list registered region servers", e);
    }
    if (result == null) {
      throw new IOException("got null when listing registered region servers");
    }
    return result.stream().map(ServerName::parseServerName).collect(ImmutableSet.toImmutableSet());
  }

  @Override
  protected Set<ServerName> internalLoadLiveRegionServersAndInitializeListeners()
    throws IOException {
    return getRegisteredRegionServers();
  }
}
