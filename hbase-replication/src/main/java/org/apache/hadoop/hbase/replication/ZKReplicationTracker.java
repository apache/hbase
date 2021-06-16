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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

import org.apache.hbase.thirdparty.com.google.common.base.Splitter;

/**
 * This class is a ZooKeeper implementation of the ReplicationTracker interface. This class is
 * responsible for handling replication events that are defined in the ReplicationListener
 * interface.
 */
@InterfaceAudience.Private
class ZKReplicationTracker extends ReplicationTrackerBase {

  // Zookeeper
  private final ZKWatcher zookeeper;
  // Server to abort.
  private final Abortable abortable;
  // All about stopping
  private final Stoppable stopper;
  // List of all the other region servers in this cluster
  private final Set<ServerName> regionServers = new HashSet<>();

  ZKReplicationTracker(ReplicationTrackerParams params) {
    this.zookeeper = params.zookeeper();
    this.abortable = params.abortable();
    this.stopper = params.stopptable();
    this.zookeeper.registerListener(new OtherRegionServerWatcher(this.zookeeper));
  }

  /**
   * Watcher used to be notified of the other region server's death in the local cluster. It
   * initiates the process to transfer the queues if it is able to grab the lock.
   */
  public class OtherRegionServerWatcher extends ZKListener {

    /**
     * Construct a ZooKeeper event listener.
     */
    public OtherRegionServerWatcher(ZKWatcher watcher) {
      super(watcher);
    }

    /**
     * Called when a new node has been created.
     * @param path full path of the new node
     */
    @Override
    public void nodeCreated(String path) {
      if (stopper.isStopped()) {
        return;
      }
      refreshListIfRightPath(path);
    }

    /**
     * Called when a node has been deleted
     * @param path full path of the deleted node
     */
    @Override
    public void nodeDeleted(String path) {
      if (stopper.isStopped()) {
        return;
      }
      if (!refreshListIfRightPath(path)) {
        return;
      }
      notifyListeners(ServerName.valueOf(getZNodeName(path)));
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

    private boolean refreshListIfRightPath(String path) {
      if (!path.startsWith(this.watcher.getZNodePaths().rsZNode)) {
        return false;
      }
      return refreshRegionServerList();
    }
  }

  /**
   * Extracts the znode name of a peer cluster from a ZK path
   * @param fullPath Path to extract the id from
   * @return the id or an empty string if path is invalid
   */
  private String getZNodeName(String fullPath) {
    List<String> parts = Splitter.on('/').splitToList(fullPath);
    return parts.size() > 0 ? parts.get(parts.size() - 1) : "";
  }

  /**
   * Reads the list of region servers from ZK and atomically clears our local view of it and
   * replaces it with the updated list.
   * @return true if the local list of the other region servers was updated with the ZK data (even
   *         if it was empty), false if the data was missing in ZK
   */
  private boolean refreshRegionServerList() {
    Set<ServerName> newRsList = getRegisteredRegionServers();
    if (newRsList == null) {
      return false;
    } else {
      synchronized (regionServers) {
        regionServers.clear();
        regionServers.addAll(newRsList);
      }
    }
    return true;
  }

  /**
   * Get a list of all the other region servers in this cluster and set a watch
   * @return a list of server nanes
   */
  private Set<ServerName> getRegisteredRegionServers() {
    List<String> result = null;
    try {
      result =
        ZKUtil.listChildrenAndWatchThem(this.zookeeper, this.zookeeper.getZNodePaths().rsZNode);
    } catch (KeeperException e) {
      this.abortable.abort("Get list of registered region servers", e);
    }
    return result == null ? null :
      result.stream().map(ServerName::parseServerName).collect(Collectors.toSet());
  }

  @Override
  protected Set<ServerName> internalLoadLiveRegionServersAndInitializeListeners()
    throws IOException {
    if (!refreshRegionServerList()) {
      throw new IOException("failed to refresh region server list");
    }
    synchronized (regionServers) {
      return new HashSet<>(regionServers);
    }
  }
}
