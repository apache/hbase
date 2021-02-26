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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a ZooKeeper implementation of the ReplicationTracker interface. This class is
 * responsible for handling replication events that are defined in the ReplicationListener
 * interface.
 */
@InterfaceAudience.Private
public class ReplicationTrackerZKImpl implements ReplicationTracker {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationTrackerZKImpl.class);

  // Zookeeper
  private final ZKWatcher zookeeper;
  // Server to abort.
  private final Abortable abortable;
  // All about stopping
  private final Stoppable stopper;
  // listeners to be notified
  private final List<ReplicationListener> listeners = new CopyOnWriteArrayList<>();
  // List of all the other region servers in this cluster
  private final List<ServerName> otherRegionServers = new ArrayList<>();

  public ReplicationTrackerZKImpl(ZKWatcher zookeeper, Abortable abortable, Stoppable stopper) {
    this.zookeeper = zookeeper;
    this.abortable = abortable;
    this.stopper = stopper;
    this.zookeeper.registerListener(new OtherRegionServerWatcher(this.zookeeper));
    // watch the changes
    refreshOtherRegionServersList(true);
  }

  @Override
  public void registerListener(ReplicationListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(ReplicationListener listener) {
    listeners.remove(listener);
  }

  /**
   * Return a snapshot of the current region servers.
   */
  @Override
  public List<ServerName> getListOfRegionServers() {
    refreshOtherRegionServersList(false);

    List<ServerName> list = null;
    synchronized (otherRegionServers) {
      list = new ArrayList<>(otherRegionServers);
    }
    return list;
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
      boolean cont = refreshListIfRightPath(path);
      if (!cont) {
        return;
      }
      LOG.info(path + " znode expired, triggering replicatorRemoved event");
      for (ReplicationListener rl : listeners) {
        rl.regionServerRemoved(getZNodeName(path));
      }
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
      return refreshOtherRegionServersList(true);
    }
  }

  /**
   * Extracts the znode name of a peer cluster from a ZK path
   * @param fullPath Path to extract the id from
   * @return the id or an empty string if path is invalid
   */
  private String getZNodeName(String fullPath) {
    String[] parts = fullPath.split("/");
    return parts.length > 0 ? parts[parts.length - 1] : "";
  }

  /**
   * Reads the list of region servers from ZK and atomically clears our local view of it and
   * replaces it with the updated list.
   * @return true if the local list of the other region servers was updated with the ZK data (even
   *         if it was empty), false if the data was missing in ZK
   */
  private boolean refreshOtherRegionServersList(boolean watch) {
    List<ServerName> newRsList = getRegisteredRegionServers(watch);
    if (newRsList == null) {
      return false;
    } else {
      synchronized (otherRegionServers) {
        otherRegionServers.clear();
        otherRegionServers.addAll(newRsList);
      }
    }
    return true;
  }

  /**
   * Get a list of all the other region servers in this cluster and set a watch
   * @return a list of server nanes
   */
  private List<ServerName> getRegisteredRegionServers(boolean watch) {
    List<String> result = null;
    try {
      if (watch) {
        result = ZKUtil.listChildrenAndWatchThem(this.zookeeper,
                this.zookeeper.getZNodePaths().rsZNode);
      } else {
        result = ZKUtil.listChildrenNoWatch(this.zookeeper, this.zookeeper.getZNodePaths().rsZNode);
      }
    } catch (KeeperException e) {
      this.abortable.abort("Get list of registered region servers", e);
    }
    return result == null ? null :
      result.stream().map(ServerName::parseServerName).collect(Collectors.toList());
  }
}
