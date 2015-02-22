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
package org.apache.hadoop.hbase.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * This class is a Zookeeper implementation of the ReplicationTracker interface. This class is
 * responsible for handling replication events that are defined in the ReplicationListener
 * interface.
 */
@InterfaceAudience.Private
public class ReplicationTrackerZKImpl extends ReplicationStateZKBase implements ReplicationTracker {

  private static final Log LOG = LogFactory.getLog(ReplicationTrackerZKImpl.class);
  // All about stopping
  private final Stoppable stopper;
  // listeners to be notified
  private final List<ReplicationListener> listeners =
      new CopyOnWriteArrayList<ReplicationListener>();
  // List of all the other region servers in this cluster
  private final ArrayList<String> otherRegionServers = new ArrayList<String>();
  private final ReplicationPeers replicationPeers;

  public ReplicationTrackerZKImpl(ZooKeeperWatcher zookeeper,
      final ReplicationPeers replicationPeers, Configuration conf, Abortable abortable,
      Stoppable stopper) {
    super(zookeeper, conf, abortable);
    this.replicationPeers = replicationPeers;
    this.stopper = stopper;
    this.zookeeper.registerListener(new OtherRegionServerWatcher(this.zookeeper));
    this.zookeeper.registerListener(new PeersWatcher(this.zookeeper));
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
  public List<String> getListOfRegionServers() {
    refreshOtherRegionServersList();

    List<String> list = null;
    synchronized (otherRegionServers) {
      list = new ArrayList<String>(otherRegionServers);
    }
    return list;
  }

  /**
   * Watcher used to be notified of the other region server's death in the local cluster. It
   * initiates the process to transfer the queues if it is able to grab the lock.
   */
  public class OtherRegionServerWatcher extends ZooKeeperListener {

    /**
     * Construct a ZooKeeper event listener.
     */
    public OtherRegionServerWatcher(ZooKeeperWatcher watcher) {
      super(watcher);
    }

    /**
     * Called when a new node has been created.
     * @param path full path of the new node
     */
    public void nodeCreated(String path) {
      refreshListIfRightPath(path);
    }

    /**
     * Called when a node has been deleted
     * @param path full path of the deleted node
     */
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
    public void nodeChildrenChanged(String path) {
      if (stopper.isStopped()) {
        return;
      }
      refreshListIfRightPath(path);
    }

    private boolean refreshListIfRightPath(String path) {
      if (!path.startsWith(this.watcher.rsZNode)) {
        return false;
      }
      return refreshOtherRegionServersList();
    }
  }

  /**
   * Watcher used to follow the creation and deletion of peer clusters.
   */
  public class PeersWatcher extends ZooKeeperListener {

    /**
     * Construct a ZooKeeper event listener.
     */
    public PeersWatcher(ZooKeeperWatcher watcher) {
      super(watcher);
    }

    /**
     * Called when a node has been deleted
     * @param path full path of the deleted node
     */
    public void nodeDeleted(String path) {
      List<String> peers = refreshPeersList(path);
      if (peers == null) {
        return;
      }
      if (isPeerPath(path)) {
        String id = getZNodeName(path);
        LOG.info(path + " znode expired, triggering peerRemoved event");
        for (ReplicationListener rl : listeners) {
          rl.peerRemoved(id);
        }
      }
    }

    /**
     * Called when an existing node has a child node added or removed.
     * @param path full path of the node whose children have changed
     */
    public void nodeChildrenChanged(String path) {
      List<String> peers = refreshPeersList(path);
      if (peers == null) {
        return;
      }
      LOG.info(path + " znode expired, triggering peerListChanged event");
      for (ReplicationListener rl : listeners) {
        rl.peerListChanged(peers);
      }
    }
  }

  /**
   * Verify if this event is meant for us, and if so then get the latest peers' list from ZK. Also
   * reset the watches.
   * @param path path to check against
   * @return A list of peers' identifiers if the event concerns this watcher, else null.
   */
  private List<String> refreshPeersList(String path) {
    if (!path.startsWith(getPeersZNode())) {
      return null;
    }
    return this.replicationPeers.getAllPeerIds();
  }

  private String getPeersZNode() {
    return this.peersZNode;
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
  private boolean refreshOtherRegionServersList() {
    List<String> newRsList = getRegisteredRegionServers();
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
  private List<String> getRegisteredRegionServers() {
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenAndWatchThem(this.zookeeper, this.zookeeper.rsZNode);
    } catch (KeeperException e) {
      this.abortable.abort("Get list of registered region servers", e);
    }
    return result;
  }
}
