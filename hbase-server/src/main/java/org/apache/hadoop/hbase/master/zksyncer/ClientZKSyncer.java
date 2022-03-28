/**
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
package org.apache.hadoop.hbase.master.zksyncer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the target znode(s) on server ZK cluster and synchronize them to client ZK cluster if
 * changed
 * <p/>
 * The target znode(s) is given through {@link #getPathsToWatch()} method
 */
@InterfaceAudience.Private
public abstract class ClientZKSyncer extends ZKListener {
  private static final Logger LOG = LoggerFactory.getLogger(ClientZKSyncer.class);
  private final Server server;
  private final ZKWatcher clientZkWatcher;

  /**
   * Used to store the newest data which we want to sync to client zk.
   * <p/>
   * For meta location, since we may reduce the replica number, so here we add a {@code delete} flag
   * to tell the updater delete the znode on client zk and quit.
   */
  private static final class ZKData {

    byte[] data;

    boolean delete = false;

    synchronized void set(byte[] data) {
      this.data = data;
      notifyAll();
    }

    synchronized byte[] get() throws InterruptedException {
      while (!delete && data == null) {
        wait();
      }
      byte[] d = data;
      data = null;
      return d;
    }

    synchronized void delete() {
      this.delete = true;
      notifyAll();
    }

    synchronized boolean isDeleted() {
      return delete;
    }
  }

  // We use queues and daemon threads to synchronize the data to client ZK cluster
  // to avoid blocking the single event thread for watchers
  private final ConcurrentMap<String, ZKData> queues;

  public ClientZKSyncer(ZKWatcher watcher, ZKWatcher clientZkWatcher, Server server) {
    super(watcher);
    this.server = server;
    this.clientZkWatcher = clientZkWatcher;
    this.queues = new ConcurrentHashMap<>();
  }

  private void startNewSyncThread(String path) {
    ZKData zkData = new ZKData();
    queues.put(path, zkData);
    Thread updater = new ClientZkUpdater(path, zkData);
    updater.setDaemon(true);
    updater.start();
    watchAndCheckExists(path);
  }

  /**
   * Starts the syncer
   * @throws KeeperException if error occurs when trying to create base nodes on client ZK
   */
  public void start() throws KeeperException {
    LOG.debug("Starting " + getClass().getSimpleName());
    this.watcher.registerListener(this);
    // create base znode on remote ZK
    ZKUtil.createWithParents(clientZkWatcher, watcher.getZNodePaths().baseZNode);
    // set znodes for client ZK
    Set<String> paths = getPathsToWatch();
    LOG.debug("ZNodes to watch: {}", paths);
    // initialize queues and threads
    for (String path : paths) {
      startNewSyncThread(path);
    }
  }

  private void watchAndCheckExists(String node) {
    try {
      if (ZKUtil.watchAndCheckExists(watcher, node)) {
        byte[] data = ZKUtil.getDataAndWatch(watcher, node);
        if (data != null) {
          // put the data into queue
          upsertQueue(node, data);
        } else {
          // It existed but now does not, should has been tracked by our watcher, ignore
          LOG.debug("Found no data from " + node);
          watchAndCheckExists(node);
        }
      } else {
        // cleanup stale ZNodes on client ZK to avoid invalid requests to server
        ZKUtil.deleteNodeFailSilent(clientZkWatcher, node);
      }
    } catch (KeeperException e) {
      server.abort("Unexpected exception during initialization, aborting", e);
    }
  }

  /**
   * Update the value of the single element in queue if any, or else insert.
   * <p/>
   * We only need to synchronize the latest znode value to client ZK rather than synchronize each
   * time
   * @param data the data to write to queue
   */
  private void upsertQueue(String node, byte[] data) {
    ZKData zkData = queues.get(node);
    if (zkData != null) {
      zkData.set(data);
    }
  }

  /**
   * Set data for client ZK and retry until succeed. Be very careful to prevent dead loop when
   * modifying this method
   * @param node the znode to set on client ZK
   * @param data the data to set to client ZK
   * @throws InterruptedException if the thread is interrupted during process
   */
  private void setDataForClientZkUntilSuccess(String node, byte[] data)
    throws InterruptedException {
    boolean create = false;
    while (!server.isStopped()) {
      try {
        LOG.debug("Set data for remote " + node + ", client zk wather: " + clientZkWatcher);
        if (create) {
          ZKUtil.createNodeIfNotExistsNoWatch(clientZkWatcher, node, data, CreateMode.PERSISTENT);
        } else {
          ZKUtil.setData(clientZkWatcher, node, data);
        }
        break;
      } catch (KeeperException e) {
        LOG.debug("Failed to set data for {} to client ZK, will retry later", node, e);
        if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
          reconnectAfterExpiration();
        }
        if (e.code() == KeeperException.Code.NONODE) {
          create = true;
        }
        if (e.code() == KeeperException.Code.NODEEXISTS) {
          create = false;
        }
      }
      Threads.sleep(HConstants.SOCKET_RETRY_WAIT_MS);
    }
  }

  private void deleteDataForClientZkUntilSuccess(String node) throws InterruptedException {
    while (!server.isStopped()) {
      LOG.debug("Delete remote " + node + ", client zk wather: " + clientZkWatcher);
      try {
        ZKUtil.deleteNode(clientZkWatcher, node);
      } catch (KeeperException e) {
        LOG.debug("Failed to delete node from client ZK, will retry later", e);
        if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
          reconnectAfterExpiration();
        }
        
      }
    }
  }

  private final void reconnectAfterExpiration() throws InterruptedException {
    LOG.warn("ZK session expired or lost. Retry a new connection...");
    try {
      clientZkWatcher.reconnectAfterExpiration();
    } catch (IOException | KeeperException e) {
      LOG.warn("Failed to reconnect to client zk after session expiration, will retry later", e);
    }
  }

  private void getDataAndWatch(String path) {
    try {
      byte[] data = ZKUtil.getDataAndWatch(watcher, path);
      upsertQueue(path, data);
    } catch (KeeperException e) {
      LOG.warn("Unexpected exception handling nodeCreated event", e);
    }
  }

  private void removeQueue(String path) {
    ZKData zkData = queues.remove(path);
    if (zkData != null) {
      zkData.delete();
    }
  }

  @Override
  public void nodeCreated(String path) {
    if (validate(path)) {
      getDataAndWatch(path);
    } else {
      removeQueue(path);
    }
  }

  @Override
  public void nodeDataChanged(String path) {
    nodeCreated(path);
  }

  @Override
  public synchronized void nodeDeleted(String path) {
    if (validate(path)) {
      try {
        if (ZKUtil.watchAndCheckExists(watcher, path)) {
          getDataAndWatch(path);
        }
      } catch (KeeperException e) {
        LOG.warn("Unexpected exception handling nodeDeleted event for path: " + path, e);
      }
    } else {
      removeQueue(path);
    }
  }

  /**
   * Validate whether a znode path is watched by us
   * @param path the path to validate
   * @return true if the znode is watched by us
   */
  protected abstract boolean validate(String path);

  /**
   * @return the zk path(s) to watch
   */
  protected abstract Set<String> getPathsToWatch();

  protected final void refreshWatchingList() {
    Set<String> newPaths = getPathsToWatch();
    LOG.debug("New ZNodes to watch: {}", newPaths);
    Iterator<Map.Entry<String, ZKData>> iter = queues.entrySet().iterator();
    // stop unused syncers
    while (iter.hasNext()) {
      Map.Entry<String, ZKData> entry = iter.next();
      if (!newPaths.contains(entry.getKey())) {
        iter.remove();
        entry.getValue().delete();
      }
    }
    // start new syncers
    for (String newPath : newPaths) {
      if (!queues.containsKey(newPath)) {
        startNewSyncThread(newPath);
      }
    }
  }

  /**
   * Thread to synchronize znode data to client ZK cluster
   */
  private final class ClientZkUpdater extends Thread {
    private final String znode;
    private final ZKData zkData;

    public ClientZkUpdater(String znode, ZKData zkData) {
      this.znode = znode;
      this.zkData = zkData;
      setName("ClientZKUpdater-" + znode);
    }

    @Override
    public void run() {
      LOG.debug("Client zk updater for znode {} started", znode);
      while (!server.isStopped()) {
        try {
          byte[] data = zkData.get();
          if (data != null) {
            setDataForClientZkUntilSuccess(znode, data);
          } else {
            if (zkData.isDeleted()) {
              deleteDataForClientZkUntilSuccess(znode);
              break;
            }
          }
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while checking whether need to update meta location to client zk");
          Thread.currentThread().interrupt();
          break;
        }
      }
      LOG.debug("Client zk updater for znode {} stopped", znode);
    }
  }
}
