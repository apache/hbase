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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

/**
 * Tracks the target znode(s) on server ZK cluster and synchronize them to client ZK cluster if
 * changed
 * <p/>
 * The target znode(s) is given through {@link #getNodesToWatch()} method
 */
@InterfaceAudience.Private
public abstract class ClientZKSyncer extends ZKListener {
  private static final Log LOG = LogFactory.getLog(ClientZKSyncer.class);
  private final Server server;
  private final ZKWatcher clientZkWatcher;
  // We use queues and daemon threads to synchronize the data to client ZK cluster
  // to avoid blocking the single event thread for watchers
  private final Map<String, BlockingQueue<byte[]>> queues;

  public ClientZKSyncer(ZKWatcher watcher, ZKWatcher clientZkWatcher, Server server) {
    super(watcher);
    this.server = server;
    this.clientZkWatcher = clientZkWatcher;
    this.queues = new HashMap<>();
  }

  /**
   * Starts the syncer
   * @throws KeeperException if error occurs when trying to create base nodes on client ZK
   */
  public void start() throws KeeperException {
    LOG.debug("Starting " + getClass().getSimpleName());
    this.watcher.registerListener(this);
    // create base znode on remote ZK
    ZKUtil.createWithParents(clientZkWatcher, watcher.znodePaths.baseZNode);
    // set meta znodes for client ZK
    Collection<String> nodes = getNodesToWatch();
    LOG.debug("Znodes to watch: " + nodes);
    // initialize queues and threads
    for (String node : nodes) {
      BlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(1);
      queues.put(node, queue);
      Thread updater = new ClientZkUpdater(node, queue);
      updater.setDaemon(true);
      updater.start();
      watchAndCheckExists(node);
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
    BlockingQueue<byte[]> queue = queues.get(node);
    synchronized (queue) {
      queue.poll();
      queue.offer(data);
    }
  }

  /**
   * Set data for client ZK and retry until succeed. Be very careful to prevent dead loop when
   * modifying this method
   * @param node the znode to set on client ZK
   * @param data the data to set to client ZK
   * @throws InterruptedException if the thread is interrupted during process
   */
  private final void setDataForClientZkUntilSuccess(String node, byte[] data)
      throws InterruptedException {
    while (!server.isStopped()) {
      try {
        LOG.debug("Set data for remote " + node + ", client zk wather: " + clientZkWatcher);
        ZKUtil.setData(clientZkWatcher, node, data);
        break;
      } catch (KeeperException.NoNodeException nne) {
        // Node doesn't exist, create it and set value
        try {
          ZKUtil.createNodeIfNotExistsNoWatch(clientZkWatcher, node, data, CreateMode.PERSISTENT);
          break;
        } catch (KeeperException.ConnectionLossException
            | KeeperException.SessionExpiredException ee) {
          reconnectAfterExpiration();
        } catch (KeeperException e) {
          LOG.warn(
            "Failed to create znode " + node + " due to: " + e.getMessage() + ", will retry later");
        }
      } catch (KeeperException.ConnectionLossException
          | KeeperException.SessionExpiredException ee) {
        reconnectAfterExpiration();
      } catch (KeeperException e) {
        LOG.debug("Failed to set data to client ZK, will retry later", e);
      }
      Threads.sleep(HConstants.SOCKET_RETRY_WAIT_MS);
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

  @Override
  public void nodeCreated(String path) {
    if (!validate(path)) {
      return;
    }
    try {
      byte[] data = ZKUtil.getDataAndWatch(watcher, path);
      upsertQueue(path, data);
    } catch (KeeperException e) {
      LOG.warn("Unexpected exception handling nodeCreated event", e);
    }
  }

  @Override
  public void nodeDataChanged(String path) {
    if (validate(path)) {
      nodeCreated(path);
    }
  }

  @Override
  public synchronized void nodeDeleted(String path) {
    if (validate(path)) {
      try {
        if (ZKUtil.watchAndCheckExists(watcher, path)) {
          nodeCreated(path);
        }
      } catch (KeeperException e) {
        LOG.warn("Unexpected exception handling nodeDeleted event for path: " + path, e);
      }
    }
  }

  /**
   * Validate whether a znode path is watched by us
   * @param path the path to validate
   * @return true if the znode is watched by us
   */
  abstract boolean validate(String path);

  /**
   * @return the znode(s) to watch
   */
  abstract Collection<String> getNodesToWatch();

  /**
   * Thread to synchronize znode data to client ZK cluster
   */
  class ClientZkUpdater extends Thread {
    final String znode;
    final BlockingQueue<byte[]> queue;

    public ClientZkUpdater(String znode, BlockingQueue<byte[]> queue) {
      this.znode = znode;
      this.queue = queue;
      setName("ClientZKUpdater-" + znode);
    }

    @Override
    public void run() {
      while (!server.isStopped()) {
        try {
          byte[] data = queue.take();
          setDataForClientZkUntilSuccess(znode, data);
        } catch (InterruptedException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
              "Interrupted while checking whether need to update meta location to client zk");
          }
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }
}
