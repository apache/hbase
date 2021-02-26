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

/**
 * Tracks the availability and value of a single ZooKeeper node.
 *
 * <p>Utilizes the {@link ZKListener} interface to get the necessary
 * ZooKeeper events related to the node.
 *
 * <p>This is the base class used by trackers in both the Master and
 * RegionServers.
 */
@InterfaceAudience.Private
public abstract class ZKNodeTracker extends ZKListener {
  // LOG is being used in subclasses, hence keeping it protected
  protected static final Logger LOG = LoggerFactory.getLogger(ZKNodeTracker.class);
  /** Path of node being tracked */
  protected final String node;

  /** Data of the node being tracked */
  private byte [] data;

  /** Used to abort if a fatal error occurs */
  protected final Abortable abortable;

  private boolean stopped = false;

  /**
   * Constructs a new ZK node tracker.
   *
   * <p>After construction, use {@link #start} to kick off tracking.
   *
   * @param watcher reference to the {@link ZKWatcher} which also contains configuration and
   *                constants
   * @param node path of the node being tracked
   * @param abortable used to abort if a fatal error occurs
   */
  public ZKNodeTracker(ZKWatcher watcher, String node,
                       Abortable abortable) {
    super(watcher);
    this.node = node;
    this.abortable = abortable;
    this.data = null;
  }

  /**
   * Starts the tracking of the node in ZooKeeper.
   *
   * <p>Use {@link #blockUntilAvailable()} to block until the node is available
   * or {@link #getData(boolean)} to get the data of the node if it is available.
   */
  public synchronized void start() {
    this.watcher.registerListener(this);
    try {
      if(ZKUtil.watchAndCheckExists(watcher, node)) {
        byte [] data = ZKUtil.getDataAndWatch(watcher, node);
        if(data != null) {
          this.data = data;
        } else {
          // It existed but now does not, try again to ensure a watch is set
          LOG.debug("Try starting again because there is no data from {}", node);
          start();
        }
      }
    } catch (KeeperException e) {
      abortable.abort("Unexpected exception during initialization, aborting", e);
    }
  }

  public synchronized void stop() {
    this.stopped = true;
    notifyAll();
  }

  /**
   * Gets the data of the node, blocking until the node is available.
   *
   * @return data of the node
   * @throws InterruptedException if the waiting thread is interrupted
   */
  public synchronized byte [] blockUntilAvailable()
    throws InterruptedException {
    return blockUntilAvailable(0, false);
  }

  /**
   * Gets the data of the node, blocking until the node is available or the
   * specified timeout has elapsed.
   *
   * @param timeout maximum time to wait for the node data to be available, n milliseconds. Pass 0
   *                for no timeout.
   * @return data of the node
   * @throws InterruptedException if the waiting thread is interrupted
   */
  public synchronized byte [] blockUntilAvailable(long timeout, boolean refresh)
          throws InterruptedException {
    if (timeout < 0) {
      throw new IllegalArgumentException();
    }

    boolean notimeout = timeout == 0;
    long startTime = System.currentTimeMillis();
    long remaining = timeout;
    if (refresh) {
      try {
        // This does not create a watch if the node does not exists
        this.data = ZKUtil.getDataAndWatch(watcher, node);
      } catch(KeeperException e) {
        // We use to abort here, but in some cases the abort is ignored (
        //  (empty Abortable), so it's better to log...
        LOG.warn("Unexpected exception handling blockUntilAvailable", e);
        abortable.abort("Unexpected exception handling blockUntilAvailable", e);
      }
    }
    boolean nodeExistsChecked = (!refresh ||data!=null);
    while (!this.stopped && (notimeout || remaining > 0) && this.data == null) {
      if (!nodeExistsChecked) {
        try {
          nodeExistsChecked = (ZKUtil.checkExists(watcher, node) != -1);
        } catch (KeeperException e) {
          LOG.warn("Got exception while trying to check existence in  ZooKeeper" +
            " of the node: " + node + ", retrying if timeout not reached", e);
        }

        // It did not exists, and now it does.
        if (nodeExistsChecked){
          LOG.debug("Node {} now exists, resetting a watcher", node);
          try {
            // This does not create a watch if the node does not exists
            this.data = ZKUtil.getDataAndWatch(watcher, node);
          } catch (KeeperException e) {
            LOG.warn("Unexpected exception handling blockUntilAvailable", e);
            abortable.abort("Unexpected exception handling blockUntilAvailable", e);
          }
        }
      }
      // We expect a notification; but we wait with a
      //  a timeout to lower the impact of a race condition if any
      wait(100);
      remaining = timeout - (System.currentTimeMillis() - startTime);
    }
    return this.data;
  }

  /**
   * Gets the data of the node.
   *
   * <p>If the node is currently available, the most up-to-date known version of
   * the data is returned.  If the node is not currently available, null is
   * returned.
   * @param refresh whether to refresh the data by calling ZK directly.
   * @return data of the node, null if unavailable
   */
  public synchronized byte [] getData(boolean refresh) {
    if (refresh) {
      try {
        this.data = ZKUtil.getDataAndWatch(watcher, node);
      } catch(KeeperException e) {
        abortable.abort("Unexpected exception handling getData", e);
      }
    }
    return this.data;
  }

  public String getNode() {
    return this.node;
  }

  @Override
  public synchronized void nodeCreated(String path) {
    if (!path.equals(node)) {
      return;
    }

    try {
      byte [] data = ZKUtil.getDataAndWatch(watcher, node);
      if (data != null) {
        this.data = data;
        notifyAll();
      } else {
        nodeDeleted(path);
      }
    } catch(KeeperException e) {
      abortable.abort("Unexpected exception handling nodeCreated event", e);
    }
  }

  @Override
  public synchronized void nodeDeleted(String path) {
    if(path.equals(node)) {
      try {
        if(ZKUtil.watchAndCheckExists(watcher, node)) {
          nodeCreated(path);
        } else {
          this.data = null;
        }
      } catch(KeeperException e) {
        abortable.abort("Unexpected exception handling nodeDeleted event", e);
      }
    }
  }

  @Override
  public synchronized void nodeDataChanged(String path) {
    if(path.equals(node)) {
      nodeCreated(path);
    }
  }

  /**
   * Checks if the baseznode set as per the property 'zookeeper.znode.parent'
   * exists.
   * @return true if baseznode exists.
   *         false if doesnot exists.
   */
  public boolean checkIfBaseNodeAvailable() {
    try {
      if (ZKUtil.checkExists(watcher, watcher.getZNodePaths().baseZNode) == -1) {
        return false;
      }
    } catch (KeeperException e) {
      abortable.abort("Exception while checking if basenode (" + watcher.getZNodePaths().baseZNode
          + ") exists in ZooKeeper.",
        e);
    }
    return true;
  }

  @Override
  public String toString() {
    return "ZKNodeTracker{" +
        "node='" + node + ", stopped=" + stopped + '}';
  }
}
