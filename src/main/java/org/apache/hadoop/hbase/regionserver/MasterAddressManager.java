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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.ServerController;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Manages the location of the current active Master for this RegionServer.
 *
 * Listens for ZooKeeper events related to the master address. The node /master
 * will contain the address of the current master. This listener is interested
 * in NodeDeleted and NodeCreated events on /master.
 *
 * This class is thread-safe and takes care of re-setting all watchers to
 * ensure it always knows the up-to-date master.  To kick it off, instantiate
 * the class and run the {@link #monitorMaster()} method.
 *
 * You can get the current master via {@link #getMasterAddress()} or the
 * blocking method {@link #waitMasterAddress()}.
 */
public class MasterAddressManager extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(MasterAddressManager.class);

  // Address of the current primary master, null if no primary master
  private HServerAddress masterAddress;

  // Status and controller for the regionserver
  private ServerController status;

  /**
   * Construct a master address listener with the specified zookeeper reference.
   *
   * This constructor does not trigger any actions, you must call methods
   * explicitly.  Normally you will just want to execute {@link #monitorMaster()}
   * and you will ensure to
   *
   * @param watcher zk reference and watcher
   */
  public MasterAddressManager(ZooKeeperWatcher watcher, ServerController status) {
    super(watcher);
    this.status = status;
    this.masterAddress = null;
  }

  /**
   * Get the address of the current master if one is available.  Returns null
   * if no current master.
   *
   * Use {@link #waitMasterAddress} if you want to block until the master is
   * available.
   * @return server address of current active master, or null if none available
   */
  public synchronized HServerAddress getMasterAddress() {
    return masterAddress;
  }

  /**
   * Check if there is a master available.
   * @return true if there is a master set, false if not.
   */
  public synchronized boolean hasMaster() {
    return masterAddress != null;
  }

  /**
   * Get the address of the current master.  If no master is available, method
   * will block until one is available, the thread is interrupted, or timeout
   * has passed.
   *
   * TODO: Make this work, currently unused, kept with existing retry semantics.
   *
   * @return server address of current active master, null if timed out
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public synchronized HServerAddress waitForMaster()
  throws InterruptedException {
    return masterAddress;
  }

  /**
   * Setup to watch for the primary master of the cluster.
   *
   * If the master is already available in ZooKeeper, this method will ensure
   * it gets set and that any further changes are also watched for.
   *
   * If no master is available, this method ensures we become aware of it and
   * will take care of setting it.
   */
  public void monitorMaster() {
    try {
      if(ZKUtil.watchAndCheckExists(watcher, watcher.masterAddressZNode)) {
        handleNewMaster();
      }
    } catch(KeeperException ke) {
      // If we have a ZK exception trying to find the master we must abort
      LOG.fatal("Unexpected ZooKeeper exception", ke);
      status.abort();
    }
  }

  @Override
  public void nodeCreated(String path) {
    LOG.info("nodeCreated(" + path + ")");
    if(path.equals(watcher.masterAddressZNode)) {
      handleNewMaster();
    }
    monitorMaster();
  }

  @Override
  public void nodeDeleted(String path) {
    if(path.equals(watcher.masterAddressZNode)) {
      handleDeadMaster();
    }
    monitorMaster();
  }

  /**
   * Set the master address to the specified address.  This operation is
   * idempotent, a master will only be set if there is currently no master set.
   */
  private synchronized void setMasterAddress(HServerAddress address) {
    if(masterAddress == null) {
      LOG.info("Found and set master address: " + address);
      masterAddress = address;
    }
  }

  /**
   * Unsets the master address.  Used when the master goes offline so none is
   * available.
   */
  private synchronized void unsetMasterAddress() {
    if(masterAddress != null) {
      LOG.info("Master has been unset.  There is no current master available");
      masterAddress = null;
    }
  }

  /**
   * Handle a new master being set.
   *
   * This method should be called to check if there is a new master.  If there
   * is already a master set, this method returns immediately.  If none is set,
   * this will attempt to grab the master location from ZooKeeper and will set
   * it.
   *
   * This method uses an atomic operation to ensure a new master is only set
   * once.
   */
  private void handleNewMaster() {
    if(hasMaster()) {
      return;
    }
    HServerAddress address = null;
    try {
      address = ZKUtil.getDataAsAddress(watcher, watcher.masterAddressZNode);
    } catch (KeeperException ke) {
      // If we have a ZK exception trying to find the master we must abort
      LOG.fatal("Unexpected ZooKeeper exception", ke);
      status.abort();
    }
    if(address != null) {
      setMasterAddress(address);
    }
  }

  /**
   * Handle a master failure.
   *
   * Triggered when a master node is deleted.
   *
   * TODO: Other ways we figure master is "dead"?  What do we do if set in ZK
   *       but we can't communicate with TCP?
   */
  private void handleDeadMaster() {
    unsetMasterAddress();
  }
}
