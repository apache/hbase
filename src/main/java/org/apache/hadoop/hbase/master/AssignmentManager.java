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
package org.apache.hadoop.hbase.master;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.master.handler.MasterCloseRegionHandler;
import org.apache.hadoop.hbase.master.handler.MasterOpenRegionHandler;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.NodeAndData;
import org.apache.zookeeper.KeeperException;

/**
 * Manages region assignment.
 *
 * <p>Monitors ZooKeeper for events related to regions in transition.
 *
 * <p>Handles existing regions in transition during master failover.
 */
public class AssignmentManager extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(AssignmentManager.class);

  private MasterStatus status;

  private ServerManager serverManager;

  private RegionManager regionManager;

  private String serverName;

//  TODO: Eventually RIT will move here?
//  private final Map<String,RegionState> regionsInTransition =
//    new TreeMap<String,RegionState>();

  /**
   * Constructs a new assignment manager.
   *
   * <p>This manager must be started with {@link #start()}.
   *
   * @param watcher zookeeper watcher
   * @param status master status
   */
  public AssignmentManager(ZooKeeperWatcher watcher, MasterStatus status,
      ServerManager serverManager, RegionManager regionManager) {
    super(watcher);
    this.status = status;
    this.serverManager = serverManager;
    this.regionManager = regionManager;
    serverName = status.getHServerAddress().toString();
  }

  /**
   * Starts the assignment manager.
   *
   * <p>This includes registering itself with ZooKeeper and handling
   * the initial state of whatever unassigned nodes already exist.
   * @throws KeeperException
   */
  public void start() throws KeeperException {
    watcher.registerListener(this);
    if(status.isClusterStartup()) {
      processStartup();
    } else {
      processFailover();
    }
  }

  public synchronized void processStartup() throws KeeperException {
    ZKAssign.deleteAllNodes(watcher);
    ZKUtil.listChildrenAndWatchForNewChildren(watcher, watcher.assignmentZNode);
  }

  /**
   * Handle failover.
   * @throws KeeperException
   */
  public synchronized void processFailover() throws KeeperException {
    List<String> nodes = ZKUtil.listChildrenAndWatchForNewChildren(watcher,
        watcher.assignmentZNode);
    if(nodes.isEmpty()) {
      LOG.info("No regions in transition in ZK, nothing to do for failover");
      return;
    }
    LOG.info("Failed-over master needs to process " + nodes.size() +
        " regions in transition");
    for(String regionName : nodes) {
      RegionTransitionData data = ZKAssign.getData(watcher, regionName);
      switch(data.getEventType()) {
        case M2ZK_REGION_OFFLINE:
          // TODO: Generate new assignment and send OPEN RPC
          break;
        case RS2ZK_REGION_CLOSING:
          // TODO: Only need to deal with timeouts.
          break;
        case RS2ZK_REGION_CLOSED:
          // TODO: Generate new assignment and send OPEN RPC
          break;
        case RS2ZK_REGION_OPENING:
          // TODO: Only need to deal with timeouts.
          break;
        case RS2ZK_REGION_OPENED:
          // TODO: Delete the node from ZK.  Region successfully opened but not
          //       acknowledged.
          break;
      }
    }
  }

  private synchronized void handleRegion(RegionTransitionData data) {
    switch(data.getEventType()) {
      case RS2ZK_REGION_CLOSED:
        new MasterCloseRegionHandler(data.getEventType(), serverManager,
            serverName, data.getRegionName(), data.getBytes())
        .submit();
        break;
      case RS2ZK_REGION_OPENED:
      case RS2ZK_REGION_OPENING:
        new MasterOpenRegionHandler(data.getEventType(), serverManager,
            serverName, data.getRegionName(), data.getBytes())
        .submit();
        break;
    }
  }

  // ZooKeeper events

  /**
   * New unassigned node has been created.
   *
   * <p>This happens when an RS begins the OPENING or CLOSING of a region by
   * creating an unassigned node.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further events</li>
   *   <li>Read and handle the state in the node</li>
   * </ol>
   */
  @Override
  public synchronized void nodeCreated(String path) {
    if(path.startsWith(watcher.assignmentZNode)) {
      try {
        RegionTransitionData data = ZKAssign.getData(watcher, path);
        if(data == null) {
          return;
        }
        handleRegion(data);
      } catch (KeeperException e) {
        LOG.error("Unexpected ZK exception reading unassigned node data", e);
        status.abort();
      }
    }
  }

  /**
   * Existing unassigned node has had data changed.
   *
   * <p>This happens when an RS transitions from OFFLINE to OPENING, or between
   * OPENING/OPENED and CLOSING/CLOSED.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further events</li>
   *   <li>Read and handle the state in the node</li>
   * </ol>
   */
  @Override
  public synchronized void nodeDataChanged(String path) {
    if(path.startsWith(watcher.assignmentZNode)) {
      try {
        RegionTransitionData data = ZKAssign.getData(watcher, path);
        if(data == null) {
          return;
        }
        handleRegion(data);
      } catch (KeeperException e) {
        LOG.error("Unexpected ZK exception reading unassigned node data", e);
        status.abort();
      }
    }
  }

  /**
   * New unassigned node has been created.
   *
   * <p>This happens when an RS begins the OPENING or CLOSING of a region by
   * creating an unassigned node.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further children changed events</li>
   *   <li>Watch all new children for changed events</li>
   *   <li>Read all children and handle them</li>
   * </ol>
   */
  @Override
  public synchronized void nodeChildrenChanged(String path) {
    if(path.equals(watcher.assignmentZNode)) {
      try {
        List<NodeAndData> newNodes = ZKUtil.watchAndGetNewChildren(watcher,
            watcher.assignmentZNode);
        for(NodeAndData newNode : newNodes) {
          LOG.debug("Handling new unassigned node: " + newNode);
          handleRegion(RegionTransitionData.fromBytes(newNode.getData()));
        }
      } catch(KeeperException e) {
        LOG.error("Unexpected ZK exception reading unassigned children", e);
        status.abort();
      }
    }
  }
}
