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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.executor.RegionTransitionEventData;
import org.apache.hadoop.hbase.master.handler.MasterCloseRegionHandler;
import org.apache.hadoop.hbase.master.handler.MasterOpenRegionHandler;
import org.apache.hadoop.hbase.util.DrainableQueue;
import org.apache.hadoop.hbase.util.ParamCallable;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZNodeEventData;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.hbase.util.InjectionEvent;
import org.apache.hadoop.hbase.util.InjectionHandler;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * Watches the UNASSIGNED znode in ZK for the master, and handles all events
 * relating to region transitions.
 */
public class ZKUnassignedWatcher implements Watcher {
  private static final Log LOG = LogFactory.getLog(ZKUnassignedWatcher.class);

  private final ZooKeeperWrapper zkWrapper;
  private final String serverName;
  private final RegionManager regionManager;
  private final ServerManager serverManager;
  private final String unassignedZNode;

  private DrainableQueue<ZNodeEventData> delayedZKEvents =
      new DrainableQueue<ZNodeEventData>("delayedZKEvents");

  private List<String> unassignedDirSnapshot = new ArrayList<String>();

  private ParamCallable<ZNodeEventData> processEvent = new ParamCallable<ZNodeEventData>() {
    @Override
    public void call(ZNodeEventData pathAndData) {
      try {
        handleRegionStateInZK(pathAndData.getEventType(),
            pathAndData.getzNodePath(), pathAndData.getData(), false);
      } catch (IOException e) {
        LOG.error("Could not process event from ZooKeeper", e);
      }
    }
  };
  
  ZKUnassignedWatcher(HMaster master) throws IOException {
    LOG.debug("Started ZKUnassigned watcher");
    this.serverName = master.getHServerAddress().toString();
    this.regionManager = master.getRegionManager();
    this.serverManager = master.getServerManager();
    zkWrapper = ZooKeeperWrapper.getInstance(master.getConfiguration(), master.getZKWrapperName());
    unassignedZNode = zkWrapper.getRegionInTransitionZNode();

    // Set a watch on Zookeeper's UNASSIGNED node if it exists.
    zkWrapper.registerListener(this);

    if (zkWrapper.exists(unassignedZNode, false)) {
      // The unassigned directory already exists in ZK. Take a snapshot of unassigned regions.
      try {
        unassignedDirSnapshot = zkWrapper.listChildrenAndWatchForNewChildren(unassignedZNode);
      } catch (KeeperException ke) {
        throw new IOException(ke);
      }
    } else {
      zkWrapper.createZNodeIfNotExists(unassignedZNode);  // create and watch
    }
  }

  /**
   * This is the processing loop that gets triggered from the ZooKeeperWrapper.
   * This zookeeper events process function dies the following:
   *   - WATCHES the following events: NodeCreated, NodeDataChanged, NodeChildrenChanged
   *   - IGNORES the following events: None, NodeDeleted
   */
  @Override
  public synchronized void process(WatchedEvent event) {
    EventType eventType = event.getType();
    // Handle the ignored events
    if(eventType.equals(EventType.None)       ||
       eventType.equals(EventType.NodeDeleted)) {
      return;
    }

    // check if the path is for the UNASSIGNED directory we care about
    if (event.getPath() == null || !event.getPath().startsWith(unassignedZNode)) {
      return;
    }

    LOG.debug("ZK-EVENT-PROCESS: Got zkEvent " + eventType +
              " path:" + event.getPath());
 
    try
    {
      /*
       * If a node is created in the UNASSIGNED directory in zookeeper, then:
       *   1. watch its updates (this is an unassigned region).
       *   2. read to see what its state is and handle as needed (state may have
       *      changed before we started watching it)
       */
      if(eventType.equals(EventType.NodeCreated)) {
        zkWrapper.watchZNode(event.getPath());
        handleRegionStateInZK(eventType, event.getPath());
      }
      /*
       * Data on some node has changed. Read to see what the state is and handle
       * as needed.
       */
      else if(eventType.equals(EventType.NodeDataChanged)) {
        handleRegionStateInZK(eventType, event.getPath());
      }
      /*
       * If there were some nodes created then watch those nodes
       */
      else if(eventType.equals(EventType.NodeChildrenChanged)) {
        List<ZNodeEventData> newZNodes =
            zkWrapper.watchAndGetNewChildren(event.getPath());
        for(ZNodeEventData zNodePathAndData : newZNodes) {
          LOG.debug("Handling updates for znode: " + zNodePathAndData.getzNodePath());
          handleRegionStateInZK(eventType, zNodePathAndData.getzNodePath(),
              zNodePathAndData.getData(), true);
        }
      }
    }
    catch (IOException e)
    {
      LOG.error("Could not process event from ZooKeeper", e);
    }
  }

  /**
   * Read the state of a node in ZK, and do the needful. We want to do the
   * following:
   *   1. If region's state is updated as CLOSED, invoke the ClosedRegionHandler.
   *   2. If region's state is updated as OPENED, invoke the OpenRegionHandler.
   * @param eventType ZK event type
   * @param zNodePath unassigned region znode
   * @throws IOException
   */
  private void handleRegionStateInZK(EventType eventType, String zNodePath) throws IOException {
    byte[] data = zkWrapper.readZNode(zNodePath, null);
    handleRegionStateInZK(eventType, zNodePath, data, true);
  }

  public void handleRegionStateInZK(EventType eventType, String zNodePath,
      byte[] data, boolean canDefer) throws IOException {
    // a null value is set when a node is created, we don't need to handle this
    if(data == null) {
      return;
    }

    String region = zNodePath.substring(
        zNodePath.indexOf(unassignedZNode) + unassignedZNode.length() + 1);

    if (eventType == EventType.NodeCreated && regionManager.regionIsInTransition(region)) {
      // Since the master is the only one who can create unassigned znodes, we generally don't need
      // to handle this event. However, there is an unlikely case that the previous incarnation of
      // the master died after creating the znode and the new master came up quickly enough to
      // catch the NodeCreated event. We distinguish between these two cases by checking if the
      // region is in transition.
      LOG.debug("Ignoring " + eventType + " for " + region + ": already in transition");
      return;
    }

    HBaseEventType rsEvent = HBaseEventType.fromByte(data[0]);
    LOG.debug("Got event type [ " + rsEvent + " ] for region " + region +
        " triggered by " + eventType);

    RegionTransitionEventData rt = new RegionTransitionEventData();
    Writables.getWritable(data, rt);


    if (canDefer && delayedZKEvents.canEnqueue()) {
      ZNodeEventData pathAndData = new ZNodeEventData(eventType, zNodePath, data);
      if (delayedZKEvents.enqueue(pathAndData)) {
        // We will process this event after the initial scan of the unassigned directory is done.
        LOG.debug("ZK-EVENT-PROCESS: deferring processing of event " + rsEvent + ", path "
            + zNodePath + " until master startup is complete");
        return;
      }
    }

    // if the node was CLOSED then handle it
    if(rsEvent == HBaseEventType.RS2ZK_REGION_CLOSED) {
      new MasterCloseRegionHandler(rsEvent, serverManager, serverName, region, data).submit();
    }
    // if the region was OPENED then handle that
    else if(rsEvent == HBaseEventType.RS2ZK_REGION_OPENED ||
            rsEvent == HBaseEventType.RS2ZK_REGION_OPENING) {
      new MasterOpenRegionHandler(rsEvent, serverManager, serverName, region,
          data).submit();

      // For testing purposes
      if (rsEvent == HBaseEventType.RS2ZK_REGION_OPENED) {
        InjectionHandler.processEvent(InjectionEvent.ZKUNASSIGNEDWATCHER_REGION_OPENED,
            this, eventType, zNodePath, data);
      }
    }
  }

  void drainZKEventQueue() {
    LOG.info("Draining ZK unassigned event queue");
    delayedZKEvents.drain(processEvent);
    LOG.info("Finished draining ZK unassigned event queue");
  }

  /** @return a snapshot of the ZK unassigned directory taken when we set the watch */
  List<String> getUnassignedDirSnapshot() {
    return unassignedDirSnapshot;
  }
  
}
