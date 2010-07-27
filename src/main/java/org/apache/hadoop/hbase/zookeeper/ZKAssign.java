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
package org.apache.hadoop.hbase.zookeeper;

import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;

/**
 * Utility class for doing region assignment in ZooKeeper.  This class extends
 * stuff done in {@link ZKUtil} to cover specific assignment operations.
 * <p>
 * Contains only static methods and constants.
 * <p>
 * Used by both the Master and RegionServer.
 * <p>
 * All valid transitions outlined below:
 * <p>
 * <b>MASTER</b>
 * <ol>
 *   <li>
 *     Master creates an unassigned node as OFFLINE.
 *     - Cluster startup and table enabling.
 *   </li>
 *   <li>
 *     Master forces an existing unassigned node to OFFLINE.
 *     - RegionServer failure.
 *     - Allows transitions from all states to OFFLINE.
 *   </li>
 *   <li>
 *     Master deletes an unassigned node that was in a OPENED state.
 *     - Normal region transitions.  Besides cluster startup, no other deletions
 *     of unassigned nodes is allowed.
 *   </li>
 *   <li>
 *     Master deletes all unassigned nodes regardless of state.
 *     - Cluster startup before any assignment happens.
 *   </li>
 * </ol>
 * <p>
 * <b>REGIONSERVER</b>
 * <ol>
 *   <li>
 *     RegionServer creates an unassigned node as CLOSING.
 *     - All region closes will do this in response to a CLOSE RPC from Master.
 *     - A node can never be transitioned to CLOSING, only created.
 *   </li>
 *   <li>
 *     RegionServer transitions an unassigned node from CLOSING to CLOSED.
 *     - Normal region closes.  CAS operation.
 *   </li>
 *   <li>
 *     RegionServer transitions an unassigned node from OFFLINE to OPENING.
 *     - All region opens will do this in response to an OPEN RPC from the Master.
 *     - Normal region opens.  CAS operation.
 *   </li>
 *   <li>
 *     RegionServer transitions an unassigned node from OPENING to OPENED.
 *     - Normal region opens.  CAS operation.
 *   </li>
 * </ol>
 */
public class ZKAssign {

  /**
   * Gets the full path node name for the unassigned node for the specified
   * region.
   * @param zkw zk reference
   * @param regionName region name
   * @return full path node name
   */
  private static String getNodeName(ZooKeeperWatcher zkw, String regionName) {
    return ZKUtil.joinZNode(zkw.assignmentZNode, regionName);
  }

  /**
   * Gets the region name from the full path node name of an unassigned node.
   * @param path full zk path
   * @return region name
   */
  public static String getRegionName(ZooKeeperWatcher zkw, String path) {
    return path.substring(zkw.assignmentZNode.length()+1);
  }

  // Master methods

  /**
   * Creates a new unassigned node in the OFFLINE state for the specified
   * region.
   *
   * <p>Does not transition nodes from other states.  If a node already exists
   * for this region, a {@link NodeExistsException} will be thrown.
   *
   * <p>Sets a watcher on the unassigned region node if the method is
   * successful.
   *
   * <p>This method should only be used during cluster startup and the enabling
   * of a table.
   *
   * @param zkw zk reference
   * @param regionName region to be created as offline
   * @param serverName server event originates from
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NodeExistsException if node already exists
   */
  public static void createNodeOffline(ZooKeeperWatcher zkw, String regionName,
      String serverName)
  throws KeeperException, KeeperException.NodeExistsException {
    zkw.debug("Creating an unassigned node for " + regionName +
        " in an OFFLINE state");
    RegionTransitionData data = new RegionTransitionData(
        HBaseEventType.M2ZK_REGION_OFFLINE, regionName, serverName);
    synchronized(zkw.getNodes()) {
      String node = getNodeName(zkw, regionName);
      zkw.getNodes().add(node);
      ZKUtil.createAndWatch(zkw, node, data.getBytes());
    }
  }

  /**
   * Forces an existing unassigned node to the OFFLINE state for the specified
   * region.
   *
   * <p>Does not create a new node.  If a node does not already exist for this
   * region, a {@link NoNodeException} will be thrown.
   *
   * <p>Sets a watcher on the unassigned region node if the method is
   * successful.
   *
   * <p>This method should only be used during recovery of regionserver failure.
   *
   * @param zkw zk reference
   * @param regionName region to be forced as offline
   * @param serverName server event originates from
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NoNodeException if node does not exist
   */
  public static void forceNodeOffline(ZooKeeperWatcher zkw, String regionName,
      String serverName)
  throws KeeperException, KeeperException.NoNodeException {
    zkw.debug("Forcing an existing unassigned node for " + regionName +
        " to an OFFLINE state");
    RegionTransitionData data = new RegionTransitionData(
        HBaseEventType.M2ZK_REGION_OFFLINE, regionName, serverName);
    synchronized(zkw.getNodes()) {
      String node = getNodeName(zkw, regionName);
      zkw.getNodes().add(node);
      ZKUtil.setData(zkw, node, data.getBytes());
    }
  }

  /**
   * Deletes an existing unassigned node that is in the OPENED state for the
   * specified region.
   *
   * <p>If a node does not already exist for this region, a
   * {@link NoNodeException} will be thrown.
   *
   * <p>No watcher is set whether this succeeds or not.
   *
   * <p>Returns false if the node was not in the proper state but did exist.
   *
   * <p>This method is used during normal region transitions when a region
   * finishes successfully opening.  This is the Master acknowledging completion
   * of the specified regions transition.
   *
   * @param zkw zk reference
   * @param regionName opened region to be deleted from zk
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NoNodeException if node does not exist
   */
  public static boolean deleteOpenedNode(ZooKeeperWatcher zkw,
      String regionName)
  throws KeeperException, KeeperException.NoNodeException {
    zkw.debug("Deleting an existing unassigned node for " + regionName +
        " that is in a OPENED state");
    String node = getNodeName(zkw, regionName);
    Stat stat = new Stat();
    byte [] bytes = ZKUtil.getDataNoWatch(zkw, node, stat);
    if(bytes == null) {
      throw KeeperException.create(Code.NONODE);
    }
    RegionTransitionData data = RegionTransitionData.fromBytes(bytes);
    if(!data.getEventType().equals(HBaseEventType.RS2ZK_REGION_OPENED)) {
      zkw.warn("Attempting to delete an unassigned node in OPENED state but " +
          "node is in " + data.getEventType() + " state");
      return false;
    }
    synchronized(zkw.getNodes()) {
      // TODO: Does this go here or only if we successfully delete node?
      zkw.getNodes().remove(node);
      if(!ZKUtil.deleteNode(zkw, node, stat.getVersion())) {
        zkw.warn("Attempting to delete an unassigned node in OPENED state but " +
            "after verifying it was in OPENED state, we got a version mismatch");
        return false;
      }
      return true;
    }
  }

  /**
   * Deletes all unassigned nodes regardless of their state.
   *
   * <p>No watchers are set.
   *
   * <p>This method is used by the Master during cluster startup to clear out
   * any existing state from other cluster runs.
   *
   * @param zkw zk reference
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void deleteAllNodes(ZooKeeperWatcher zkw)
  throws KeeperException {
    zkw.debug("Deleting any existing unassigned nodes");
    ZKUtil.deleteChildrenRecursively(zkw, zkw.assignmentZNode);
  }

  // RegionServer methods

  /**
   * Creates a new unassigned node in the CLOSING state for the specified
   * region.
   *
   * <p>Does not transition nodes from any states.  If a node already exists
   * for this region, a {@link NodeExistsException} will be thrown.
   *
   * <p>Does not set any watches.
   *
   * <p>This method should only be used by a RegionServer when initiating a
   * close of a region after receiving a CLOSE RPC from the Master.
   *
   * @param zkw zk reference
   * @param regionName region to be created as closing
   * @param serverName server event originates from
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NodeExistsException if node already exists
   */
  public static void createNodeClosing(ZooKeeperWatcher zkw, String regionName,
      String serverName)
  throws KeeperException, KeeperException.NodeExistsException {
    zkw.debug("Creating an unassigned node for " + regionName +
    " in a CLOSING state");
    RegionTransitionData data = new RegionTransitionData(
        HBaseEventType.RS2ZK_REGION_CLOSING, regionName, serverName);
    synchronized(zkw.getNodes()) {
      String node = getNodeName(zkw, regionName);
      zkw.getNodes().add(node);
      ZKUtil.createAndWatch(zkw, node, data.getBytes());
    }
  }

  /**
   * Transitions an existing unassigned node for the specified region which is
   * currently in the CLOSING state to be in the CLOSED state.
   *
   * <p>Does not transition nodes from other states.  If for some reason the
   * node could not be transitioned, the method returns false.
   *
   * <p>This method can fail and return false for three different reasons:
   * <ul><li>Unassigned node for this region does not exist</li>
   * <li>Unassigned node for this region is not in CLOSING state</li>
   * <li>After verifying CLOSING state, update fails because of wrong version
   * (someone else already transitioned the node)</li>
   * </ul>
   *
   * <p>Does not set any watches.
   *
   * <p>This method should only be used by a RegionServer when initiating a
   * close of a region after receiving a CLOSE RPC from the Master.
   *
   * @param zkw zk reference
   * @param regionName region to be transitioned to closed
   * @param serverName server event originates from
   * @return true if transition was successful, false if not
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean transitionNodeClosed(ZooKeeperWatcher zkw,
      String regionName, String serverName)
  throws KeeperException {
    return transitionNode(zkw, regionName, serverName,
        HBaseEventType.RS2ZK_REGION_CLOSING,
        HBaseEventType.RS2ZK_REGION_CLOSED);
  }

  /**
   * Transitions an existing unassigned node for the specified region which is
   * currently in the OFFLINE state to be in the OPENING state.
   *
   * <p>Does not transition nodes from other states.  If for some reason the
   * node could not be transitioned, the method returns false.
   *
   * <p>This method can fail and return false for three different reasons:
   * <ul><li>Unassigned node for this region does not exist</li>
   * <li>Unassigned node for this region is not in OFFLINE state</li>
   * <li>After verifying OFFLINE state, update fails because of wrong version
   * (someone else already transitioned the node)</li>
   * </ul>
   *
   * <p>Does not set any watches.
   *
   * <p>This method should only be used by a RegionServer when initiating an
   * open of a region after receiving an OPEN RPC from the Master.
   *
   * @param zkw zk reference
   * @param regionName region to be transitioned to opening
   * @param serverName server event originates from
   * @return true if transition was successful, false if not
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean transitionNodeOpening(ZooKeeperWatcher zkw,
      String regionName, String serverName)
  throws KeeperException {
    return transitionNode(zkw, regionName, serverName,
        HBaseEventType.M2ZK_REGION_OFFLINE,
        HBaseEventType.RS2ZK_REGION_OPENING);
  }

  /**
   * Transitions an existing unassigned node for the specified region which is
   * currently in the OPENING state to be in the OPENED state.
   *
   * <p>Does not transition nodes from other states.  If for some reason the
   * node could not be transitioned, the method returns false.
   *
   * <p>This method can fail and return false for three different reasons:
   * <ul><li>Unassigned node for this region does not exist</li>
   * <li>Unassigned node for this region is not in OPENING state</li>
   * <li>After verifying OPENING state, update fails because of wrong version
   * (this should never actually happen since an RS only does this transition
   * following a transition to OPENING.  if two RS are conflicting, one would
   * fail the original transition to OPENING and not this transition)</li>
   * </ul>
   *
   * <p>Does not set any watches.
   *
   * <p>This method should only be used by a RegionServer when completing the
   * open of a region.
   *
   * @param zkw zk reference
   * @param regionName region to be transitioned to opened
   * @param serverName server event originates from
   * @return true if transition was successful, false if not
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean transitionNodeOpened(ZooKeeperWatcher zkw,
      String regionName, String serverName)
  throws KeeperException {
    return transitionNode(zkw, regionName, serverName,
        HBaseEventType.RS2ZK_REGION_OPENING,
        HBaseEventType.RS2ZK_REGION_OPENED);
  }

  /**
   * Private method that actually performs unassigned node transitions.
   *
   * <p>Attempts to transition the unassigned node for the specified region
   * from the expected state to the state in the specified transition data.
   *
   * <p>Method first reads existing data and verifies it is in the expected
   * state.  If the node does not exist or the node is not in the expected
   * state, the method returns false.
   *
   * <p>If the read state is what is expected, it attempts to write the new
   * state and data into the node.  When doing this, it includes the expected
   * version (determined when the existing state was verified) to ensure that
   * only one transition is successful.  If there is a version mismatch, the
   * method returns false.
   *
   * <p>If the write is successful, no watch is set and the method returns true.
   *
   * @param zkw zk reference
   * @param regionName region to be transitioned to opened
   * @param serverName server event originates from
   * @param beginState state the node must currently be in to do transition
   * @param endState state to transition node to if all checks pass
   * @return true if transition was successful, false if not
   * @throws KeeperException if unexpected zookeeper exception
   */
  private static boolean transitionNode(ZooKeeperWatcher zkw, String regionName,
      String serverName, HBaseEventType beginState, HBaseEventType endState)
  throws KeeperException {
    if(zkw.isDebugEnabled()) {
      zkw.debug("Attempting to transition node for " + regionName +
        " from " + beginState.toString() + " to " + endState.toString());
    }

    String node = getNodeName(zkw, regionName);

    // Read existing data of the node
    Stat stat = new Stat();
    byte [] existingBytes =
      ZKUtil.getDataNoWatch(zkw, node, stat);
    RegionTransitionData existingData =
      RegionTransitionData.fromBytes(existingBytes);

    // Verify it is in expected state
    if(!existingData.getEventType().equals(beginState)) {
      zkw.warn("Attempt to transition the unassigned node for " + regionName +
        " from " + beginState + " to " + endState + " failed, " +
        "the node existed but was in the state " + existingData.getEventType());
      return false;
    }

    // Write new data, ensuring data has not changed since we last read it
    try {
      RegionTransitionData data = new RegionTransitionData(endState,
          regionName, serverName);
      if(!ZKUtil.setData(zkw, node, data.getBytes(),
          stat.getVersion())) {
        zkw.warn("Attempt to transition the unassigned node for " + regionName +
        " from " + beginState + " to " + endState + " failed, " +
        "the node existed and was in the expected state but then when " +
        "setting data we got a version mismatch");
        return false;
      }
      if(zkw.isDebugEnabled()) {
        zkw.debug("Successfully transitioned node for " + regionName +
          " from " + beginState + " to " + endState);
      }
      return true;
    } catch (KeeperException.NoNodeException nne) {
      zkw.warn("Attempt to transition the unassigned node for " + regionName +
        " from " + beginState + " to " + endState + " failed, " +
        "the node existed and was in the expected state but then when " +
        "setting data it no longer existed");
      return false;
    }
  }

  /**
   * Gets the current data in the unassigned node for the specified region name
   * or fully-qualified path.
   *
   * <p>Returns null if the region does not currently have a node.
   *
   * <p>Sets a watch on the node if the node exists.
   *
   * @param watcher zk reference
   * @param pathOrRegionName fully-specified path or region name
   * @return data for the unassigned node
   * @throws KeeperException
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static RegionTransitionData getData(ZooKeeperWatcher zkw,
      String pathOrRegionName)
  throws KeeperException {
    String node = pathOrRegionName.startsWith("/") ?
        pathOrRegionName : getNodeName(zkw, pathOrRegionName);
    byte [] data = ZKUtil.getDataAndWatch(zkw, node);
    if(data == null) {
      return null;
    }
    return RegionTransitionData.fromBytes(data);
  }
}
