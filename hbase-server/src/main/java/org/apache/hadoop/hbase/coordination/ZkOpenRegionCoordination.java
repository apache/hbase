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
package org.apache.hadoop.hbase.coordination;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * ZK-based implementation of {@link OpenRegionCoordination}.
 */
@InterfaceAudience.Private
public class ZkOpenRegionCoordination implements OpenRegionCoordination {
  private static final Log LOG = LogFactory.getLog(ZkOpenRegionCoordination.class);

  private CoordinatedStateManager coordination;
  private final ZooKeeperWatcher watcher;

  public ZkOpenRegionCoordination(CoordinatedStateManager coordination,
                                  ZooKeeperWatcher watcher) {
    this.coordination = coordination;
    this.watcher = watcher;
  }

  //-------------------------------
  // Region Server-side operations
  //-------------------------------

  /**
   * @param r Region we're working on.
   * @return whether znode is successfully transitioned to OPENED state.
   * @throws java.io.IOException
   */
  @Override
  public boolean transitionToOpened(final HRegion r, OpenRegionDetails ord) throws IOException {
    ZkOpenRegionDetails zkOrd = (ZkOpenRegionDetails) ord;

    boolean result = false;
    HRegionInfo hri = r.getRegionInfo();
    final String name = hri.getRegionNameAsString();
    // Finally, Transition ZK node to OPENED
    try {
      if (ZKAssign.transitionNodeOpened(watcher, hri,
        zkOrd.getServerName(), zkOrd.getVersion()) == -1) {
        String warnMsg = "Completed the OPEN of region " + name +
          " but when transitioning from " + " OPENING to OPENED ";
        try {
          String node = ZKAssign.getNodeName(watcher, hri.getEncodedName());
          if (ZKUtil.checkExists(watcher, node) < 0) {
            // if the znode
            coordination.getServer().abort(warnMsg + "the znode disappeared", null);
          } else {
            LOG.warn(warnMsg + "got a version mismatch, someone else clashed; " +
              "so now unassigning -- closing region on server: " + zkOrd.getServerName());
          }
        } catch (KeeperException ke) {
          coordination.getServer().abort(warnMsg, ke);
        }
      } else {
        LOG.debug("Transitioned " + r.getRegionInfo().getEncodedName() +
          " to OPENED in zk on " + zkOrd.getServerName());
        result = true;
      }
    } catch (KeeperException e) {
      LOG.error("Failed transitioning node " + name +
        " from OPENING to OPENED -- closing region", e);
    }
    return result;
  }

  /**
   * Transition ZK node from OFFLINE to OPENING.
   * @param regionInfo region info instance
   * @param ord - instance of open region details, for ZK implementation
   *   will include version Of OfflineNode that needs to be compared
   *   before changing the node's state from OFFLINE
   * @return True if successful transition.
   */
  @Override
  public boolean transitionFromOfflineToOpening(HRegionInfo regionInfo,
                                                OpenRegionDetails ord) {
    ZkOpenRegionDetails zkOrd = (ZkOpenRegionDetails) ord;

    // encoded name is used as znode encoded name in ZK
    final String encodedName = regionInfo.getEncodedName();

    // TODO: should also handle transition from CLOSED?
    try {
      // Initialize the znode version.
      zkOrd.setVersion(ZKAssign.transitionNode(watcher, regionInfo,
        zkOrd.getServerName(), EventType.M_ZK_REGION_OFFLINE,
        EventType.RS_ZK_REGION_OPENING, zkOrd.getVersionOfOfflineNode()));
    } catch (KeeperException e) {
      LOG.error("Error transition from OFFLINE to OPENING for region=" +
        encodedName, e);
      zkOrd.setVersion(-1);
      return false;
    }
    boolean b = isGoodVersion(zkOrd);
    if (!b) {
      LOG.warn("Failed transition from OFFLINE to OPENING for region=" +
        encodedName);
    }
    return b;
  }

  /**
   * Update our OPENING state in zookeeper.
   * Do this so master doesn't timeout this region-in-transition.
   * We may lose the znode ownership during the open.  Currently its
   * too hard interrupting ongoing region open.  Just let it complete
   * and check we still have the znode after region open.
   *
   * @param context Some context to add to logs if failure
   * @return True if successful transition.
   */
  @Override
  public boolean tickleOpening(OpenRegionDetails ord, HRegionInfo regionInfo,
                               RegionServerServices rsServices, final String context) {
    ZkOpenRegionDetails zkOrd = (ZkOpenRegionDetails) ord;
    if (!isRegionStillOpening(regionInfo, rsServices)) {
      LOG.warn("Open region aborted since it isn't opening any more");
      return false;
    }
    // If previous checks failed... do not try again.
    if (!isGoodVersion(zkOrd)) return false;
    String encodedName = regionInfo.getEncodedName();
    try {
      zkOrd.setVersion(ZKAssign.confirmNodeOpening(watcher,
          regionInfo, zkOrd.getServerName(), zkOrd.getVersion()));
    } catch (KeeperException e) {
      coordination.getServer().abort("Exception refreshing OPENING; region=" + encodedName +
        ", context=" + context, e);
      zkOrd.setVersion(-1);
      return false;
    }
    boolean b = isGoodVersion(zkOrd);
    if (!b) {
      LOG.warn("Failed refreshing OPENING; region=" + encodedName +
        ", context=" + context);
    }
    return b;
  }

  /**
   * Try to transition to open.
   *
   * This is not guaranteed to succeed, we just do our best.
   *
   * @param rsServices
   * @param hri Region we're working on.
   * @param ord Details about region open task
   * @return whether znode is successfully transitioned to FAILED_OPEN state.
   */
  @Override
  public boolean tryTransitionFromOfflineToFailedOpen(RegionServerServices rsServices,
                                                      final HRegionInfo hri,
                                                      OpenRegionDetails ord) {
    ZkOpenRegionDetails zkOrd = (ZkOpenRegionDetails) ord;
    boolean result = false;
    final String name = hri.getRegionNameAsString();
    try {
      LOG.info("Opening of region " + hri + " failed, transitioning" +
        " from OFFLINE to FAILED_OPEN in ZK, expecting version " +
        zkOrd.getVersionOfOfflineNode());
      if (ZKAssign.transitionNode(
        rsServices.getZooKeeper(), hri,
        rsServices.getServerName(),
        EventType.M_ZK_REGION_OFFLINE,
        EventType.RS_ZK_REGION_FAILED_OPEN,
        zkOrd.getVersionOfOfflineNode()) == -1) {
        LOG.warn("Unable to mark region " + hri + " as FAILED_OPEN. " +
          "It's likely that the master already timed out this open " +
          "attempt, and thus another RS already has the region.");
      } else {
        result = true;
      }
    } catch (KeeperException e) {
      LOG.error("Failed transitioning node " + name + " from OFFLINE to FAILED_OPEN", e);
    }
    return result;
  }

  private boolean isGoodVersion(ZkOpenRegionDetails zkOrd) {
    return zkOrd.getVersion() != -1;
  }

  /**
   * This is not guaranteed to succeed, we just do our best.
   * @param hri Region we're working on.
   * @return whether znode is successfully transitioned to FAILED_OPEN state.
   */
  @Override
  public boolean tryTransitionFromOpeningToFailedOpen(final HRegionInfo hri,
                                                      OpenRegionDetails ord) {
    ZkOpenRegionDetails zkOrd = (ZkOpenRegionDetails) ord;
    boolean result = false;
    final String name = hri.getRegionNameAsString();
    try {
      LOG.info("Opening of region " + hri + " failed, transitioning" +
        " from OPENING to FAILED_OPEN in ZK, expecting version " + zkOrd.getVersion());
      if (ZKAssign.transitionNode(
        watcher, hri,
        zkOrd.getServerName(),
        EventType.RS_ZK_REGION_OPENING,
        EventType.RS_ZK_REGION_FAILED_OPEN,
        zkOrd.getVersion()) == -1) {
        LOG.warn("Unable to mark region " + hri + " as FAILED_OPEN. " +
          "It's likely that the master already timed out this open " +
          "attempt, and thus another RS already has the region.");
      } else {
        result = true;
      }
    } catch (KeeperException e) {
      LOG.error("Failed transitioning node " + name +
        " from OPENING to FAILED_OPEN", e);
    }
    return result;
  }

  /**
   * Parse ZK-related fields from request.
   */
  @Override
  public OpenRegionCoordination.OpenRegionDetails parseFromProtoRequest(
      AdminProtos.OpenRegionRequest.RegionOpenInfo regionOpenInfo) {
    ZkOpenRegionCoordination.ZkOpenRegionDetails zkCrd =
      new ZkOpenRegionCoordination.ZkOpenRegionDetails();

    int versionOfOfflineNode = -1;
    if (regionOpenInfo.hasVersionOfOfflineNode()) {
      versionOfOfflineNode = regionOpenInfo.getVersionOfOfflineNode();
    }
    zkCrd.setVersionOfOfflineNode(versionOfOfflineNode);
    zkCrd.setServerName(coordination.getServer().getServerName());

    return zkCrd;
  }

  /**
   * No ZK tracking will be performed for that case.
   * This method should be used when we want to construct CloseRegionDetails,
   * but don't want any coordination on that (when it's initiated by regionserver),
   * so no znode state transitions will be performed.
   */
  @Override
  public OpenRegionCoordination.OpenRegionDetails getDetailsForNonCoordinatedOpening() {
    ZkOpenRegionCoordination.ZkOpenRegionDetails zkCrd =
      new ZkOpenRegionCoordination.ZkOpenRegionDetails();
    zkCrd.setVersionOfOfflineNode(-1);
    zkCrd.setServerName(coordination.getServer().getServerName());

    return zkCrd;
  }

  //--------------------------
  // HMaster-side operations
  //--------------------------
  @Override
  public boolean commitOpenOnMasterSide(AssignmentManager assignmentManager,
                                        HRegionInfo regionInfo,
                                        OpenRegionDetails ord) {
    boolean committedSuccessfully = true;

    // Code to defend against case where we get SPLIT before region open
    // processing completes; temporary till we make SPLITs go via zk -- 0.92.
    RegionState regionState = assignmentManager.getRegionStates()
      .getRegionTransitionState(regionInfo.getEncodedName());
    boolean openedNodeDeleted = false;
    if (regionState != null && regionState.isOpened()) {
      openedNodeDeleted = deleteOpenedNode(regionInfo, ord);
      if (!openedNodeDeleted) {
        LOG.error("Znode of region " + regionInfo.getShortNameToLog() + " could not be deleted.");
      }
    } else {
      LOG.warn("Skipping the onlining of " + regionInfo.getShortNameToLog() +
        " because regions is NOT in RIT -- presuming this is because it SPLIT");
    }
    if (!openedNodeDeleted) {
      if (assignmentManager.getTableStateManager().isTableState(regionInfo.getTable(),
          TableState.State.DISABLED, TableState.State.DISABLING)) {
        debugLog(regionInfo, "Opened region "
          + regionInfo.getShortNameToLog() + " but "
          + "this table is disabled, triggering close of region");
        committedSuccessfully = false;
      }
    }

    return committedSuccessfully;
  }

  private boolean deleteOpenedNode(HRegionInfo regionInfo, OpenRegionDetails ord) {
    ZkOpenRegionDetails zkOrd = (ZkOpenRegionDetails) ord;
    int expectedVersion = zkOrd.getVersion();

    debugLog(regionInfo, "Handling OPENED of " +
      regionInfo.getShortNameToLog() + " from " + zkOrd.getServerName().toString() +
      "; deleting unassigned node");
    try {
      // delete the opened znode only if the version matches.
      return ZKAssign.deleteNode(this.coordination.getServer().getZooKeeper(),
        regionInfo.getEncodedName(), EventType.RS_ZK_REGION_OPENED, expectedVersion);
    } catch(KeeperException.NoNodeException e){
      // Getting no node exception here means that already the region has been opened.
      LOG.warn("The znode of the region " + regionInfo.getShortNameToLog() +
        " would have already been deleted");
      return false;
    } catch (KeeperException e) {
      this.coordination.getServer().abort("Error deleting OPENED node in ZK (" +
        regionInfo.getRegionNameAsString() + ")", e);
    }
    return false;
  }

  private void debugLog(HRegionInfo region, String string) {
    if (region.isMetaTable()) {
      LOG.info(string);
    } else {
      LOG.debug(string);
    }
  }

  // Additional classes and helper methods

  /**
   * ZK-based implementation. Has details about whether the state transition should be
   * reflected in ZK, as well as expected version of znode.
   */
  public static class ZkOpenRegionDetails implements OpenRegionCoordination.OpenRegionDetails {

    // We get version of our znode at start of open process and monitor it across
    // the total open. We'll fail the open if someone hijacks our znode; we can
    // tell this has happened if version is not as expected.
    private volatile int version = -1;

    //version of the offline node that was set by the master
    private volatile int versionOfOfflineNode = -1;

    /**
     * Server name the handler is running on.
     */
    private ServerName serverName;

    public ZkOpenRegionDetails() {
    }

    public ZkOpenRegionDetails(int versionOfOfflineNode) {
      this.versionOfOfflineNode = versionOfOfflineNode;
    }

    public int getVersionOfOfflineNode() {
      return versionOfOfflineNode;
    }

    public void setVersionOfOfflineNode(int versionOfOfflineNode) {
      this.versionOfOfflineNode = versionOfOfflineNode;
    }

    public int getVersion() {
      return version;
    }

    public void setVersion(int version) {
      this.version = version;
    }

    @Override
    public ServerName getServerName() {
      return serverName;
    }

    @Override
    public void setServerName(ServerName serverName) {
      this.serverName = serverName;
    }
  }

  private boolean isRegionStillOpening(HRegionInfo regionInfo, RegionServerServices rsServices) {
    byte[] encodedName = regionInfo.getEncodedNameAsBytes();
    Boolean action = rsServices.getRegionsInTransitionInRS().get(encodedName);
    return Boolean.TRUE.equals(action); // true means opening for RIT
  }
}
