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
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * ZK-based implementation of {@link CloseRegionCoordination}.
 */
@InterfaceAudience.Private
public class ZkCloseRegionCoordination implements CloseRegionCoordination {
  private static final Log LOG = LogFactory.getLog(ZkCloseRegionCoordination.class);

  private final static int FAILED_VERSION = -1;

  private CoordinatedStateManager csm;
  private final ZooKeeperWatcher watcher;

  public ZkCloseRegionCoordination(CoordinatedStateManager csm, ZooKeeperWatcher watcher) {
    this.csm = csm;
    this.watcher = watcher;
  }

  /**
   * In ZK-based version we're checking for bad znode state, e.g. if we're
   * trying to delete the znode, and it's not ours (version doesn't match).
   */
  @Override
  public boolean checkClosingState(HRegionInfo regionInfo, CloseRegionDetails crd) {
    ZkCloseRegionDetails zkCrd = (ZkCloseRegionDetails) crd;

    try {
      return zkCrd.isPublishStatusInZk() && !ZKAssign.checkClosingState(watcher,
        regionInfo, ((ZkCloseRegionDetails) crd).getExpectedVersion());
    } catch (KeeperException ke) {
       csm.getServer().abort("Unrecoverable exception while checking state with zk " +
          regionInfo.getRegionNameAsString() + ", still finishing close", ke);
        throw new RuntimeException(ke);
    }
  }

  /**
   * In ZK-based version we do some znodes transitioning.
   */
  @Override
  public void setClosedState(HRegion region, ServerName sn, CloseRegionDetails crd) {
    ZkCloseRegionDetails zkCrd = (ZkCloseRegionDetails) crd;
    String name = region.getRegionInfo().getRegionNameAsString();

    if (zkCrd.isPublishStatusInZk()) {
      if (setClosedState(region,sn, zkCrd)) {
        LOG.debug("Set closed state in zk for " + name + " on " + sn);
      } else {
        LOG.debug("Set closed state in zk UNSUCCESSFUL for " + name + " on " + sn);
      }
    }
  }

  /**
   * Parse ZK-related fields from request.
   */
  @Override
  public CloseRegionDetails parseFromProtoRequest(AdminProtos.CloseRegionRequest request) {
    ZkCloseRegionCoordination.ZkCloseRegionDetails zkCrd =
      new ZkCloseRegionCoordination.ZkCloseRegionDetails();
    zkCrd.setPublishStatusInZk(request.getTransitionInZK());
    int versionOfClosingNode = -1;
    if (request.hasVersionOfClosingNode()) {
      versionOfClosingNode = request.getVersionOfClosingNode();
    }
    zkCrd.setExpectedVersion(versionOfClosingNode);

    return zkCrd;
  }

  /**
   * No ZK tracking will be performed for that case.
   * This method should be used when we want to construct CloseRegionDetails,
   * but don't want any coordination on that (when it's initiated by regionserver),
   * so no znode state transitions will be performed.
   */
  @Override
  public CloseRegionDetails getDetaultDetails() {
    ZkCloseRegionCoordination.ZkCloseRegionDetails zkCrd =
      new ZkCloseRegionCoordination.ZkCloseRegionDetails();
    zkCrd.setPublishStatusInZk(false);
    zkCrd.setExpectedVersion(FAILED_VERSION);

    return zkCrd;
  }

  /**
   * Transition ZK node to CLOSED
   * @param region HRegion instance being closed
   * @param sn ServerName on which task runs
   * @param zkCrd  details about region closing operation.
   * @return If the state is set successfully
   */
  private boolean setClosedState(final HRegion region,
                                 ServerName sn,
                                 ZkCloseRegionDetails zkCrd) {
    final int expectedVersion = zkCrd.getExpectedVersion();

    try {
      if (ZKAssign.transitionNodeClosed(watcher, region.getRegionInfo(),
        sn, expectedVersion) == FAILED_VERSION) {
        LOG.warn("Completed the CLOSE of a region but when transitioning from " +
          " CLOSING to CLOSED got a version mismatch, someone else clashed " +
          "so now unassigning");
        region.close();
        return false;
      }
    } catch (NullPointerException e) {
      // I've seen NPE when table was deleted while close was running in unit tests.
      LOG.warn("NPE during close -- catching and continuing...", e);
      return false;
    } catch (KeeperException e) {
      LOG.error("Failed transitioning node from CLOSING to CLOSED", e);
      return false;
    } catch (IOException e) {
      LOG.error("Failed to close region after failing to transition", e);
      return false;
    }
    return true;
  }

  /**
   * ZK-based implementation. Has details about whether the state transition should be
   * reflected in ZK, as well as expected version of znode.
   */
  public static class ZkCloseRegionDetails implements CloseRegionCoordination.CloseRegionDetails {

    /**
     * True if we are to update zk about the region close; if the close
     * was orchestrated by master, then update zk.  If the close is being run by
     * the regionserver because its going down, don't update zk.
     * */
    private boolean publishStatusInZk;

    /**
     * The version of znode to compare when RS transitions the znode from
     * CLOSING state.
     */
    private int expectedVersion = FAILED_VERSION;

    public ZkCloseRegionDetails() {
    }

    public ZkCloseRegionDetails(boolean publishStatusInZk, int expectedVersion) {
      this.publishStatusInZk = publishStatusInZk;
      this.expectedVersion = expectedVersion;
    }

    public boolean isPublishStatusInZk() {
      return publishStatusInZk;
    }

    public void setPublishStatusInZk(boolean publishStatusInZk) {
      this.publishStatusInZk = publishStatusInZk;
    }

    public int getExpectedVersion() {
      return expectedVersion;
    }

    public void setExpectedVersion(int expectedVersion) {
      this.expectedVersion = expectedVersion;
    }
  }
}
