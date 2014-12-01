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

package org.apache.hadoop.hbase.coordination;

import static org.apache.hadoop.hbase.executor.EventType.RS_ZK_REGION_MERGED;
import static org.apache.hadoop.hbase.executor.EventType.RS_ZK_REGION_MERGING;
import static org.apache.hadoop.hbase.executor.EventType.RS_ZK_REQUEST_REGION_MERGE;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public class ZkRegionMergeCoordination implements RegionMergeCoordination {

  private CoordinatedStateManager manager;
  private final ZooKeeperWatcher watcher;

  private static final Log LOG = LogFactory.getLog(ZkRegionMergeCoordination.class);

  public ZkRegionMergeCoordination(CoordinatedStateManager manager,
      ZooKeeperWatcher watcher) {
    this.manager = manager;
    this.watcher = watcher;
  }

  /**
   * ZK-based implementation. Has details about whether the state transition should be reflected in
   * ZK, as well as expected version of znode.
   */
  public static class ZkRegionMergeDetails implements RegionMergeCoordination.RegionMergeDetails {
    private int znodeVersion;

    public ZkRegionMergeDetails() {
    }

    public int getZnodeVersion() {
      return znodeVersion;
    }

    public void setZnodeVersion(int znodeVersion) {
      this.znodeVersion = znodeVersion;
    }
  }

  @Override
  public RegionMergeDetails getDefaultDetails() {
    ZkRegionMergeDetails zstd = new ZkRegionMergeDetails();
    zstd.setZnodeVersion(-1);
    return zstd;
  }

  /**
   * Wait for the merging node to be transitioned from pending_merge
   * to merging by master. That's how we are sure master has processed
   * the event and is good with us to move on. If we don't get any update,
   * we periodically transition the node so that master gets the callback.
   * If the node is removed or is not in pending_merge state any more,
   * we abort the merge.
   * @throws IOException
   */

  @Override
  public void waitForRegionMergeTransaction(RegionServerServices services,
      HRegionInfo mergedRegionInfo, HRegion region_a, HRegion region_b, RegionMergeDetails details)
      throws IOException {
    try {
      int spins = 0;
      Stat stat = new Stat();
      ServerName expectedServer = manager.getServer().getServerName();
      String node = mergedRegionInfo.getEncodedName();
      ZkRegionMergeDetails zdetails = (ZkRegionMergeDetails) details;
      while (!(manager.getServer().isStopped() || services.isStopping())) {
        if (spins % 5 == 0) {
          LOG.debug("Still waiting for master to process " + "the pending_merge for " + node);
          ZkRegionMergeDetails zrmd = (ZkRegionMergeDetails) getDefaultDetails();
          transitionMergingNode(mergedRegionInfo, region_a.getRegionInfo(),
            region_b.getRegionInfo(), expectedServer, zrmd, RS_ZK_REQUEST_REGION_MERGE,
            RS_ZK_REQUEST_REGION_MERGE);
        }
        Thread.sleep(100);
        spins++;
        byte[] data = ZKAssign.getDataNoWatch(watcher, node, stat);
        if (data == null) {
          throw new IOException("Data is null, merging node " + node + " no longer exists");
        }
        RegionTransition rt = RegionTransition.parseFrom(data);
        EventType et = rt.getEventType();
        if (et == RS_ZK_REGION_MERGING) {
          ServerName serverName = rt.getServerName();
          if (!serverName.equals(expectedServer)) {
            throw new IOException("Merging node " + node + " is for " + serverName + ", not us "
                + expectedServer);
          }
          byte[] payloadOfMerging = rt.getPayload();
          List<HRegionInfo> mergingRegions =
              HRegionInfo.parseDelimitedFrom(payloadOfMerging, 0, payloadOfMerging.length);
          assert mergingRegions.size() == 3;
          HRegionInfo a = mergingRegions.get(1);
          HRegionInfo b = mergingRegions.get(2);
          HRegionInfo hri_a = region_a.getRegionInfo();
          HRegionInfo hri_b = region_b.getRegionInfo();
          if (!(hri_a.equals(a) && hri_b.equals(b))) {
            throw new IOException("Merging node " + node + " is for " + a + ", " + b
                + ", not expected regions: " + hri_a + ", " + hri_b);
          }
          // Master has processed it.
          zdetails.setZnodeVersion(stat.getVersion());
          return;
        }
        if (et != RS_ZK_REQUEST_REGION_MERGE) {
          throw new IOException("Merging node " + node + " moved out of merging to " + et);
        }
      }
      // Server is stopping/stopped
      throw new IOException("Server is " + (services.isStopping() ? "stopping" : "stopped"));
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IOException("Failed getting MERGING znode on "
          + mergedRegionInfo.getRegionNameAsString(), e);
    }
  }

  /**
   * Creates a new ephemeral node in the PENDING_MERGE state for the merged region.
   * Create it ephemeral in case regionserver dies mid-merge.
   *
   * <p>
   * Does not transition nodes from other states. If a node already exists for
   * this region, a {@link org.apache.zookeeper.KeeperException.NodeExistsException} will be thrown.
   *
   * @param region region to be created as offline
   * @param serverName server event originates from
   * @throws IOException
   */
  @Override
  public void startRegionMergeTransaction(final HRegionInfo region, final ServerName serverName,
      final HRegionInfo a, final HRegionInfo b) throws IOException {
    LOG.debug(watcher.prefix("Creating ephemeral node for " + region.getEncodedName()
        + " in PENDING_MERGE state"));
    byte[] payload = HRegionInfo.toDelimitedByteArray(region, a, b);
    RegionTransition rt =
        RegionTransition.createRegionTransition(RS_ZK_REQUEST_REGION_MERGE, region.getRegionName(),
          serverName, payload);
    String node = ZKAssign.getNodeName(watcher, region.getEncodedName());
    try {
      if (!ZKUtil.createEphemeralNodeAndWatch(watcher, node, rt.toByteArray())) {
        throw new IOException("Failed create of ephemeral " + node);
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hadoop.hbase.regionserver.coordination.RegionMergeCoordination#clean(org.apache.hadoop
   * .hbase.Server, org.apache.hadoop.hbase.HRegionInfo)
   */
  @Override
  public void clean(final HRegionInfo hri) {
    try {
      // Only delete if its in expected state; could have been hijacked.
      if (!ZKAssign.deleteNode(watcher, hri.getEncodedName(), RS_ZK_REQUEST_REGION_MERGE, manager
          .getServer().getServerName())) {
        ZKAssign.deleteNode(watcher, hri.getEncodedName(), RS_ZK_REGION_MERGING, manager
            .getServer().getServerName());
      }
    } catch (KeeperException.NoNodeException e) {
      LOG.info("Failed cleanup zk node of " + hri.getRegionNameAsString(), e);
    } catch (KeeperException e) {
      manager.getServer().abort("Failed cleanup zk node of " + hri.getRegionNameAsString(), e);
    }
  }

  /*
   * ZooKeeper implementation of finishRegionMergeTransaction
   */
  @Override
  public void completeRegionMergeTransaction(final RegionServerServices services,
      HRegionInfo mergedRegionInfo, HRegion region_a, HRegion region_b, RegionMergeDetails rmd,
      HRegion mergedRegion) throws IOException {
    ZkRegionMergeDetails zrmd = (ZkRegionMergeDetails) rmd;
    if (manager.getServer() == null
        || manager.getServer().getCoordinatedStateManager() == null) {
      return;
    }
    // Tell master about merge by updating zk. If we fail, abort.
    try {
      transitionMergingNode(mergedRegionInfo, region_a.getRegionInfo(), region_b.getRegionInfo(),
        manager.getServer().getServerName(), rmd, RS_ZK_REGION_MERGING, RS_ZK_REGION_MERGED);

      long startTime = EnvironmentEdgeManager.currentTime();
      int spins = 0;
      // Now wait for the master to process the merge. We know it's done
      // when the znode is deleted. The reason we keep tickling the znode is
      // that it's possible for the master to miss an event.
      do {
        if (spins % 10 == 0) {
          LOG.debug("Still waiting on the master to process the merge for "
              + mergedRegionInfo.getEncodedName() + ", waited "
              + (EnvironmentEdgeManager.currentTime() - startTime) + "ms");
        }
        Thread.sleep(100);
        // When this returns -1 it means the znode doesn't exist
        transitionMergingNode(mergedRegionInfo, region_a.getRegionInfo(), region_b.getRegionInfo(),
          manager.getServer().getServerName(), rmd, RS_ZK_REGION_MERGED, RS_ZK_REGION_MERGED);
        spins++;
      } while (zrmd.getZnodeVersion() != -1 && !manager.getServer().isStopped()
          && !services.isStopping());
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IOException("Failed telling master about merge "
          + mergedRegionInfo.getEncodedName(), e);
    }
    // Leaving here, the mergedir with its dross will be in place but since the
    // merge was successful, just leave it; it'll be cleaned when region_a is
    // cleaned up by CatalogJanitor on master
  }

  /*
   * Zookeeper implementation of region merge confirmation
   */
  @Override
  public void confirmRegionMergeTransaction(HRegionInfo merged, HRegionInfo a, HRegionInfo b,
      ServerName serverName, RegionMergeDetails rmd) throws IOException {
    transitionMergingNode(merged, a, b, serverName, rmd, RS_ZK_REGION_MERGING,
      RS_ZK_REGION_MERGING);
  }

  /*
   * Zookeeper implementation of region merge processing
   */
  @Override
  public void processRegionMergeRequest(HRegionInfo p, HRegionInfo hri_a, HRegionInfo hri_b,
      ServerName sn, RegionMergeDetails rmd) throws IOException {
    transitionMergingNode(p, hri_a, hri_b, sn, rmd, EventType.RS_ZK_REQUEST_REGION_MERGE,
      EventType.RS_ZK_REGION_MERGING);
  }

  /**
   * Transitions an existing ephemeral node for the specified region which is
   * currently in the begin state to be in the end state. Master cleans up the
   * final MERGE znode when it reads it (or if we crash, zk will clean it up).
   *
   * <p>
   * Does not transition nodes from other states. If for some reason the node
   * could not be transitioned, the method returns -1. If the transition is
   * successful, the version of the node after transition is updated in details.
   *
   * <p>
   * This method can fail and return false for three different reasons:
   * <ul>
   * <li>Node for this region does not exist</li>
   * <li>Node for this region is not in the begin state</li>
   * <li>After verifying the begin state, update fails because of wrong version
   * (this should never actually happen since an RS only does this transition
   * following a transition to the begin state. If two RS are conflicting, one would
   * fail the original transition to the begin state and not this transition)</li>
   * </ul>
   *
   * <p>
   * Does not set any watches.
   *
   * <p>
   * This method should only be used by a RegionServer when merging two regions.
   *
   * @param merged region to be transitioned to opened
   * @param a merging region A
   * @param b merging region B
   * @param serverName server event originates from
   * @param rmd region merge details
   * @param beginState the expected current state the node should be
   * @param endState the state to be transition to
   * @throws IOException
   */
  private void transitionMergingNode(HRegionInfo merged, HRegionInfo a, HRegionInfo b,
      ServerName serverName, RegionMergeDetails rmd, final EventType beginState,
      final EventType endState) throws IOException {
    ZkRegionMergeDetails zrmd = (ZkRegionMergeDetails) rmd;
    byte[] payload = HRegionInfo.toDelimitedByteArray(merged, a, b);
    try {
      zrmd.setZnodeVersion(ZKAssign.transitionNode(watcher, merged, serverName, beginState,
        endState, zrmd.getZnodeVersion(), payload));
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }
}
