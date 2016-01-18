/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.coordination;

import static org.apache.hadoop.hbase.executor.EventType.RS_ZK_REGION_SPLIT;
import static org.apache.hadoop.hbase.executor.EventType.RS_ZK_REGION_SPLITTING;
import static org.apache.hadoop.hbase.executor.EventType.RS_ZK_REQUEST_REGION_SPLIT;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coordination.SplitTransactionCoordination;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public class ZKSplitTransactionCoordination implements SplitTransactionCoordination {

  private CoordinatedStateManager coordinationManager;
  private final ZooKeeperWatcher watcher;

  private static final Log LOG = LogFactory.getLog(ZKSplitTransactionCoordination.class);

  public ZKSplitTransactionCoordination(CoordinatedStateManager coordinationProvider,
      ZooKeeperWatcher watcher) {
    this.coordinationManager = coordinationProvider;
    this.watcher = watcher;
  }

  /**
   * Creates a new ephemeral node in the PENDING_SPLIT state for the specified region. Create it
   * ephemeral in case regionserver dies mid-split.
   * <p>
   * Does not transition nodes from other states. If a node already exists for this region, an
   * Exception will be thrown.
   * @param parent region to be created as offline
   * @param serverName server event originates from
   * @param hri_a daughter region
   * @param hri_b daughter region
   * @throws IOException
   */

  @Override
  public void startSplitTransaction(HRegion parent, ServerName serverName, HRegionInfo hri_a,
      HRegionInfo hri_b) throws IOException {

    HRegionInfo region = parent.getRegionInfo();
    try {

      LOG.debug(watcher.prefix("Creating ephemeral node for " + region.getEncodedName()
          + " in PENDING_SPLIT state"));
      byte[] payload = HRegionInfo.toDelimitedByteArray(hri_a, hri_b);
      RegionTransition rt =
          RegionTransition.createRegionTransition(RS_ZK_REQUEST_REGION_SPLIT,
            region.getRegionName(), serverName, payload);
      String node = ZKAssign.getNodeName(watcher, region.getEncodedName());
      if (!ZKUtil.createEphemeralNodeAndWatch(watcher, node, rt.toByteArray())) {
        throw new IOException("Failed create of ephemeral " + node);
      }

    } catch (KeeperException e) {
      throw new IOException("Failed creating PENDING_SPLIT znode on "
          + parent.getRegionInfo().getRegionNameAsString(), e);
    }

  }

  /**
   * Transitions an existing ephemeral node for the specified region which is currently in the begin
   * state to be in the end state. Master cleans up the final SPLIT znode when it reads it (or if we
   * crash, zk will clean it up).
   * <p>
   * Does not transition nodes from other states. If for some reason the node could not be
   * transitioned, the method returns -1. If the transition is successful, the version of the node
   * after transition is returned.
   * <p>
   * This method can fail and return false for three different reasons:
   * <ul>
   * <li>Node for this region does not exist</li>
   * <li>Node for this region is not in the begin state</li>
   * <li>After verifying the begin state, update fails because of wrong version (this should never
   * actually happen since an RS only does this transition following a transition to the begin
   * state. If two RS are conflicting, one would fail the original transition to the begin state and
   * not this transition)</li>
   * </ul>
   * <p>
   * Does not set any watches.
   * <p>
   * This method should only be used by a RegionServer when splitting a region.
   * @param parent region to be transitioned to opened
   * @param a Daughter a of split
   * @param b Daughter b of split
   * @param serverName server event originates from
   * @param std split transaction details
   * @param beginState the expected current state the znode should be
   * @param endState the state to be transition to
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws IOException
   */

  private int transitionSplittingNode(HRegionInfo parent, HRegionInfo a, HRegionInfo b,
      ServerName serverName, SplitTransactionDetails std, final EventType beginState,
      final EventType endState) throws IOException {
    ZkSplitTransactionDetails zstd = (ZkSplitTransactionDetails) std;
    byte[] payload = HRegionInfo.toDelimitedByteArray(a, b);
    try {
      return ZKAssign.transitionNode(watcher, parent, serverName, beginState, endState,
        zstd.getZnodeVersion(), payload);
    } catch (KeeperException e) {
      throw new IOException(
          "Failed transition of splitting node " + parent.getRegionNameAsString(), e);
    }
  }

  /**
   * Wait for the splitting node to be transitioned from pending_split to splitting by master.
   * That's how we are sure master has processed the event and is good with us to move on. If we
   * don't get any update, we periodically transition the node so that master gets the callback. If
   * the node is removed or is not in pending_split state any more, we abort the split.
   */
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION",
    justification="Intended")
  public void waitForSplitTransaction(final RegionServerServices services, Region parent,
      HRegionInfo hri_a, HRegionInfo hri_b, SplitTransactionDetails sptd) throws IOException {
    ZkSplitTransactionDetails zstd = (ZkSplitTransactionDetails) sptd;

    // After creating the split node, wait for master to transition it
    // from PENDING_SPLIT to SPLITTING so that we can move on. We want master
    // knows about it and won't transition any region which is splitting.
    try {
      int spins = 0;
      Stat stat = new Stat();
      ServerName expectedServer = coordinationManager.getServer().getServerName();
      String node = parent.getRegionInfo().getEncodedName();
      while (!(coordinationManager.getServer().isStopped() || services.isStopping())) {
        if (spins % 5 == 0) {
          LOG.debug("Still waiting for master to process " + "the pending_split for " + node);
          SplitTransactionDetails temp = getDefaultDetails();
          transitionSplittingNode(parent.getRegionInfo(), hri_a, hri_b, expectedServer, temp,
            RS_ZK_REQUEST_REGION_SPLIT, RS_ZK_REQUEST_REGION_SPLIT);
        }
        Thread.sleep(100);
        spins++;
        byte[] data = ZKAssign.getDataNoWatch(watcher, node, stat);
        if (data == null) {
          throw new IOException("Data is null, splitting node " + node + " no longer exists");
        }
        RegionTransition rt = RegionTransition.parseFrom(data);
        EventType et = rt.getEventType();
        if (et == RS_ZK_REGION_SPLITTING) {
          ServerName serverName = rt.getServerName();
          if (!serverName.equals(expectedServer)) {
            throw new IOException("Splitting node " + node + " is for " + serverName + ", not us "
                + expectedServer);
          }
          byte[] payloadOfSplitting = rt.getPayload();
          List<HRegionInfo> splittingRegions =
              HRegionInfo.parseDelimitedFrom(payloadOfSplitting, 0, payloadOfSplitting.length);
          assert splittingRegions.size() == 2;
          HRegionInfo a = splittingRegions.get(0);
          HRegionInfo b = splittingRegions.get(1);
          if (!(hri_a.equals(a) && hri_b.equals(b))) {
            throw new IOException("Splitting node " + node + " is for " + a + ", " + b
                + ", not expected daughters: " + hri_a + ", " + hri_b);
          }
          // Master has processed it.
          zstd.setZnodeVersion(stat.getVersion());
          return;
        }
        if (et != RS_ZK_REQUEST_REGION_SPLIT) {
          throw new IOException("Splitting node " + node + " moved out of splitting to " + et);
        }
      }
      // Server is stopping/stopped
      throw new IOException("Server is " + (services.isStopping() ? "stopping" : "stopped"));
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IOException("Failed getting SPLITTING znode on " +
        parent.getRegionInfo().getRegionNameAsString(), e);
    }
  }

  /**
   * Finish off split transaction, transition the zknode
   * @param services Used to online/offline regions.
   * @param a daughter region
   * @param b daughter region
   * @param std split transaction details
   * @param parent
   * @throws IOException If thrown, transaction failed. Call
   *  {@link org.apache.hadoop.hbase.regionserver.SplitTransaction#rollback(
   *  Server, RegionServerServices)}
   */
  @Override
  public void completeSplitTransaction(final RegionServerServices services, Region a, Region b,
      SplitTransactionDetails std, Region parent) throws IOException {
    ZkSplitTransactionDetails zstd = (ZkSplitTransactionDetails) std;
    // Tell master about split by updating zk. If we fail, abort.
    if (coordinationManager.getServer() != null) {
      try {
        zstd.setZnodeVersion(transitionSplittingNode(parent.getRegionInfo(), a.getRegionInfo(),
          b.getRegionInfo(), coordinationManager.getServer().getServerName(), zstd,
          RS_ZK_REGION_SPLITTING, RS_ZK_REGION_SPLIT));

        int spins = 0;
        // Now wait for the master to process the split. We know it's done
        // when the znode is deleted. The reason we keep tickling the znode is
        // that it's possible for the master to miss an event.
        do {
          if (spins % 10 == 0) {
            LOG.debug("Still waiting on the master to process the split for "
                + parent.getRegionInfo().getEncodedName());
          }
          Thread.sleep(100);
          // When this returns -1 it means the znode doesn't exist
          zstd.setZnodeVersion(transitionSplittingNode(parent.getRegionInfo(), a.getRegionInfo(),
            b.getRegionInfo(), coordinationManager.getServer().getServerName(), zstd,
            RS_ZK_REGION_SPLIT, RS_ZK_REGION_SPLIT));
          spins++;
        } while (zstd.getZnodeVersion() != -1 && !coordinationManager.getServer().isStopped()
            && !services.isStopping());
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new IOException("Failed telling master about split", e);
      }
    }

    // Leaving here, the splitdir with its dross will be in place but since the
    // split was successful, just leave it; it'll be cleaned when parent is
    // deleted and cleaned up.
  }

  @Override
  public void clean(final HRegionInfo hri) {
    try {
      // Only delete if its in expected state; could have been hijacked.
      if (!ZKAssign.deleteNode(coordinationManager.getServer().getZooKeeper(),
        hri.getEncodedName(), RS_ZK_REQUEST_REGION_SPLIT, coordinationManager.getServer()
            .getServerName())) {
        ZKAssign.deleteNode(coordinationManager.getServer().getZooKeeper(), hri.getEncodedName(),
          RS_ZK_REGION_SPLITTING, coordinationManager.getServer().getServerName());
      }
    } catch (KeeperException.NoNodeException e) {
      LOG.info("Failed cleanup zk node of " + hri.getRegionNameAsString(), e);
    } catch (KeeperException e) {
      coordinationManager.getServer().abort("Failed cleanup of " + hri.getRegionNameAsString(), e);
    }
  }

  /**
   * ZK-based implementation. Has details about whether the state transition should be reflected in
   * ZK, as well as expected version of znode.
   */
  public static class ZkSplitTransactionDetails implements
      SplitTransactionCoordination.SplitTransactionDetails {
    private int znodeVersion;

    public ZkSplitTransactionDetails() {
    }

    /**
     * @return znode current version
     */
    public int getZnodeVersion() {
      return znodeVersion;
    }

    /**
     * @param znodeVersion znode new version
     */
    public void setZnodeVersion(int znodeVersion) {
      this.znodeVersion = znodeVersion;
    }
  }

  @Override
  public SplitTransactionDetails getDefaultDetails() {
    ZkSplitTransactionDetails zstd = new ZkSplitTransactionDetails();
    zstd.setZnodeVersion(-1);
    return zstd;
  }

  @Override
  public int processTransition(HRegionInfo p, HRegionInfo hri_a, HRegionInfo hri_b, ServerName sn,
      SplitTransactionDetails std) throws IOException {
    return transitionSplittingNode(p, hri_a, hri_b, sn, std, RS_ZK_REQUEST_REGION_SPLIT,
      RS_ZK_REGION_SPLITTING);

  }
}
