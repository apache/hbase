/*
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

import com.google.errorprone.annotations.RestrictedApi;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos.MetaRegionServer;

/**
 * Utility class to perform operation (get/wait for/verify/set/delete) on znode in ZooKeeper which
 * keeps hbase:meta region server location.
 * <p/>
 * Stateless class with a bunch of static methods. Doesn't manage resources passed in (e.g.
 * Connection, ZKWatcher etc).
 * <p/>
 * Meta region location is set by <code>RegionServerServices</code>. This class doesn't use ZK
 * watchers, rather accesses ZK directly.
 * <p/>
 * TODO: rewrite using RPC calls to master to find out about hbase:meta.
 */
@InterfaceAudience.Private
public final class MetaTableLocator {
  private static final Logger LOG = LoggerFactory.getLogger(MetaTableLocator.class);

  private MetaTableLocator() {
  }

  /**
   * Gets the meta region location, if available.  Does not block.
   * @param zkw zookeeper connection to use
   * @return server name or null if we failed to get the data.
   */
  @RestrictedApi(explanation = "Should only be called in tests or ZKUtil", link = "",
    allowedOnPath = ".*/src/test/.*|.*/ZKDump\\.java")
  public static ServerName getMetaRegionLocation(final ZKWatcher zkw) {
    try {
      RegionState state = getMetaRegionState(zkw);
      return state.isOpened() ? state.getServerName() : null;
    } catch (KeeperException ke) {
      return null;
    }
  }

  /**
   * Gets the meta region location, if available.  Does not block.
   * @param zkw reference to the {@link ZKWatcher} which also contains configuration and operation
   * @param replicaId the ID of the replica
   * @return server name
   */
  @RestrictedApi(explanation = "Should only be called in self or ZKUtil", link = "",
    allowedOnPath = ".*(MetaTableLocator|ZKDump)\\.java")
  public static ServerName getMetaRegionLocation(final ZKWatcher zkw, int replicaId) {
    try {
      RegionState state = getMetaRegionState(zkw, replicaId);
      return state.isOpened() ? state.getServerName() : null;
    } catch (KeeperException ke) {
      return null;
    }
  }

  /**
   * Gets the meta region location, if available, and waits for up to the specified timeout if not
   * immediately available. Given the zookeeper notification could be delayed, we will try to get
   * the latest data.
   * @param zkw reference to the {@link ZKWatcher} which also contains configuration and operation
   * @param timeout maximum time to wait, in millis
   * @return server name for server hosting meta region formatted as per {@link ServerName}, or null
   *         if none available
   * @throws InterruptedException if interrupted while waiting
   * @throws NotAllMetaRegionsOnlineException if a meta or root region is not online
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*/src/test/.*")
  public static ServerName waitMetaRegionLocation(ZKWatcher zkw, long timeout)
      throws InterruptedException, NotAllMetaRegionsOnlineException {
    return waitMetaRegionLocation(zkw, RegionInfo.DEFAULT_REPLICA_ID, timeout);
  }

  /**
   * Gets the meta region location, if available, and waits for up to the specified timeout if not
   * immediately available. Given the zookeeper notification could be delayed, we will try to get
   * the latest data.
   * @param zkw reference to the {@link ZKWatcher} which also contains configuration and operation
   * @param replicaId the ID of the replica
   * @param timeout maximum time to wait, in millis
   * @return server name for server hosting meta region formatted as per {@link ServerName}, or null
   *         if none available
   * @throws InterruptedException if waiting for the socket operation fails
   * @throws NotAllMetaRegionsOnlineException if a meta or root region is not online
   */
  private static ServerName waitMetaRegionLocation(ZKWatcher zkw, int replicaId, long timeout)
      throws InterruptedException, NotAllMetaRegionsOnlineException {
    try {
      if (ZKUtil.checkExists(zkw, zkw.getZNodePaths().baseZNode) == -1) {
        String errorMsg = "Check the value configured in 'zookeeper.znode.parent'. " +
          "There could be a mismatch with the one configured in the master.";
        LOG.error(errorMsg);
        throw new IllegalArgumentException(errorMsg);
      }
    } catch (KeeperException e) {
      throw new IllegalStateException("KeeperException while trying to check baseZNode:", e);
    }
    ServerName sn = blockUntilAvailable(zkw, replicaId, timeout);

    if (sn == null) {
      throw new NotAllMetaRegionsOnlineException("Timed out; " + timeout + "ms");
    }

    return sn;
  }

  /**
   * Sets the location of <code>hbase:meta</code> in ZooKeeper to the
   * specified server address.
   * @param zookeeper zookeeper reference
   * @param serverName The server hosting <code>hbase:meta</code>
   * @param state The region transition state
   * @throws KeeperException unexpected zookeeper exception
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*/src/test/.*")
  public static void setMetaLocation(ZKWatcher zookeeper,
      ServerName serverName, RegionState.State state) throws KeeperException {
    setMetaLocation(zookeeper, serverName, RegionInfo.DEFAULT_REPLICA_ID, state);
  }

  /**
   * Sets the location of <code>hbase:meta</code> in ZooKeeper to the specified server address.
   * @param zookeeper reference to the {@link ZKWatcher} which also contains configuration and
   *                  operation
   * @param serverName the name of the server
   * @param replicaId the ID of the replica
   * @param state the state of the region
   * @throws KeeperException if a ZooKeeper operation fails
   */
  public static void setMetaLocation(ZKWatcher zookeeper, ServerName serverName, int replicaId,
      RegionState.State state) throws KeeperException {
    if (serverName == null) {
      LOG.warn("Tried to set null ServerName in hbase:meta; skipping -- ServerName required");
      return;
    }
    LOG.info("Setting hbase:meta replicaId={} location in ZooKeeper as {}, state={}", replicaId,
      serverName, state);
    // Make the MetaRegionServer pb and then get its bytes and save this as
    // the znode content.
    MetaRegionServer pbrsr = MetaRegionServer.newBuilder()
      .setServer(ProtobufUtil.toServerName(serverName))
      .setRpcVersion(HConstants.RPC_CURRENT_VERSION)
      .setState(state.convert()).build();
    byte[] data = ProtobufUtil.prependPBMagic(pbrsr.toByteArray());
    try {
      ZKUtil.setData(zookeeper,
          zookeeper.getZNodePaths().getZNodeForReplica(replicaId), data);
    } catch(KeeperException.NoNodeException nne) {
      if (replicaId == RegionInfo.DEFAULT_REPLICA_ID) {
        LOG.debug("hbase:meta region location doesn't exist, create it");
      } else {
        LOG.debug("hbase:meta region location doesn't exist for replicaId=" + replicaId +
            ", create it");
      }
      ZKUtil.createAndWatch(zookeeper, zookeeper.getZNodePaths().getZNodeForReplica(replicaId),
              data);
    }
  }

  /**
   * Load the meta region state from the meta server ZNode.
   */
  @RestrictedApi(explanation = "Should only be called in self or tests", link = "",
    allowedOnPath = ".*/src/test/.*|.*/MetaTableLocator\\.java")
  public static RegionState getMetaRegionState(ZKWatcher zkw) throws KeeperException {
    return getMetaRegionState(zkw, RegionInfo.DEFAULT_REPLICA_ID);
  }

  /**
   * Load the meta region state from the meta region server ZNode.
   *
   * @param zkw reference to the {@link ZKWatcher} which also contains configuration and operation
   * @param replicaId the ID of the replica
   * @return regionstate
   * @throws KeeperException if a ZooKeeper operation fails
   */
  public static RegionState getMetaRegionState(ZKWatcher zkw, int replicaId)
      throws KeeperException {
    RegionState regionState = null;
    try {
      byte[] data = ZKUtil.getData(zkw, zkw.getZNodePaths().getZNodeForReplica(replicaId));
      regionState = ProtobufUtil.parseMetaRegionStateFrom(data, replicaId);
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return regionState;
  }

  /**
   * Deletes the location of <code>hbase:meta</code> in ZooKeeper.
   * @param zookeeper zookeeper reference
   * @throws KeeperException unexpected zookeeper exception
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*/src/test/.*")
  public static void deleteMetaLocation(ZKWatcher zookeeper)
    throws KeeperException {
    deleteMetaLocation(zookeeper, RegionInfo.DEFAULT_REPLICA_ID);
  }

  public static void deleteMetaLocation(ZKWatcher zookeeper, int replicaId)
    throws KeeperException {
    if (replicaId == RegionInfo.DEFAULT_REPLICA_ID) {
      LOG.info("Deleting hbase:meta region location in ZooKeeper");
    } else {
      LOG.info("Deleting hbase:meta for {} region location in ZooKeeper", replicaId);
    }
    try {
      // Just delete the node.  Don't need any watches.
      ZKUtil.deleteNode(zookeeper, zookeeper.getZNodePaths().getZNodeForReplica(replicaId));
    } catch(KeeperException.NoNodeException nne) {
      // Has already been deleted
    }
  }

  /**
   * Wait until the meta region is available and is not in transition.
   * @param zkw reference to the {@link ZKWatcher} which also contains configuration and constants
   * @param replicaId the ID of the replica
   * @param timeout maximum time to wait in millis
   * @return ServerName or null if we timed out.
   * @throws InterruptedException if waiting for the socket operation fails
   */
  private static ServerName blockUntilAvailable(final ZKWatcher zkw, int replicaId,
    final long timeout) throws InterruptedException {
    if (timeout < 0) {
      throw new IllegalArgumentException();
    }

    if (zkw == null) {
      throw new IllegalArgumentException();
    }

    long startTime = EnvironmentEdgeManager.currentTime();
    ServerName sn = null;
    while (true) {
      sn = getMetaRegionLocation(zkw, replicaId);
      if (sn != null || (EnvironmentEdgeManager.currentTime() - startTime) > timeout -
        HConstants.SOCKET_RETRY_WAIT_MS) {
        break;
      }
      Thread.sleep(HConstants.SOCKET_RETRY_WAIT_MS);
    }
    return sn;
  }
}
