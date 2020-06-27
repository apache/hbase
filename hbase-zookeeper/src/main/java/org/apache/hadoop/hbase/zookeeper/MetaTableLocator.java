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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.RegionState;
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
 * @deprecated Since 3.0.0, will be removed in 4.0.0. Now we store the meta location in the local
 *             store of master, the location on zk is only a mirror of the first meta region to keep
 *             compatibility.
 */
@Deprecated
@InterfaceAudience.Private
public final class MetaTableLocator {
  private static final Logger LOG = LoggerFactory.getLogger(MetaTableLocator.class);

  private MetaTableLocator() {
  }

  /**
   * Sets the location of <code>hbase:meta</code> in ZooKeeper to the specified server address.
   * @param zookeeper reference to the {@link ZKWatcher} which also contains configuration and
   *          operation
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
    LOG.info("Setting hbase:meta (replicaId={}) location in ZooKeeper as {}, state={}", replicaId,
      serverName, state);
    // Make the MetaRegionServer pb and then get its bytes and save this as
    // the znode content.
    MetaRegionServer pbrsr =
      MetaRegionServer.newBuilder().setServer(ProtobufUtil.toServerName(serverName))
        .setRpcVersion(HConstants.RPC_CURRENT_VERSION).setState(state.convert()).build();
    byte[] data = ProtobufUtil.prependPBMagic(pbrsr.toByteArray());
    try {
      ZKUtil.setData(zookeeper, zookeeper.getZNodePaths().getZNodeForReplica(replicaId), data);
    } catch (KeeperException.NoNodeException nne) {
      if (replicaId == RegionInfo.DEFAULT_REPLICA_ID) {
        LOG.debug("META region location doesn't exist, create it");
      } else {
        LOG.debug("META region location doesn't exist for replicaId=" + replicaId + ", create it");
      }
      ZKUtil.createAndWatch(zookeeper, zookeeper.getZNodePaths().getZNodeForReplica(replicaId),
        data);
    }
  }

  /**
   * Load the meta region state from the meta region server ZNode.
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
}
