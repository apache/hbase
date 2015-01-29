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
package org.apache.hadoop.hbase.zookeeper;

import com.google.common.base.Stopwatch;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.FailedServerException;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.MetaRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.KeeperException;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.rmi.UnknownHostException;

import java.util.List;
import java.util.ArrayList;

import javax.annotation.Nullable;

/**
 * Utility class to perform operation (get/wait for/verify/set/delete) on znode in ZooKeeper
 * which keeps hbase:meta region server location.
 *
 * Stateless class with a bunch of static methods. Doesn't manage resources passed in
 * (e.g. HConnection, ZooKeeperWatcher etc).
 *
 * Meta region location is set by <code>RegionServerServices</code>.
 * This class doesn't use ZK watchers, rather accesses ZK directly.
 *
 * This class it stateless. The only reason it's not made a non-instantiable util class
 * with a collection of static methods is that it'd be rather hard to mock properly in tests.
 *
 * TODO: rewrite using RPC calls to master to find out about hbase:meta.
 */
@InterfaceAudience.Private
public class MetaTableLocator {
  private static final Log LOG = LogFactory.getLog(MetaTableLocator.class);

  // only needed to allow non-timeout infinite waits to stop when cluster shuts down
  private volatile boolean stopped = false;

  /**
   * Checks if the meta region location is available.
   * @return true if meta region location is available, false if not
   */
  public boolean isLocationAvailable(ZooKeeperWatcher zkw) {
    return getMetaRegionLocation(zkw) != null;
  }

  /**
   * @param zkw ZooKeeper watcher to be used
   * @return meta table regions and their locations.
   */
  public List<Pair<HRegionInfo, ServerName>> getMetaRegionsAndLocations(ZooKeeperWatcher zkw) {
    return getMetaRegionsAndLocations(zkw, HRegionInfo.DEFAULT_REPLICA_ID);
  }

  /**
   * 
   * @param zkw
   * @param replicaId
   * @return meta table regions and their locations.
   */
  public List<Pair<HRegionInfo, ServerName>> getMetaRegionsAndLocations(ZooKeeperWatcher zkw,
      int replicaId) {
    ServerName serverName = getMetaRegionLocation(zkw, replicaId);
    List<Pair<HRegionInfo, ServerName>> list = new ArrayList<Pair<HRegionInfo, ServerName>>();
    list.add(new Pair<HRegionInfo, ServerName>(RegionReplicaUtil.getRegionInfoForReplica(
        HRegionInfo.FIRST_META_REGIONINFO, replicaId), serverName));
    return list;
  }

  /**
   * @param zkw ZooKeeper watcher to be used
   * @return List of meta regions
   */
  public List<HRegionInfo> getMetaRegions(ZooKeeperWatcher zkw) {
    return getMetaRegions(zkw, HRegionInfo.DEFAULT_REPLICA_ID);
  }

  /**
   * 
   * @param zkw
   * @param replicaId
   * @return List of meta regions
   */
  public List<HRegionInfo> getMetaRegions(ZooKeeperWatcher zkw, int replicaId) {
    List<Pair<HRegionInfo, ServerName>> result;
    result = getMetaRegionsAndLocations(zkw, replicaId);
    return getListOfHRegionInfos(result);
  }

  private List<HRegionInfo> getListOfHRegionInfos(
      final List<Pair<HRegionInfo, ServerName>> pairs) {
    if (pairs == null || pairs.isEmpty()) return null;
    List<HRegionInfo> result = new ArrayList<HRegionInfo>(pairs.size());
    for (Pair<HRegionInfo, ServerName> pair: pairs) {
      result.add(pair.getFirst());
    }
    return result;
  }

  /**
   * Gets the meta region location, if available.  Does not block.
   * @param zkw zookeeper connection to use
   * @return server name or null if we failed to get the data.
   */
  public ServerName getMetaRegionLocation(final ZooKeeperWatcher zkw) {
    try {
      RegionState state = getMetaRegionState(zkw);
      return state.isOpened() ? state.getServerName() : null;
    } catch (KeeperException ke) {
      return null;
    }
  }

  /**
   * Gets the meta region location, if available.  Does not block.
   * @param zkw
   * @param replicaId
   * @return server name
   */
  public ServerName getMetaRegionLocation(final ZooKeeperWatcher zkw, int replicaId) {
    try {
      RegionState state = getMetaRegionState(zkw, replicaId);
      return state.isOpened() ? state.getServerName() : null;
    } catch (KeeperException ke) {
      return null;
    }
  }

  /**
   * Gets the meta region location, if available, and waits for up to the
   * specified timeout if not immediately available.
   * Given the zookeeper notification could be delayed, we will try to
   * get the latest data.
   * @param zkw
   * @param timeout maximum time to wait, in millis
   * @return server name for server hosting meta region formatted as per
   * {@link ServerName}, or null if none available
   * @throws InterruptedException if interrupted while waiting
   * @throws NotAllMetaRegionsOnlineException
   */
  public ServerName waitMetaRegionLocation(ZooKeeperWatcher zkw, long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException {
    return waitMetaRegionLocation(zkw, HRegionInfo.DEFAULT_REPLICA_ID, timeout);
  }

  /**
   * Gets the meta region location, if available, and waits for up to the
   * specified timeout if not immediately available.
   * Given the zookeeper notification could be delayed, we will try to
   * get the latest data.
   * @param zkw
   * @param replicaId
   * @param timeout maximum time to wait, in millis
   * @return server name for server hosting meta region formatted as per
   * {@link ServerName}, or null if none available
   * @throws InterruptedException
   * @throws NotAllMetaRegionsOnlineException
   */
  public ServerName waitMetaRegionLocation(ZooKeeperWatcher zkw, int replicaId, long timeout)
  throws InterruptedException, NotAllMetaRegionsOnlineException {
    try {
      if (ZKUtil.checkExists(zkw, zkw.baseZNode) == -1) {
        String errorMsg = "Check the value configured in 'zookeeper.znode.parent'. "
            + "There could be a mismatch with the one configured in the master.";
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
   * Waits indefinitely for availability of <code>hbase:meta</code>.  Used during
   * cluster startup.  Does not verify meta, just that something has been
   * set up in zk.
   * @see #waitMetaRegionLocation(org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher, long)
   * @throws InterruptedException if interrupted while waiting
   */
  public void waitMetaRegionLocation(ZooKeeperWatcher zkw) throws InterruptedException {
    Stopwatch stopwatch = new Stopwatch().start();
    while (!stopped) {
      try {
        if (waitMetaRegionLocation(zkw, 100) != null) break;
        long sleepTime = stopwatch.elapsedMillis();
        // +1 in case sleepTime=0
        if ((sleepTime + 1) % 10000 == 0) {
          LOG.warn("Have been waiting for meta to be assigned for " + sleepTime + "ms");
        }
      } catch (NotAllMetaRegionsOnlineException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("hbase:meta still not available, sleeping and retrying." +
            " Reason: " + e.getMessage());
        }
      }
    }
  }

  /**
   * Verify <code>hbase:meta</code> is deployed and accessible.
   * @param hConnection
   * @param zkw
   * @param timeout How long to wait on zk for meta address (passed through to
   * the internal call to {@link #getMetaServerConnection}.
   * @return True if the <code>hbase:meta</code> location is healthy.
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  public boolean verifyMetaRegionLocation(HConnection hConnection,
      ZooKeeperWatcher zkw, final long timeout)
  throws InterruptedException, IOException {
    return verifyMetaRegionLocation(hConnection, zkw, timeout, HRegionInfo.DEFAULT_REPLICA_ID);
  }

  /**
   * Verify <code>hbase:meta</code> is deployed and accessible.
   * @param hConnection
   * @param zkw
   * @param timeout How long to wait on zk for meta address (passed through to
   * @param replicaId
   * @return True if the <code>hbase:meta</code> location is healthy.
   * @throws InterruptedException
   * @throws IOException
   */
  public boolean verifyMetaRegionLocation(HConnection hConnection,
      ZooKeeperWatcher zkw, final long timeout, int replicaId)
  throws InterruptedException, IOException {
    AdminProtos.AdminService.BlockingInterface service = null;
    try {
      service = getMetaServerConnection(hConnection, zkw, timeout, replicaId);
    } catch (NotAllMetaRegionsOnlineException e) {
      // Pass
    } catch (ServerNotRunningYetException e) {
      // Pass -- remote server is not up so can't be carrying root
    } catch (UnknownHostException e) {
      // Pass -- server name doesn't resolve so it can't be assigned anything.
    } catch (RegionServerStoppedException e) {
      // Pass -- server name sends us to a server that is dying or already dead.
    }
    return (service != null) && verifyRegionLocation(service,
            getMetaRegionLocation(zkw, replicaId), RegionReplicaUtil.getRegionInfoForReplica(
                HRegionInfo.FIRST_META_REGIONINFO, replicaId).getRegionName());
  }

  /**
   * Verify we can connect to <code>hostingServer</code> and that its carrying
   * <code>regionName</code>.
   * @param hostingServer Interface to the server hosting <code>regionName</code>
   * @param address The servername that goes with the <code>metaServer</code>
   * Interface.  Used logging.
   * @param regionName The regionname we are interested in.
   * @return True if we were able to verify the region located at other side of
   * the Interface.
   * @throws IOException
   */
  // TODO: We should be able to get the ServerName from the AdminProtocol
  // rather than have to pass it in.  Its made awkward by the fact that the
  // HRI is likely a proxy against remote server so the getServerName needs
  // to be fixed to go to a local method or to a cache before we can do this.
  private boolean verifyRegionLocation(AdminService.BlockingInterface hostingServer,
      final ServerName address, final byte [] regionName)
  throws IOException {
    if (hostingServer == null) {
      LOG.info("Passed hostingServer is null");
      return false;
    }
    Throwable t;
    try {
      // Try and get regioninfo from the hosting server.
      return ProtobufUtil.getRegionInfo(hostingServer, regionName) != null;
    } catch (ConnectException e) {
      t = e;
    } catch (RetriesExhaustedException e) {
      t = e;
    } catch (RemoteException e) {
      IOException ioe = e.unwrapRemoteException();
      t = ioe;
    } catch (IOException e) {
      Throwable cause = e.getCause();
      if (cause != null && cause instanceof EOFException) {
        t = cause;
      } else if (cause != null && cause.getMessage() != null
          && cause.getMessage().contains("Connection reset")) {
        t = cause;
      } else {
        t = e;
      }
    }
    LOG.info("Failed verification of " + Bytes.toStringBinary(regionName) +
      " at address=" + address + ", exception=" + t.getMessage());
    return false;
  }

  /**
   * Gets a connection to the server hosting meta, as reported by ZooKeeper,
   * waiting up to the specified timeout for availability.
   * <p>WARNING: Does not retry.  Use an {@link org.apache.hadoop.hbase.client.HTable} instead.
   * @param hConnection
   * @param zkw
   * @param timeout How long to wait on meta location
   * @param replicaId
   * @return connection to server hosting meta
   * @throws InterruptedException
   * @throws NotAllMetaRegionsOnlineException if timed out waiting
   * @throws IOException
   */
  private AdminService.BlockingInterface getMetaServerConnection(HConnection hConnection,
      ZooKeeperWatcher zkw, long timeout, int replicaId)
  throws InterruptedException, NotAllMetaRegionsOnlineException, IOException {
    return getCachedConnection(hConnection, waitMetaRegionLocation(zkw, replicaId, timeout));
  }

  /**
   * @param sn ServerName to get a connection against.
   * @return The AdminProtocol we got when we connected to <code>sn</code>
   * May have come from cache, may not be good, may have been setup by this
   * invocation, or may be null.
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  private static AdminService.BlockingInterface getCachedConnection(HConnection hConnection,
    ServerName sn)
  throws IOException {
    if (sn == null) {
      return null;
    }
    AdminService.BlockingInterface service = null;
    try {
      service = hConnection.getAdmin(sn);
    } catch (RetriesExhaustedException e) {
      if (e.getCause() != null && e.getCause() instanceof ConnectException) {
        // Catch this; presume it means the cached connection has gone bad.
      } else {
        throw e;
      }
    } catch (SocketTimeoutException e) {
      LOG.debug("Timed out connecting to " + sn);
    } catch (NoRouteToHostException e) {
      LOG.debug("Connecting to " + sn, e);
    } catch (SocketException e) {
      LOG.debug("Exception connecting to " + sn);
    } catch (UnknownHostException e) {
      LOG.debug("Unknown host exception connecting to  " + sn);
    } catch (FailedServerException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Server " + sn + " is in failed server list.");
      }
    } catch (IOException ioe) {
      Throwable cause = ioe.getCause();
      if (ioe instanceof ConnectException) {
        // Catch. Connect refused.
      } else if (cause != null && cause instanceof EOFException) {
        // Catch. Other end disconnected us.
      } else if (cause != null && cause.getMessage() != null &&
        cause.getMessage().toLowerCase().contains("connection reset")) {
        // Catch. Connection reset.
      } else {
        throw ioe;
      }

    }
    return service;
  }

  /**
   * Sets the location of <code>hbase:meta</code> in ZooKeeper to the
   * specified server address.
   * @param zookeeper zookeeper reference
   * @param serverName The server hosting <code>hbase:meta</code>
   * @param state The region transition state
   * @throws KeeperException unexpected zookeeper exception
   */
  public static void setMetaLocation(ZooKeeperWatcher zookeeper,
      ServerName serverName, RegionState.State state) throws KeeperException {
    setMetaLocation(zookeeper, serverName, HRegionInfo.DEFAULT_REPLICA_ID, state);
  }

  /**
   * Sets the location of <code>hbase:meta</code> in ZooKeeper to the
   * specified server address.
   * @param zookeeper
   * @param serverName
   * @param replicaId
   * @param state
   * @throws KeeperException
   */
  public static void setMetaLocation(ZooKeeperWatcher zookeeper,
      ServerName serverName, int replicaId, RegionState.State state) throws KeeperException {
    LOG.info("Setting hbase:meta region location in ZooKeeper as " + serverName);
    // Make the MetaRegionServer pb and then get its bytes and save this as
    // the znode content.
    MetaRegionServer pbrsr = MetaRegionServer.newBuilder()
      .setServer(ProtobufUtil.toServerName(serverName))
      .setRpcVersion(HConstants.RPC_CURRENT_VERSION)
      .setState(state.convert()).build();
    byte[] data = ProtobufUtil.prependPBMagic(pbrsr.toByteArray());
    try {
      ZKUtil.setData(zookeeper, zookeeper.getZNodeForReplica(replicaId), data);
    } catch(KeeperException.NoNodeException nne) {
      if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
        LOG.debug("META region location doesn't exist, create it");
      } else {
        LOG.debug("META region location doesn't exist for replicaId " + replicaId +
            ", create it");
      }
      ZKUtil.createAndWatch(zookeeper, zookeeper.getZNodeForReplica(replicaId), data);
    }
  }

  /**
   * Load the meta region state from the meta server ZNode.
   */
  public static RegionState getMetaRegionState(ZooKeeperWatcher zkw) throws KeeperException {
    return getMetaRegionState(zkw, HRegionInfo.DEFAULT_REPLICA_ID);
  }

  /**
   * Load the meta region state from the meta server ZNode.
   * @param zkw
   * @param replicaId
   * @return regionstate
   * @throws KeeperException
   */
  public static RegionState getMetaRegionState(ZooKeeperWatcher zkw, int replicaId)
      throws KeeperException {
    RegionState.State state = RegionState.State.OPEN;
    ServerName serverName = null;
    try {
      byte[] data = ZKUtil.getData(zkw, zkw.getZNodeForReplica(replicaId));
      if (data != null && data.length > 0 && ProtobufUtil.isPBMagicPrefix(data)) {
        try {
          int prefixLen = ProtobufUtil.lengthOfPBMagic();
          ZooKeeperProtos.MetaRegionServer rl =
            ZooKeeperProtos.MetaRegionServer.PARSER.parseFrom
              (data, prefixLen, data.length - prefixLen);
          if (rl.hasState()) {
            state = RegionState.State.convert(rl.getState());
          }
          HBaseProtos.ServerName sn = rl.getServer();
          serverName = ServerName.valueOf(
            sn.getHostName(), sn.getPort(), sn.getStartCode());
        } catch (InvalidProtocolBufferException e) {
          throw new DeserializationException("Unable to parse meta region location");
        }
      } else {
        // old style of meta region location?
        serverName = ServerName.parseFrom(data);
      }
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (serverName == null) {
      state = RegionState.State.OFFLINE;
    }
    return new RegionState(
        RegionReplicaUtil.getRegionInfoForReplica(HRegionInfo.FIRST_META_REGIONINFO, replicaId),
      state, serverName);
  }

  /**
   * Deletes the location of <code>hbase:meta</code> in ZooKeeper.
   * @param zookeeper zookeeper reference
   * @throws KeeperException unexpected zookeeper exception
   */
  public void deleteMetaLocation(ZooKeeperWatcher zookeeper)
  throws KeeperException {
    deleteMetaLocation(zookeeper, HRegionInfo.DEFAULT_REPLICA_ID);
  }

  public void deleteMetaLocation(ZooKeeperWatcher zookeeper, int replicaId)
  throws KeeperException {
    if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
      LOG.info("Deleting hbase:meta region location in ZooKeeper");
    } else {
      LOG.info("Deleting hbase:meta for " + replicaId + " region location in ZooKeeper");
    }
    try {
      // Just delete the node.  Don't need any watches.
      ZKUtil.deleteNode(zookeeper, zookeeper.getZNodeForReplica(replicaId));
    } catch(KeeperException.NoNodeException nne) {
      // Has already been deleted
    }
  }
  /**
   * Wait until the primary meta region is available. Get the secondary
   * locations as well but don't block for those.
   * @param zkw
   * @param timeout
   * @param conf
   * @return ServerName or null if we timed out.
   * @throws InterruptedException
   */
  public List<ServerName> blockUntilAvailable(final ZooKeeperWatcher zkw,
      final long timeout, Configuration conf)
          throws InterruptedException {
    int numReplicasConfigured = 1;
    try {
      List<String> metaReplicaNodes = zkw.getMetaReplicaNodes();
      numReplicasConfigured = metaReplicaNodes.size();
    } catch (KeeperException e) {
      LOG.warn("Got ZK exception " + e);
    }
    List<ServerName> servers = new ArrayList<ServerName>(numReplicasConfigured);
    ServerName server = blockUntilAvailable(zkw, timeout);
    if (server == null) return null;
    servers.add(server);

    for (int replicaId = 1; replicaId < numReplicasConfigured; replicaId++) {
      // return all replica locations for the meta
      servers.add(getMetaRegionLocation(zkw, replicaId));
    }
    return servers;
  }

  /**
   * Wait until the meta region is available and is not in transition.
   * @param zkw zookeeper connection to use
   * @param timeout maximum time to wait, in millis
   * @return ServerName or null if we timed out.
   * @throws InterruptedException
   */
  public ServerName blockUntilAvailable(final ZooKeeperWatcher zkw,
      final long timeout)
  throws InterruptedException {
    return blockUntilAvailable(zkw, HRegionInfo.DEFAULT_REPLICA_ID, timeout);
  }

  /**
   * Wait until the meta region is available and is not in transition.
   * @param zkw
   * @param replicaId
   * @param timeout
   * @return ServerName or null if we timed out.
   * @throws InterruptedException
   */
  public ServerName blockUntilAvailable(final ZooKeeperWatcher zkw, int replicaId,
      final long timeout)
  throws InterruptedException {
    if (timeout < 0) throw new IllegalArgumentException();
    if (zkw == null) throw new IllegalArgumentException();
    Stopwatch sw = new Stopwatch().start();
    ServerName sn = null;
    try {
      while (true) {
        sn = getMetaRegionLocation(zkw, replicaId);
        if (sn != null || sw.elapsedMillis()
            > timeout - HConstants.SOCKET_RETRY_WAIT_MS) {
          break;
        }
        Thread.sleep(HConstants.SOCKET_RETRY_WAIT_MS);
      }
    } finally {
      sw.stop();
    }
    return sn;
  }

  /**
   * Stop working.
   * Interrupts any ongoing waits.
   */
  public void stop() {
    if (!stopped) {
      LOG.debug("Stopping MetaTableLocator");
      stopped = true;
    }
  }
}
