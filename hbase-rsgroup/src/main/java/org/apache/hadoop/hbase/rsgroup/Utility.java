/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.rsgroup;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.ipc.FailedServerException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;

/**
 * Utility for this RSGroup package in hbase-rsgroup.
 */
@InterfaceAudience.Private
final class Utility {

  private static final Logger LOG = LoggerFactory.getLogger(Utility.class);

  private Utility() {
  }

  /**
   * @param master the master to get online servers for
   * @return Set of online Servers named for their hostname and port (not ServerName).
   */
  static Set<Address> getOnlineServers(final MasterServices master) {
    Set<Address> onlineServers = new HashSet<Address>();
    if (master == null) {
      return onlineServers;
    }

    for (ServerName server : master.getServerManager().getOnlineServers().keySet()) {
      onlineServers.add(server.getAddress());
    }
    return onlineServers;
  }

  /**
   * Verify <code>hbase:meta</code> is deployed and accessible.
   * @param hConnection the connection to use
   * @param zkw reference to the {@link ZKWatcher} which also contains configuration and operation
   * @param timeout How long to wait on zk for meta address (passed through to the internal call to
   *          {@link #getMetaServerConnection}.
   * @return True if the <code>hbase:meta</code> location is healthy.
   * @throws IOException if the number of retries for getting the connection is exceeded
   * @throws InterruptedException if waiting for the socket operation fails
   */
  public static boolean verifyMetaRegionLocation(ClusterConnection hConnection, ZKWatcher zkw,
      final long timeout) throws InterruptedException, IOException {
    return verifyMetaRegionLocation(hConnection, zkw, timeout, RegionInfo.DEFAULT_REPLICA_ID);
  }

  /**
   * Verify <code>hbase:meta</code> is deployed and accessible.
   * @param connection the connection to use
   * @param zkw reference to the {@link ZKWatcher} which also contains configuration and operation
   * @param timeout How long to wait on zk for meta address (passed through to
   * @param replicaId the ID of the replica
   * @return True if the <code>hbase:meta</code> location is healthy.
   * @throws InterruptedException if waiting for the socket operation fails
   * @throws IOException if the number of retries for getting the connection is exceeded
   */
  public static boolean verifyMetaRegionLocation(ClusterConnection connection, ZKWatcher zkw,
      final long timeout, int replicaId) throws InterruptedException, IOException {
    AdminProtos.AdminService.BlockingInterface service = null;
    try {
      service = getMetaServerConnection(connection, zkw, timeout, replicaId);
    } catch (NotAllMetaRegionsOnlineException e) {
      // Pass
    } catch (ServerNotRunningYetException e) {
      // Pass -- remote server is not up so can't be carrying root
    } catch (UnknownHostException e) {
      // Pass -- server name doesn't resolve so it can't be assigned anything.
    } catch (RegionServerStoppedException e) {
      // Pass -- server name sends us to a server that is dying or already dead.
    }
    return (service != null) && verifyRegionLocation(connection, service,
      MetaTableLocator.getMetaRegionLocation(zkw, replicaId),
      RegionReplicaUtil.getRegionInfoForReplica(RegionInfoBuilder.FIRST_META_REGIONINFO, replicaId)
        .getRegionName());
  }

  /**
   * Verify we can connect to <code>hostingServer</code> and that its carrying
   * <code>regionName</code>.
   * @param hostingServer Interface to the server hosting <code>regionName</code>
   * @param address The servername that goes with the <code>metaServer</code> interface. Used
   *          logging.
   * @param regionName The regionname we are interested in.
   * @return True if we were able to verify the region located at other side of the interface.
   */
  // TODO: We should be able to get the ServerName from the AdminProtocol
  // rather than have to pass it in. Its made awkward by the fact that the
  // HRI is likely a proxy against remote server so the getServerName needs
  // to be fixed to go to a local method or to a cache before we can do this.
  private static boolean verifyRegionLocation(final ClusterConnection connection,
      AdminService.BlockingInterface hostingServer, final ServerName address,
      final byte[] regionName) {
    if (hostingServer == null) {
      LOG.info("Passed hostingServer is null");
      return false;
    }
    Throwable t;
    HBaseRpcController controller = connection.getRpcControllerFactory().newController();
    try {
      // Try and get regioninfo from the hosting server.
      return ProtobufUtil.getRegionInfo(controller, hostingServer, regionName) != null;
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
      } else if (cause != null && cause.getMessage() != null &&
        cause.getMessage().contains("Connection reset")) {
        t = cause;
      } else {
        t = e;
      }
    }
    LOG.info("Failed verification of " + Bytes.toStringBinary(regionName) + " at address=" +
      address + ", exception=" + t.getMessage());
    return false;
  }

  /**
   * Gets a connection to the server hosting meta, as reported by ZooKeeper, waiting up to the
   * specified timeout for availability.
   * <p>
   * WARNING: Does not retry. Use an {@link org.apache.hadoop.hbase.client.HTable} instead.
   * @param connection the connection to use
   * @param zkw reference to the {@link ZKWatcher} which also contains configuration and operation
   * @param timeout How long to wait on meta location
   * @param replicaId the ID of the replica
   * @return connection to server hosting meta
   * @throws InterruptedException if waiting for the socket operation fails
   * @throws IOException if the number of retries for getting the connection is exceeded
   */
  private static AdminService.BlockingInterface getMetaServerConnection(
      ClusterConnection connection, ZKWatcher zkw, long timeout, int replicaId)
      throws InterruptedException, IOException {
    return getCachedConnection(connection,
      MetaTableLocator.waitMetaRegionLocation(zkw, replicaId, timeout));
  }

  /**
   * @param sn ServerName to get a connection against.
   * @return The AdminProtocol we got when we connected to <code>sn</code> May have come from cache,
   *         may not be good, may have been setup by this invocation, or may be null.
   * @throws IOException if the number of retries for getting the connection is exceeded
   */
  private static AdminService.BlockingInterface getCachedConnection(ClusterConnection connection,
      ServerName sn) throws IOException {
    if (sn == null) {
      return null;
    }
    AdminService.BlockingInterface service = null;
    try {
      service = connection.getAdmin(sn);
    } catch (RetriesExhaustedException e) {
      if (e.getCause() != null && e.getCause() instanceof ConnectException) {
        LOG.debug("Catch this; presume it means the cached connection has gone bad.");
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
        LOG.debug("Catch. Connect refused.");
      } else if (cause != null && cause instanceof EOFException) {
        LOG.debug("Catch. Other end disconnected us.");
      } else if (cause != null && cause.getMessage() != null &&
        cause.getMessage().toLowerCase(Locale.ROOT).contains("connection reset")) {
        LOG.debug("Catch. Connection reset.");
      } else {
        throw ioe;
      }

    }
    return service;
  }
}
