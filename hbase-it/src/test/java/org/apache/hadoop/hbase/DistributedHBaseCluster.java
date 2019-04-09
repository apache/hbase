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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterManager.ServiceType;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;

/**
 * Manages the interactions with an already deployed distributed cluster (as opposed to
 * a pseudo-distributed, or mini/local cluster). This is used by integration and system tests.
 */
@InterfaceAudience.Private
public class DistributedHBaseCluster extends HBaseCluster {
  private Admin admin;
  private final Connection connection;

  private ClusterManager clusterManager;
  /**
   * List of RegionServers killed so far. ServerName also comprises startCode of a server,
   * so any restarted instances of the same server will have different ServerName and will not
   * coincide with past dead ones. So there's no need to cleanup this list.
   */
  private Set<ServerName> killedRegionServers = new HashSet<>();

  public DistributedHBaseCluster(Configuration conf, ClusterManager clusterManager)
      throws IOException {
    super(conf);
    this.clusterManager = clusterManager;
    this.connection = ConnectionFactory.createConnection(conf);
    this.admin = this.connection.getAdmin();
    this.initialClusterStatus = getClusterMetrics();
  }

  public void setClusterManager(ClusterManager clusterManager) {
    this.clusterManager = clusterManager;
  }

  public ClusterManager getClusterManager() {
    return clusterManager;
  }

  /**
   * Returns a ClusterStatus for this HBase cluster
   * @throws IOException
   */
  @Override
  public ClusterMetrics getClusterMetrics() throws IOException {
    return admin.getClusterMetrics();
  }

  @Override
  public ClusterMetrics getInitialClusterMetrics() throws IOException {
    return initialClusterStatus;
  }

  @Override
  public void close() throws IOException {
    if (this.admin != null) {
      admin.close();
    }
    if (this.connection != null && !this.connection.isClosed()) {
      this.connection.close();
    }
  }

  @Override
  public AdminProtos.AdminService.BlockingInterface getAdminProtocol(ServerName serverName)
  throws IOException {
    return ((ClusterConnection)this.connection).getAdmin(serverName);
  }

  @Override
  public ClientProtos.ClientService.BlockingInterface getClientProtocol(ServerName serverName)
  throws IOException {
    return ((ClusterConnection)this.connection).getClient(serverName);
  }

  @Override
  public void startRegionServer(String hostname, int port) throws IOException {
    LOG.info("Starting RS on: " + hostname);
    clusterManager.start(ServiceType.HBASE_REGIONSERVER, hostname, port);
  }

  @Override
  public void killRegionServer(ServerName serverName) throws IOException {
    LOG.info("Aborting RS: " + serverName.getServerName());
    killedRegionServers.add(serverName);
    clusterManager.kill(ServiceType.HBASE_REGIONSERVER,
      serverName.getHostname(), serverName.getPort());
  }

  @Override
  public boolean isKilledRS(ServerName serverName) {
    return killedRegionServers.contains(serverName);
  }

  @Override
  public void stopRegionServer(ServerName serverName) throws IOException {
    LOG.info("Stopping RS: " + serverName.getServerName());
    clusterManager.stop(ServiceType.HBASE_REGIONSERVER,
      serverName.getHostname(), serverName.getPort());
  }

  @Override
  public void waitForRegionServerToStop(ServerName serverName, long timeout) throws IOException {
    waitForServiceToStop(ServiceType.HBASE_REGIONSERVER, serverName, timeout);
  }

  @Override
  public void startZkNode(String hostname, int port) throws IOException {
    LOG.info("Starting ZooKeeper node on: " + hostname);
    clusterManager.start(ServiceType.ZOOKEEPER_SERVER, hostname, port);
  }

  @Override
  public void killZkNode(ServerName serverName) throws IOException {
    LOG.info("Aborting ZooKeeper node on: " + serverName.getServerName());
    clusterManager.kill(ServiceType.ZOOKEEPER_SERVER,
      serverName.getHostname(), serverName.getPort());
  }

  @Override
  public void stopZkNode(ServerName serverName) throws IOException {
    LOG.info("Stopping ZooKeeper node: " + serverName.getServerName());
    clusterManager.stop(ServiceType.ZOOKEEPER_SERVER,
      serverName.getHostname(), serverName.getPort());
  }

  @Override
  public void waitForZkNodeToStart(ServerName serverName, long timeout) throws IOException {
    waitForServiceToStart(ServiceType.ZOOKEEPER_SERVER, serverName, timeout);
  }

  @Override
  public void waitForZkNodeToStop(ServerName serverName, long timeout) throws IOException {
    waitForServiceToStop(ServiceType.ZOOKEEPER_SERVER, serverName, timeout);
  }

  @Override
  public void startDataNode(ServerName serverName) throws IOException {
    LOG.info("Starting data node on: " + serverName.getServerName());
    clusterManager.start(ServiceType.HADOOP_DATANODE,
      serverName.getHostname(), serverName.getPort());
  }

  @Override
  public void killDataNode(ServerName serverName) throws IOException {
    LOG.info("Aborting data node on: " + serverName.getServerName());
    clusterManager.kill(ServiceType.HADOOP_DATANODE,
      serverName.getHostname(), serverName.getPort());
  }

  @Override
  public void stopDataNode(ServerName serverName) throws IOException {
    LOG.info("Stopping data node on: " + serverName.getServerName());
    clusterManager.stop(ServiceType.HADOOP_DATANODE,
      serverName.getHostname(), serverName.getPort());
  }

  @Override
  public void waitForDataNodeToStart(ServerName serverName, long timeout) throws IOException {
    waitForServiceToStart(ServiceType.HADOOP_DATANODE, serverName, timeout);
  }

  @Override
  public void waitForDataNodeToStop(ServerName serverName, long timeout) throws IOException {
    waitForServiceToStop(ServiceType.HADOOP_DATANODE, serverName, timeout);
  }

  @Override
  public void startNameNode(ServerName serverName) throws IOException {
    LOG.info("Starting name node on: " + serverName.getServerName());
    clusterManager.start(ServiceType.HADOOP_NAMENODE, serverName.getHostname(),
      serverName.getPort());
  }

  @Override
  public void killNameNode(ServerName serverName) throws IOException {
    LOG.info("Aborting name node on: " + serverName.getServerName());
    clusterManager.kill(ServiceType.HADOOP_NAMENODE, serverName.getHostname(),
      serverName.getPort());
  }

  @Override
  public void stopNameNode(ServerName serverName) throws IOException {
    LOG.info("Stopping name node on: " + serverName.getServerName());
    clusterManager.stop(ServiceType.HADOOP_NAMENODE, serverName.getHostname(),
      serverName.getPort());
  }

  @Override
  public void waitForNameNodeToStart(ServerName serverName, long timeout) throws IOException {
    waitForServiceToStart(ServiceType.HADOOP_NAMENODE, serverName, timeout);
  }

  @Override
  public void waitForNameNodeToStop(ServerName serverName, long timeout) throws IOException {
    waitForServiceToStop(ServiceType.HADOOP_NAMENODE, serverName, timeout);
  }

  private void waitForServiceToStop(ServiceType service, ServerName serverName, long timeout)
    throws IOException {
    LOG.info("Waiting for service: " + service + " to stop: " + serverName.getServerName());
    long start = System.currentTimeMillis();

    while ((System.currentTimeMillis() - start) < timeout) {
      if (!clusterManager.isRunning(service, serverName.getHostname(), serverName.getPort())) {
        return;
      }
      Threads.sleep(100);
    }
    throw new IOException("did timeout waiting for service to stop:" + serverName);
  }

  private void waitForServiceToStart(ServiceType service, ServerName serverName, long timeout)
    throws IOException {
    LOG.info("Waiting for service: " + service + " to start: " + serverName.getServerName());
    long start = System.currentTimeMillis();

    while ((System.currentTimeMillis() - start) < timeout) {
      if (clusterManager.isRunning(service, serverName.getHostname(), serverName.getPort())) {
        return;
      }
      Threads.sleep(100);
    }
    throw new IOException("did timeout waiting for service to start:" + serverName);
  }


  @Override
  public MasterService.BlockingInterface getMasterAdminService()
  throws IOException {
    return ((ClusterConnection)this.connection).getMaster();
  }

  @Override
  public void startMaster(String hostname, int port) throws IOException {
    LOG.info("Starting Master on: " + hostname + ":" + port);
    clusterManager.start(ServiceType.HBASE_MASTER, hostname, port);
  }

  @Override
  public void killMaster(ServerName serverName) throws IOException {
    LOG.info("Aborting Master: " + serverName.getServerName());
    clusterManager.kill(ServiceType.HBASE_MASTER, serverName.getHostname(), serverName.getPort());
  }

  @Override
  public void stopMaster(ServerName serverName) throws IOException {
    LOG.info("Stopping Master: " + serverName.getServerName());
    clusterManager.stop(ServiceType.HBASE_MASTER, serverName.getHostname(), serverName.getPort());
  }

  @Override
  public void waitForMasterToStop(ServerName serverName, long timeout) throws IOException {
    waitForServiceToStop(ServiceType.HBASE_MASTER, serverName, timeout);
  }

  @Override
  public boolean waitForActiveAndReadyMaster(long timeout) throws IOException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      try {
        getMasterAdminService();
        return true;
      } catch (MasterNotRunningException m) {
        LOG.warn("Master not started yet " + m);
      } catch (ZooKeeperConnectionException e) {
        LOG.warn("Failed to connect to ZK " + e);
      }
      Threads.sleep(1000);
    }
    return false;
  }

  @Override
  public ServerName getServerHoldingRegion(TableName tn, byte[] regionName) throws IOException {
    byte[] startKey = RegionInfo.getStartKey(regionName);
    HRegionLocation regionLoc = null;
    try (RegionLocator locator = connection.getRegionLocator(tn)) {
      regionLoc = locator.getRegionLocation(startKey, true);
    }
    if (regionLoc == null) {
      LOG.warn("Cannot find region server holding region " + Bytes.toStringBinary(regionName));
      return null;
    }
    return regionLoc.getServerName();
  }

  @Override
  public void waitUntilShutDown() {
    // Simply wait for a few seconds for now (after issuing serverManager.kill
    throw new RuntimeException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public void shutdown() throws IOException {
    // not sure we want this
    throw new RuntimeException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public boolean isDistributedCluster() {
    return true;
  }

  @Override
  public boolean restoreClusterMetrics(ClusterMetrics initial) throws IOException {
    ClusterMetrics current = getClusterMetrics();

    LOG.info("Restoring cluster - started");

    // do a best effort restore
    boolean success = true;
    success = restoreMasters(initial, current) & success;
    success = restoreRegionServers(initial, current) & success;
    success = restoreAdmin() & success;

    LOG.info("Restoring cluster - done");
    return success;
  }

  protected boolean restoreMasters(ClusterMetrics initial, ClusterMetrics current) {
    List<IOException> deferred = new ArrayList<>();
    //check whether current master has changed
    final ServerName initMaster = initial.getMasterName();
    if (!ServerName.isSameAddress(initMaster, current.getMasterName())) {
      LOG.info("Restoring cluster - Initial active master : " + initMaster.getAddress() +
        " has changed to : " + current.getMasterName().getAddress());
      // If initial master is stopped, start it, before restoring the state.
      // It will come up as a backup master, if there is already an active master.
      try {
        if (!clusterManager.isRunning(ServiceType.HBASE_MASTER,
                initMaster.getHostname(), initMaster.getPort())) {
          LOG.info("Restoring cluster - starting initial active master at:"
                  + initMaster.getAddress());
          startMaster(initMaster.getHostname(), initMaster.getPort());
        }

        // master has changed, we would like to undo this.
        // 1. Kill the current backups
        // 2. Stop current master
        // 3. Start backup masters
        for (ServerName currentBackup : current.getBackupMasterNames()) {
          if (!ServerName.isSameAddress(currentBackup, initMaster)) {
            LOG.info("Restoring cluster - stopping backup master: " + currentBackup);
            stopMaster(currentBackup);
          }
        }
        LOG.info("Restoring cluster - stopping active master: " + current.getMasterName());
        stopMaster(current.getMasterName());
        waitForActiveAndReadyMaster(); // wait so that active master takes over
      } catch (IOException ex) {
        // if we fail to start the initial active master, we do not want to continue stopping
        // backup masters. Just keep what we have now
        deferred.add(ex);
      }

      //start backup masters
      for (ServerName backup : initial.getBackupMasterNames()) {
        try {
          //these are not started in backup mode, but we should already have an active master
          if (!clusterManager.isRunning(ServiceType.HBASE_MASTER,
                  backup.getHostname(),
                  backup.getPort())) {
            LOG.info("Restoring cluster - starting initial backup master: "
                    + backup.getAddress());
            startMaster(backup.getHostname(), backup.getPort());
          }
        } catch (IOException ex) {
          deferred.add(ex);
        }
      }
    } else {
      //current master has not changed, match up backup masters
      Set<ServerName> toStart = new TreeSet<>(new ServerNameIgnoreStartCodeComparator());
      Set<ServerName> toKill = new TreeSet<>(new ServerNameIgnoreStartCodeComparator());
      toStart.addAll(initial.getBackupMasterNames());
      toKill.addAll(current.getBackupMasterNames());

      for (ServerName server : current.getBackupMasterNames()) {
        toStart.remove(server);
      }
      for (ServerName server: initial.getBackupMasterNames()) {
        toKill.remove(server);
      }

      for (ServerName sn:toStart) {
        try {
          if(!clusterManager.isRunning(ServiceType.HBASE_MASTER, sn.getHostname(), sn.getPort())) {
            LOG.info("Restoring cluster - starting initial backup master: " + sn.getAddress());
            startMaster(sn.getHostname(), sn.getPort());
          }
        } catch (IOException ex) {
          deferred.add(ex);
        }
      }

      for (ServerName sn:toKill) {
        try {
          if(clusterManager.isRunning(ServiceType.HBASE_MASTER, sn.getHostname(), sn.getPort())) {
            LOG.info("Restoring cluster - stopping backup master: " + sn.getAddress());
            stopMaster(sn);
          }
        } catch (IOException ex) {
          deferred.add(ex);
        }
      }
    }
    if (!deferred.isEmpty()) {
      LOG.warn("Restoring cluster - restoring region servers reported "
              + deferred.size() + " errors:");
      for (int i=0; i<deferred.size() && i < 3; i++) {
        LOG.warn(Objects.toString(deferred.get(i)));
      }
    }

    return deferred.isEmpty();
  }


  private static class ServerNameIgnoreStartCodeComparator implements Comparator<ServerName> {
    @Override
    public int compare(ServerName o1, ServerName o2) {
      int compare = o1.getHostname().compareToIgnoreCase(o2.getHostname());
      if (compare != 0) return compare;
      compare = o1.getPort() - o2.getPort();
      if (compare != 0) return compare;
      return 0;
    }
  }

  protected boolean restoreRegionServers(ClusterMetrics initial, ClusterMetrics current) {
    Set<ServerName> toStart = new TreeSet<>(new ServerNameIgnoreStartCodeComparator());
    Set<ServerName> toKill = new TreeSet<>(new ServerNameIgnoreStartCodeComparator());
    toStart.addAll(initial.getLiveServerMetrics().keySet());
    toKill.addAll(current.getLiveServerMetrics().keySet());

    ServerName master = initial.getMasterName();

    for (ServerName server : current.getLiveServerMetrics().keySet()) {
      toStart.remove(server);
    }
    for (ServerName server: initial.getLiveServerMetrics().keySet()) {
      toKill.remove(server);
    }

    List<IOException> deferred = new ArrayList<>();

    for(ServerName sn:toStart) {
      try {
        if (!clusterManager.isRunning(ServiceType.HBASE_REGIONSERVER, sn.getHostname(),
          sn.getPort()) && master.getPort() != sn.getPort()) {
          LOG.info("Restoring cluster - starting initial region server: " + sn.getAddress());
          startRegionServer(sn.getHostname(), sn.getPort());
        }
      } catch (IOException ex) {
        deferred.add(ex);
      }
    }

    for(ServerName sn:toKill) {
      try {
        if (clusterManager.isRunning(ServiceType.HBASE_REGIONSERVER, sn.getHostname(),
          sn.getPort()) && master.getPort() != sn.getPort()) {
          LOG.info("Restoring cluster - stopping initial region server: " + sn.getAddress());
          stopRegionServer(sn);
        }
      } catch (IOException ex) {
        deferred.add(ex);
      }
    }
    if (!deferred.isEmpty()) {
      LOG.warn("Restoring cluster - restoring region servers reported "
              + deferred.size() + " errors:");
      for (int i=0; i<deferred.size() && i < 3; i++) {
        LOG.warn(Objects.toString(deferred.get(i)));
      }
    }

    return deferred.isEmpty();
  }

  protected boolean restoreAdmin() throws IOException {
    // While restoring above, if the HBase Master which was initially the Active one, was down
    // and the restore put the cluster back to Initial configuration, HAdmin instance will need
    // to refresh its connections (otherwise it will return incorrect information) or we can
    // point it to new instance.
    admin.close();
    this.admin = this.connection.getAdmin();
    LOG.info("Added new HBaseAdmin");
    return true;
  }
}
