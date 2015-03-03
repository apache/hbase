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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterManager.ServiceType;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ServerInfo;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

import com.google.common.collect.Sets;

/**
 * Manages the interactions with an already deployed distributed cluster (as opposed to
 * a pseudo-distributed, or mini/local cluster). This is used by integration and system tests.
 */
@InterfaceAudience.Private
public class DistributedHBaseCluster extends HBaseCluster {

  private HBaseAdmin admin;

  private ClusterManager clusterManager;

  public DistributedHBaseCluster(Configuration conf, ClusterManager clusterManager)
      throws IOException {
    super(conf);
    this.clusterManager = clusterManager;
    this.admin = new HBaseAdmin(conf);
    this.initialClusterStatus = getClusterStatus();
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
  public ClusterStatus getClusterStatus() throws IOException {
    return admin.getClusterStatus();
  }

  @Override
  public ClusterStatus getInitialClusterStatus() throws IOException {
    return initialClusterStatus;
  }

  @Override
  public void close() throws IOException {
    if (this.admin != null) {
      admin.close();
    }
  }

  @Override
  public AdminProtos.AdminService.BlockingInterface getAdminProtocol(ServerName serverName)
  throws IOException {
    return admin.getConnection().getAdmin(serverName);
  }

  @Override
  public ClientProtos.ClientService.BlockingInterface getClientProtocol(ServerName serverName)
  throws IOException {
    return admin.getConnection().getClient(serverName);
  }

  @Override
  public void startRegionServer(String hostname, int port) throws IOException {
    LOG.info("Starting RS on: " + hostname);
    clusterManager.start(ServiceType.HBASE_REGIONSERVER, hostname, port);
  }

  @Override
  public void killRegionServer(ServerName serverName) throws IOException {
    LOG.info("Aborting RS: " + serverName.getServerName());
    clusterManager.kill(ServiceType.HBASE_REGIONSERVER,
            serverName.getHostname(),
            serverName.getPort());
  }

  @Override
  public void stopRegionServer(ServerName serverName) throws IOException {
    LOG.info("Stopping RS: " + serverName.getServerName());
    clusterManager.stop(ServiceType.HBASE_REGIONSERVER,
            serverName.getHostname(),
            serverName.getPort());
  }

  @Override
  public void waitForRegionServerToStop(ServerName serverName, long timeout) throws IOException {
    waitForServiceToStop(ServiceType.HBASE_REGIONSERVER, serverName, timeout);
  }

  private void waitForServiceToStop(ServiceType service, ServerName serverName, long timeout)
    throws IOException {
    LOG.info("Waiting service:" + service + " to stop: " + serverName.getServerName());
    long start = System.currentTimeMillis();

    while ((System.currentTimeMillis() - start) < timeout) {
      if (!clusterManager.isRunning(service, serverName.getHostname(), serverName.getPort())) {
        return;
      }
      Threads.sleep(1000);
    }
    throw new IOException("did timeout waiting for service to stop:" + serverName);
  }

  @Override
  public MasterService.BlockingInterface getMaster()
  throws IOException {
    HConnection conn = HConnectionManager.getConnection(conf);
    return conn.getMaster();
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
        getMaster();
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
  public ServerName getServerHoldingRegion(byte[] regionName) throws IOException {
    HConnection connection = admin.getConnection();
    HRegionLocation regionLoc = connection.locateRegion(regionName);
    if (regionLoc == null) {
      LOG.warn("Cannot find region server holding region " + Bytes.toString(regionName)
          + " for table " + HRegionInfo.getTableName(regionName) + ", start key [" +
          Bytes.toString(HRegionInfo.getStartKey(regionName)) + "]");
      return null;
    }

    AdminProtos.AdminService.BlockingInterface client =
      connection.getAdmin(regionLoc.getServerName());
    ServerInfo info = ProtobufUtil.getServerInfo(client);
    return ProtobufUtil.toServerName(info.getServerName());
  }

  @Override
  public void waitUntilShutDown() {
    // Simply wait for a few seconds for now (after issuing serverManager.kill
    throw new RuntimeException("Not implemented yet");
  }

  @Override
  public void shutdown() throws IOException {
    // not sure we want this
    throw new RuntimeException("Not implemented yet");
  }

  @Override
  public boolean isDistributedCluster() {
    return true;
  }

  @Override
  public boolean restoreClusterStatus(ClusterStatus initial) throws IOException {
    ClusterStatus current = getClusterStatus();

    LOG.info("Restoring cluster - started");

    // do a best effort restore
    boolean success = true;
    success = restoreMasters(initial, current) & success;
    success = restoreRegionServers(initial, current) & success;
    success = restoreAdmin() & success;

    LOG.info("Restoring cluster - done");
    return success;
  }

  protected boolean restoreMasters(ClusterStatus initial, ClusterStatus current) {
    List<IOException> deferred = new ArrayList<IOException>();
    //check whether current master has changed
    final ServerName initMaster = initial.getMaster();
    if (!ServerName.isSameHostnameAndPort(initMaster, current.getMaster())) {
      LOG.info("Restoring cluster - Initial active master : "
              + initMaster.getHostAndPort()
              + " has changed to : "
              + current.getMaster().getHostAndPort());
      // If initial master is stopped, start it, before restoring the state.
      // It will come up as a backup master, if there is already an active master.
      try {
        if (!clusterManager.isRunning(ServiceType.HBASE_MASTER,
                initMaster.getHostname(), initMaster.getPort())) {
          LOG.info("Restoring cluster - starting initial active master at:"
                  + initMaster.getHostAndPort());
          startMaster(initMaster.getHostname(), initMaster.getPort());
        }

        // master has changed, we would like to undo this.
        // 1. Kill the current backups
        // 2. Stop current master
        // 3. Start backup masters
        for (ServerName currentBackup : current.getBackupMasters()) {
          if (!ServerName.isSameHostnameAndPort(currentBackup, initMaster)) {
            LOG.info("Restoring cluster - stopping backup master: " + currentBackup);
            stopMaster(currentBackup);
          }
        }
        LOG.info("Restoring cluster - stopping active master: " + current.getMaster());
        stopMaster(current.getMaster());
        waitForActiveAndReadyMaster(); // wait so that active master takes over
      } catch (IOException ex) {
        // if we fail to start the initial active master, we do not want to continue stopping
        // backup masters. Just keep what we have now
        deferred.add(ex);
      }

      //start backup masters
      for (ServerName backup : initial.getBackupMasters()) {
        try {
          //these are not started in backup mode, but we should already have an active master
          if (!clusterManager.isRunning(ServiceType.HBASE_MASTER,
                  backup.getHostname(),
                  backup.getPort())) {
            LOG.info("Restoring cluster - starting initial backup master: "
                    + backup.getHostAndPort());
            startMaster(backup.getHostname(), backup.getPort());
          }
        } catch (IOException ex) {
          deferred.add(ex);
        }
      }
    } else {
      //current master has not changed, match up backup masters
      Set<ServerName> toStart = new TreeSet<ServerName>(new ServerNameIgnoreStartCodeComparator());
      Set<ServerName> toKill = new TreeSet<ServerName>(new ServerNameIgnoreStartCodeComparator());
      toStart.addAll(initial.getBackupMasters());
      toKill.addAll(current.getBackupMasters());

      for (ServerName server : current.getBackupMasters()) {
        toStart.remove(server);
      }
      for (ServerName server: initial.getBackupMasters()) {
        toKill.remove(server);
      }

      for (ServerName sn:toStart) {
        try {
          if(!clusterManager.isRunning(ServiceType.HBASE_MASTER, sn.getHostname(), sn.getPort())) {
            LOG.info("Restoring cluster - starting initial backup master: " + sn.getHostAndPort());
            startMaster(sn.getHostname(), sn.getPort());
          }
        } catch (IOException ex) {
          deferred.add(ex);
        }
      }

      for (ServerName sn:toKill) {
        try {
          if(clusterManager.isRunning(ServiceType.HBASE_MASTER, sn.getHostname(), sn.getPort())) {
            LOG.info("Restoring cluster - stopping backup master: " + sn.getHostAndPort());
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
        LOG.warn(deferred.get(i));
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

  protected boolean restoreRegionServers(ClusterStatus initial, ClusterStatus current) {
    Set<ServerName> toStart = new TreeSet<ServerName>(new ServerNameIgnoreStartCodeComparator());
    Set<ServerName> toKill = new TreeSet<ServerName>(new ServerNameIgnoreStartCodeComparator());
    toStart.addAll(initial.getBackupMasters());
    toKill.addAll(current.getBackupMasters());

    for (ServerName server : current.getServers()) {
      toStart.remove(server);
    }
    for (ServerName server: initial.getServers()) {
      toKill.remove(server);
    }

    List<IOException> deferred = new ArrayList<IOException>();

    for(ServerName sn:toStart) {
      try {
        if (!clusterManager.isRunning(ServiceType.HBASE_REGIONSERVER,
                sn.getHostname(),
                sn.getPort())) {
          LOG.info("Restoring cluster - starting initial region server: " + sn.getHostAndPort());
          startRegionServer(sn.getHostname(), sn.getPort());
        }
      } catch (IOException ex) {
        deferred.add(ex);
      }
    }

    for(ServerName sn:toKill) {
      try {
        if (clusterManager.isRunning(ServiceType.HBASE_REGIONSERVER,
                sn.getHostname(),
                sn.getPort())) {
          LOG.info("Restoring cluster - stopping initial region server: " + sn.getHostAndPort());
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
        LOG.warn(deferred.get(i));
      }
    }

    return deferred.isEmpty();
  }

  protected boolean restoreAdmin() throws IOException {
    // While restoring above, if the HBase Master which was initially the Active one, was down
    // and the restore put the cluster back to Initial configuration, HAdmin instance will need
    // to refresh its connections (otherwise it will return incorrect information) or we can
    // point it to new instance.
    try {
      admin.close();
    } catch (IOException ioe) {
      LOG.warn("While closing the old connection", ioe);
    }
    this.admin = new HBaseAdmin(conf);
    LOG.info("Added new HBaseAdmin");
    return true;
  }
}
