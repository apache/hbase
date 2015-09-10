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
import java.util.HashMap;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterManager.ServiceType;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
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
  private Admin admin;
  private final Connection connection;

  private ClusterManager clusterManager;

  public DistributedHBaseCluster(Configuration conf, ClusterManager clusterManager)
      throws IOException {
    super(conf);
    this.clusterManager = clusterManager;
    this.connection = ConnectionFactory.createConnection(conf);
    this.admin = this.connection.getAdmin();
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
  public void startRegionServer(String hostname) throws IOException {
    LOG.info("Starting RS on: " + hostname);
    clusterManager.start(ServiceType.HBASE_REGIONSERVER, hostname);
  }

  @Override
  public void killRegionServer(ServerName serverName) throws IOException {
    LOG.info("Aborting RS: " + serverName.getServerName());
    clusterManager.kill(ServiceType.HBASE_REGIONSERVER, serverName.getHostname());
  }

  @Override
  public void stopRegionServer(ServerName serverName) throws IOException {
    LOG.info("Stopping RS: " + serverName.getServerName());
    clusterManager.stop(ServiceType.HBASE_REGIONSERVER, serverName.getHostname());
  }

  @Override
  public void waitForRegionServerToStop(ServerName serverName, long timeout) throws IOException {
    waitForServiceToStop(ServiceType.HBASE_REGIONSERVER, serverName, timeout);
  }

  @Override
  public void startZkNode(String hostname, int port) throws IOException {
    LOG.info("Starting Zookeeper node on: " + hostname);
    clusterManager.start(ServiceType.ZOOKEEPER_SERVER, hostname);
  }

  @Override
  public void killZkNode(ServerName serverName) throws IOException {
    LOG.info("Aborting Zookeeper node on: " + serverName.getServerName());
    clusterManager.kill(ServiceType.ZOOKEEPER_SERVER,
      serverName.getHostname());
  }

  @Override
  public void stopZkNode(ServerName serverName) throws IOException {
    LOG.info("Stopping Zookeeper node: " + serverName.getServerName());
    clusterManager.stop(ServiceType.ZOOKEEPER_SERVER,
      serverName.getHostname());
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
      serverName.getHostname());
  }

  @Override
  public void killDataNode(ServerName serverName) throws IOException {
    LOG.info("Aborting data node on: " + serverName.getServerName());
    clusterManager.kill(ServiceType.HADOOP_DATANODE,
      serverName.getHostname());
  }

  @Override
  public void stopDataNode(ServerName serverName) throws IOException {
    LOG.info("Stopping data node on: " + serverName.getServerName());
    clusterManager.stop(ServiceType.HADOOP_DATANODE,
      serverName.getHostname());
  }

  @Override
  public void waitForDataNodeToStart(ServerName serverName, long timeout) throws IOException {
    waitForServiceToStart(ServiceType.HADOOP_DATANODE, serverName, timeout);
  }

  @Override
  public void waitForDataNodeToStop(ServerName serverName, long timeout) throws IOException {
    waitForServiceToStop(ServiceType.HADOOP_DATANODE, serverName, timeout);
  }

  private void waitForServiceToStop(ServiceType service, ServerName serverName, long timeout)
    throws IOException {
    LOG.info("Waiting for service: " + service + " to stop: " + serverName.getServerName());
    long start = System.currentTimeMillis();

    while ((System.currentTimeMillis() - start) < timeout) {
      if (!clusterManager.isRunning(service, serverName.getHostname())) {
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
      if (clusterManager.isRunning(service, serverName.getHostname())) {
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
  public void startMaster(String hostname) throws IOException {
    LOG.info("Starting Master on: " + hostname);
    clusterManager.start(ServiceType.HBASE_MASTER, hostname);
  }

  @Override
  public void killMaster(ServerName serverName) throws IOException {
    LOG.info("Aborting Master: " + serverName.getServerName());
    clusterManager.kill(ServiceType.HBASE_MASTER, serverName.getHostname());
  }

  @Override
  public void stopMaster(ServerName serverName) throws IOException {
    LOG.info("Stopping Master: " + serverName.getServerName());
    clusterManager.stop(ServiceType.HBASE_MASTER, serverName.getHostname());
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
    HRegionLocation regionLoc = null;
    try (RegionLocator locator = connection.getRegionLocator(tn)) {
      regionLoc = locator.getRegionLocation(regionName);
    }
    if (regionLoc == null) {
      LOG.warn("Cannot find region server holding region " + Bytes.toString(regionName) +
        ", start key [" + Bytes.toString(HRegionInfo.getStartKey(regionName)) + "]");
      return null;
    }

    AdminProtos.AdminService.BlockingInterface client =
        ((ClusterConnection)this.connection).getAdmin(regionLoc.getServerName());
    ServerInfo info = ProtobufUtil.getServerInfo(client);
    return ProtobufUtil.toServerName(info.getServerName());
  }

  @Override
  public void waitUntilShutDown() {
    //Simply wait for a few seconds for now (after issuing serverManager.kill
    throw new RuntimeException("Not implemented yet");
  }

  @Override
  public void shutdown() throws IOException {
    //not sure we want this
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
    if (!ServerName.isSameHostnameAndPort(initial.getMaster(), current.getMaster())) {
      LOG.info("Restoring cluster - Initial active master : " + initial.getMaster().getHostname()
          + " has changed to : " + current.getMaster().getHostname());
      // If initial master is stopped, start it, before restoring the state.
      // It will come up as a backup master, if there is already an active master.
      try {
        if (!clusterManager.isRunning(ServiceType.HBASE_MASTER, initial.getMaster().getHostname())) {
          LOG.info("Restoring cluster - starting initial active master at:" + initial.getMaster().getHostname());
          startMaster(initial.getMaster().getHostname());
        }

        //master has changed, we would like to undo this.
        //1. Kill the current backups
        //2. Stop current master
        //3. Start backup masters
        for (ServerName currentBackup : current.getBackupMasters()) {
          if (!ServerName.isSameHostnameAndPort(currentBackup, initial.getMaster())) {
            LOG.info("Restoring cluster - stopping backup master: " + currentBackup);
            stopMaster(currentBackup);
          }
        }
        LOG.info("Restoring cluster - stopping active master: " + current.getMaster());
        stopMaster(current.getMaster());
        waitForActiveAndReadyMaster(); //wait so that active master takes over
      } catch (IOException ex) {
        // if we fail to start the initial active master, we do not want to continue stopping
        // backup masters. Just keep what we have now
        deferred.add(ex);
      }

      //start backup masters
      for (ServerName backup : initial.getBackupMasters()) {
        try {
          //these are not started in backup mode, but we should already have an active master
          if(!clusterManager.isRunning(ServiceType.HBASE_MASTER, backup.getHostname())) {
            LOG.info("Restoring cluster - starting initial backup master: " + backup.getHostname());
            startMaster(backup.getHostname());
          }
        } catch (IOException ex) {
          deferred.add(ex);
        }
      }
    } else {
      //current master has not changed, match up backup masters
      HashMap<String, ServerName> initialBackups = new HashMap<String, ServerName>();
      HashMap<String, ServerName> currentBackups = new HashMap<String, ServerName>();

      for (ServerName server : initial.getBackupMasters()) {
        initialBackups.put(server.getHostname(), server);
      }
      for (ServerName server : current.getBackupMasters()) {
        currentBackups.put(server.getHostname(), server);
      }

      for (String hostname : Sets.difference(initialBackups.keySet(), currentBackups.keySet())) {
        try {
          if(!clusterManager.isRunning(ServiceType.HBASE_MASTER, hostname)) {
            LOG.info("Restoring cluster - starting initial backup master: " + hostname);
            startMaster(hostname);
          }
        } catch (IOException ex) {
          deferred.add(ex);
        }
      }

      for (String hostname : Sets.difference(currentBackups.keySet(), initialBackups.keySet())) {
        try {
          if(clusterManager.isRunning(ServiceType.HBASE_MASTER, hostname)) {
            LOG.info("Restoring cluster - stopping backup master: " + hostname);
            stopMaster(currentBackups.get(hostname));
          }
        } catch (IOException ex) {
          deferred.add(ex);
        }
      }
    }
    if (!deferred.isEmpty()) {
      LOG.warn("Restoring cluster - restoring region servers reported " + deferred.size() + " errors:");
      for (int i=0; i<deferred.size() && i < 3; i++) {
        LOG.warn(deferred.get(i));
      }
    }

    return deferred.isEmpty();
  }

  protected boolean restoreRegionServers(ClusterStatus initial, ClusterStatus current) {
    HashMap<String, ServerName> initialServers = new HashMap<String, ServerName>();
    HashMap<String, ServerName> currentServers = new HashMap<String, ServerName>();

    for (ServerName server : initial.getServers()) {
      initialServers.put(server.getHostname(), server);
    }
    for (ServerName server : current.getServers()) {
      currentServers.put(server.getHostname(), server);
    }

    List<IOException> deferred = new ArrayList<IOException>();
    for (String hostname : Sets.difference(initialServers.keySet(), currentServers.keySet())) {
      try {
        if(!clusterManager.isRunning(ServiceType.HBASE_REGIONSERVER, hostname)) {
          LOG.info("Restoring cluster - starting initial region server: " + hostname);
          startRegionServer(hostname);
        }
      } catch (IOException ex) {
        deferred.add(ex);
      }
    }

    for (String hostname : Sets.difference(currentServers.keySet(), initialServers.keySet())) {
      try {
        if(clusterManager.isRunning(ServiceType.HBASE_REGIONSERVER, hostname)) {
          LOG.info("Restoring cluster - stopping initial region server: " + hostname);
          stopRegionServer(currentServers.get(hostname));
        }
      } catch (IOException ex) {
        deferred.add(ex);
      }
    }
    if (!deferred.isEmpty()) {
      LOG.warn("Restoring cluster - restoring region servers reported " + deferred.size() + " errors:");
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
    this.admin = this.connection.getAdmin();
    LOG.info("Added new HBaseAdmin");
    return true;
  }
}
