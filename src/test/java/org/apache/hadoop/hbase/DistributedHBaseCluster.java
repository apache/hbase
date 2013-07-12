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
import java.util.HashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterManager.ServiceType;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
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

  private void waitForServiceToStop(ServiceType service, ServerName serverName, long timeout)
    throws IOException {
    LOG.info("Waiting service:" + service + " to stop: " + serverName.getServerName());
    long start = System.currentTimeMillis();

    while ((System.currentTimeMillis() - start) < timeout) {
      if (!clusterManager.isRunning(service, serverName.getHostname())) {
        return;
      }
      Threads.sleep(1000);
    }
    throw new IOException("did timeout waiting for service to stop:" + serverName);
  }

  @Override
  public HMasterInterface getMasterAdmin() throws IOException {
    HConnection conn = HConnectionManager.getConnection(conf);
    return conn.getMaster();
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
        getMasterAdmin();
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
      return null;
    }

    org.apache.hadoop.hbase.HServerInfo sn
		= connection.getHRegionConnection(regionLoc.getHostname(), regionLoc.getPort()).getHServerInfo();

    return new ServerName(sn.getServerAddress().getHostname(), sn.getServerAddress().getPort(), sn.getStartCode());
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
  public void restoreClusterStatus(ClusterStatus initial) throws IOException {
    //TODO: caution: not tested throughly
    ClusterStatus current = getClusterStatus();

    //restore masters

    //check whether current master has changed
    if (!ServerName.isSameHostnameAndPort(initial.getMaster(), current.getMaster())) {
      //master has changed, we would like to undo this.
      //1. Kill the current backups
      //2. Stop current master
      //3. Start a master at the initial hostname (if not already running as backup)
      //4. Start backup masters
      boolean foundOldMaster = false;
      for (ServerName currentBackup : current.getBackupMasters()) {
        if (!ServerName.isSameHostnameAndPort(currentBackup, initial.getMaster())) {
          stopMaster(currentBackup);
        } else {
          foundOldMaster = true;
        }
      }
      stopMaster(current.getMaster());
      if (foundOldMaster) { //if initial master is not running as a backup
        startMaster(initial.getMaster().getHostname());
      }
      waitForActiveAndReadyMaster(); //wait so that active master takes over

      //start backup masters
      for (ServerName backup : initial.getBackupMasters()) {
        //these are not started in backup mode, but we should already have an active master
        if(!clusterManager.isRunning(ServiceType.HBASE_MASTER, backup.getHostname())) {
          startMaster(backup.getHostname());
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
        if(!clusterManager.isRunning(ServiceType.HBASE_MASTER, hostname)) {
          startMaster(hostname);
        }
      }

      for (String hostname : Sets.difference(currentBackups.keySet(), initialBackups.keySet())) {
        if(clusterManager.isRunning(ServiceType.HBASE_MASTER, hostname)) {
          stopMaster(currentBackups.get(hostname));
        }
      }
    }

    //restore region servers
    HashMap<String, ServerName> initialServers = new HashMap<String, ServerName>();
    HashMap<String, ServerName> currentServers = new HashMap<String, ServerName>();

    for (ServerName server : initial.getServers()) {
      initialServers.put(server.getHostname(), server);
    }
    for (ServerName server : current.getServers()) {
      currentServers.put(server.getHostname(), server);
    }

    for (String hostname : Sets.difference(initialServers.keySet(), currentServers.keySet())) {
      if(!clusterManager.isRunning(ServiceType.HBASE_REGIONSERVER, hostname)) {
        startRegionServer(hostname);
      }
    }

    for (String hostname : Sets.difference(currentServers.keySet(), initialServers.keySet())) {
      if(clusterManager.isRunning(ServiceType.HBASE_REGIONSERVER, hostname)) {
        stopRegionServer(currentServers.get(hostname));
      }
    }
  }
}
