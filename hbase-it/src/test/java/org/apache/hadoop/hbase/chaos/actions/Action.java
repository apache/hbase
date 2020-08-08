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

package org.apache.hadoop.hbase.chaos.actions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

/**
 * A (possibly mischievous) action that the ChaosMonkey can perform.
 */
public abstract class Action {

  public static final String KILL_MASTER_TIMEOUT_KEY =
    "hbase.chaosmonkey.action.killmastertimeout";
  public static final String START_MASTER_TIMEOUT_KEY =
    "hbase.chaosmonkey.action.startmastertimeout";
  public static final String KILL_RS_TIMEOUT_KEY = "hbase.chaosmonkey.action.killrstimeout";
  public static final String START_RS_TIMEOUT_KEY = "hbase.chaosmonkey.action.startrstimeout";
  public static final String KILL_ZK_NODE_TIMEOUT_KEY =
    "hbase.chaosmonkey.action.killzknodetimeout";
  public static final String START_ZK_NODE_TIMEOUT_KEY =
    "hbase.chaosmonkey.action.startzknodetimeout";
  public static final String KILL_DATANODE_TIMEOUT_KEY =
    "hbase.chaosmonkey.action.killdatanodetimeout";
  public static final String START_DATANODE_TIMEOUT_KEY =
    "hbase.chaosmonkey.action.startdatanodetimeout";
  public static final String KILL_NAMENODE_TIMEOUT_KEY =
      "hbase.chaosmonkey.action.killnamenodetimeout";
  public static final String START_NAMENODE_TIMEOUT_KEY =
      "hbase.chaosmonkey.action.startnamenodetimeout";

  protected static final long KILL_MASTER_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long START_MASTER_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long KILL_RS_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long START_RS_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long KILL_ZK_NODE_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long START_ZK_NODE_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long KILL_DATANODE_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long START_DATANODE_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long KILL_NAMENODE_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long START_NAMENODE_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;

  protected ActionContext context;
  protected HBaseCluster cluster;
  protected ClusterStatus initialStatus;
  protected ServerName[] initialServers;

  protected long killMasterTimeout;
  protected long startMasterTimeout;
  protected long killRsTimeout;
  protected long startRsTimeout;
  protected long killZkNodeTimeout;
  protected long startZkNodeTimeout;
  protected long killDataNodeTimeout;
  protected long startDataNodeTimeout;
  protected long killNameNodeTimeout;
  protected long startNameNodeTimeout;

  public void init(ActionContext context) throws IOException {
    this.context = context;
    cluster = context.getHBaseCluster();
    initialStatus = cluster.getInitialClusterStatus();
    Collection<ServerName> regionServers = initialStatus.getServers();
    initialServers = regionServers.toArray(new ServerName[regionServers.size()]);

    killMasterTimeout = cluster.getConf().getLong(KILL_MASTER_TIMEOUT_KEY,
      KILL_MASTER_TIMEOUT_DEFAULT);
    startMasterTimeout = cluster.getConf().getLong(START_MASTER_TIMEOUT_KEY,
      START_MASTER_TIMEOUT_DEFAULT);
    killRsTimeout = cluster.getConf().getLong(KILL_RS_TIMEOUT_KEY, KILL_RS_TIMEOUT_DEFAULT);
    startRsTimeout = cluster.getConf().getLong(START_RS_TIMEOUT_KEY, START_RS_TIMEOUT_DEFAULT);
    killZkNodeTimeout = cluster.getConf().getLong(KILL_ZK_NODE_TIMEOUT_KEY,
      KILL_ZK_NODE_TIMEOUT_DEFAULT);
    startZkNodeTimeout = cluster.getConf().getLong(START_ZK_NODE_TIMEOUT_KEY,
      START_ZK_NODE_TIMEOUT_DEFAULT);
    killDataNodeTimeout = cluster.getConf().getLong(KILL_DATANODE_TIMEOUT_KEY,
      KILL_DATANODE_TIMEOUT_DEFAULT);
    startDataNodeTimeout = cluster.getConf().getLong(START_DATANODE_TIMEOUT_KEY,
      START_DATANODE_TIMEOUT_DEFAULT);
    killNameNodeTimeout =
        cluster.getConf().getLong(KILL_NAMENODE_TIMEOUT_KEY, KILL_NAMENODE_TIMEOUT_DEFAULT);
    startNameNodeTimeout =
        cluster.getConf().getLong(START_NAMENODE_TIMEOUT_KEY, START_NAMENODE_TIMEOUT_DEFAULT);
  }

  /**
   * Retrieve the instance's {@link Logger}, for use throughout the class hierarchy.
   */
  protected abstract Logger getLogger();

  public void perform() throws Exception { }

  /** Returns current region servers - active master */
  protected ServerName[] getCurrentServers() throws IOException {
    ClusterStatus clusterStatus = cluster.getClusterStatus();
    Collection<ServerName> regionServers = clusterStatus.getServers();
    int count = regionServers == null ? 0 : regionServers.size();
    if (count <= 0) {
      return new ServerName [] {};
    }
    ServerName master = clusterStatus.getMaster();
    Set<ServerName> masters = new HashSet<ServerName>();
    masters.add(master);
    masters.addAll(clusterStatus.getBackupMasters());
    ArrayList<ServerName> tmp = new ArrayList<>(count);
    tmp.addAll(regionServers);
    tmp.removeAll(masters);
    return tmp.toArray(new ServerName[0]);
  }

  protected void killMaster(ServerName server) throws IOException {
    getLogger().info("Killing master:" + server);
    cluster.killMaster(server);
    cluster.waitForMasterToStop(server, killMasterTimeout);
    getLogger().info("Killed master server:" + server);
  }

  protected void startMaster(ServerName server) throws IOException {
    getLogger().info("Starting master:" + server.getHostname());
    cluster.startMaster(server.getHostname(), server.getPort());
    cluster.waitForActiveAndReadyMaster(startMasterTimeout);
    getLogger().info("Started master: " + server);
  }

  protected void stopRs(ServerName server) throws IOException {
    getLogger().info("Stopping regionserver " + server);
    cluster.stopRegionServer(server);
    cluster.waitForRegionServerToStop(server, killRsTimeout);
    getLogger().info(String.format("Stopping regionserver %s. Reported num of rs: %s", server,
        cluster.getClusterStatus().getLiveServersLoad().size()));
  }

  protected void suspendRs(ServerName server) throws IOException {
    getLogger().info("Suspending regionserver %s" + server);
    cluster.suspendRegionServer(server);
    if(!(cluster instanceof MiniHBaseCluster)){
      cluster.waitForRegionServerToStop(server, killRsTimeout);
    }
    getLogger().info(String.format("Suspending regionserver %s. Reported num of rs: %s", server,
        cluster.getClusterStatus().getLiveServersLoad().size()));
  }

  protected void resumeRs(ServerName server) throws IOException {
    getLogger().info("Resuming regionserver " + server);
    cluster.resumeRegionServer(server);
    if(!(cluster instanceof MiniHBaseCluster)){
      cluster.waitForRegionServerToStart(server.getHostname(), server.getPort(), startRsTimeout);
    }
    getLogger().info(String.format("Resuming regionserver %s. Reported num of rs: %s", server,
        cluster.getClusterStatus().getLiveServersLoad().size()));
  }

  protected void killRs(ServerName server) throws IOException {
    getLogger().info("Killing regionserver " + server);
    cluster.killRegionServer(server);
    cluster.waitForRegionServerToStop(server, killRsTimeout);
    getLogger().info(String.format("Killed regionserver %s. Reported num of rs: %s", server,
        cluster.getClusterStatus().getLiveServersLoad().size()));
  }

  protected void startRs(ServerName server) throws IOException {
    getLogger().info("Starting regionserver " + server.getAddress());
    cluster.startRegionServer(server.getHostname(), server.getPort());
    cluster.waitForRegionServerToStart(server.getHostname(), server.getPort(), startRsTimeout);
    getLogger().info(String.format("Started regionserver %s. Reported num of rs: %s",
      server.getAddress(), cluster.getClusterStatus().getLiveServersLoad().size()));
  }

  protected void killZKNode(ServerName server) throws IOException {
    getLogger().info("Killing zookeeper node " + server);
    cluster.killZkNode(server);
    cluster.waitForZkNodeToStop(server, killZkNodeTimeout);
    getLogger().info(String.format("Killed zookeeper node %s. Reported num of rs: %s", server,
        cluster.getClusterStatus().getLiveServersLoad().size()));
  }

  protected void startZKNode(ServerName server) throws IOException {
    getLogger().info("Starting zookeeper node " + server.getHostname());
    cluster.startZkNode(server.getHostname(), server.getPort());
    cluster.waitForZkNodeToStart(server, startZkNodeTimeout);
    getLogger().info("Started zookeeper node " + server);
  }

  protected void killDataNode(ServerName server) throws IOException {
    getLogger().info("Killing datanode " + server);
    cluster.killDataNode(server);
    cluster.waitForDataNodeToStop(server, killDataNodeTimeout);
    getLogger().info(String.format("Killed datanode %s. Reported num of rs: %s", server,
        cluster.getClusterStatus().getLiveServersLoad().size()));
  }

  protected void startDataNode(ServerName server) throws IOException {
    getLogger().info("Starting datanode " + server.getHostname());
    cluster.startDataNode(server);
    cluster.waitForDataNodeToStart(server, startDataNodeTimeout);
    getLogger().info("Started datanode " + server);
  }

  protected void killNameNode(ServerName server) throws IOException {
    getLogger().info("Killing namenode : " + server.getHostname());
    cluster.killNameNode(server);
    cluster.waitForNameNodeToStop(server, killNameNodeTimeout);
    getLogger().info("Killed namenode: " + server + ". Reported num of rs:"
        + cluster.getClusterStatus().getServersSize());
  }

  protected void startNameNode(ServerName server) throws IOException {
    getLogger().info("Starting Namenode : " + server.getHostname());
    cluster.startNameNode(server);
    cluster.waitForNameNodeToStart(server, startNameNodeTimeout);
    getLogger().info("Started namenode: " +  server);
  }

  protected void unbalanceRegions(ClusterStatus clusterStatus,
      List<ServerName> fromServers, List<ServerName> toServers,
      double fractionOfRegions) throws Exception {
    List<byte[]> victimRegions = new LinkedList<byte[]>();
    for (ServerName server : fromServers) {
      ServerLoad serverLoad = clusterStatus.getLoad(server);
      // Ugh.
      List<byte[]> regions = new LinkedList<byte[]>(serverLoad.getRegionsLoad().keySet());
      int victimRegionCount = (int)Math.ceil(fractionOfRegions * regions.size());
      getLogger().debug("Removing " + victimRegionCount + " regions from "
        + server.getServerName());
      for (int i = 0; i < victimRegionCount; ++i) {
        int victimIx = RandomUtils.nextInt(regions.size());
        String regionId = HRegionInfo.encodeRegionName(regions.remove(victimIx));
        victimRegions.add(Bytes.toBytes(regionId));
      }
    }

    getLogger().info("Moving " + victimRegions.size() + " regions from " + fromServers.size()
        + " servers to " + toServers.size() + " different servers");
    Admin admin = this.context.getHBaseIntegrationTestingUtility().getHBaseAdmin();
    for (byte[] victimRegion : victimRegions) {
      // Don't keep moving regions if we're
      // trying to stop the monkey.
      if (context.isStopping()) {
        break;
      }
      int targetIx = RandomUtils.nextInt(toServers.size());
      admin.move(victimRegion, Bytes.toBytes(toServers.get(targetIx).getServerName()));
    }
  }

  protected void forceBalancer() throws Exception {
    Admin admin = this.context.getHBaseIntegrationTestingUtility().getHBaseAdmin();
    boolean result = false;
    try {
      result = admin.balancer();
    } catch (Exception e) {
      getLogger().warn("Got exception while doing balance ", e);
    }
    if (!result) {
      getLogger().error("Balancer didn't succeed");
    }
  }

  protected void setBalancer(boolean onOrOff, boolean synchronous) throws Exception {
    Admin admin = this.context.getHBaseIntegrationTestingUtility().getHBaseAdmin();
    try {
      admin.setBalancerRunning(onOrOff, synchronous);
    } catch (Exception e) {
      getLogger().warn("Got exception while switching balance ", e);
    }
  }

  public Configuration getConf() {
    return cluster.getConf();
  }

  /**
   * Context for Action's
   */
  public static class ActionContext {
    private IntegrationTestingUtility util;

    public ActionContext(IntegrationTestingUtility util) {
      this.util = util;
    }

    public IntegrationTestingUtility getHBaseIntegrationTestingUtility() {
      return util;
    }

    public HBaseCluster getHBaseCluster() {
      return util.getHBaseClusterInterface();
    }

    public boolean isStopping() {
      return false;
    }
  }
}
