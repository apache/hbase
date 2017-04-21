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

package org.apache.hadoop.hbase.chaos.actions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A (possibly mischievous) action that the ChaosMonkey can perform.
 */
public class Action {

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

  protected static final Log LOG = LogFactory.getLog(Action.class);

  protected static final long KILL_MASTER_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long START_MASTER_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long KILL_RS_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long START_RS_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long KILL_ZK_NODE_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long START_ZK_NODE_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long KILL_DATANODE_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;
  protected static final long START_DATANODE_TIMEOUT_DEFAULT = PolicyBasedChaosMonkey.TIMEOUT;

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
  }

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
    if (master == null || !regionServers.contains(master)) {
      return regionServers.toArray(new ServerName[count]);
    }
    if (count == 1) {
      return new ServerName [] {};
    }
    ArrayList<ServerName> tmp = new ArrayList<>(count);
    tmp.addAll(regionServers);
    tmp.remove(master);
    return tmp.toArray(new ServerName[count-1]);
  }

  protected void killMaster(ServerName server) throws IOException {
    LOG.info("Killing master:" + server);
    cluster.killMaster(server);
    cluster.waitForMasterToStop(server, killMasterTimeout);
    LOG.info("Killed master server:" + server);
  }

  protected void startMaster(ServerName server) throws IOException {
    LOG.info("Starting master:" + server.getHostname());
    cluster.startMaster(server.getHostname(), server.getPort());
    cluster.waitForActiveAndReadyMaster(startMasterTimeout);
    LOG.info("Started master: " + server);
  }

  protected void killRs(ServerName server) throws IOException {
    LOG.info("Killing region server:" + server);
    cluster.killRegionServer(server);
    cluster.waitForRegionServerToStop(server, killRsTimeout);
    LOG.info("Killed region server:" + server + ". Reported num of rs:"
        + cluster.getClusterStatus().getServersSize());
  }

  protected void startRs(ServerName server) throws IOException {
    LOG.info("Starting region server:" + server.getHostname());
    cluster.startRegionServer(server.getHostname(), server.getPort());
    cluster.waitForRegionServerToStart(server.getHostname(), server.getPort(), startRsTimeout);
    LOG.info("Started region server:" + server + ". Reported num of rs:"
      + cluster.getClusterStatus().getServersSize());
  }

  protected void killZKNode(ServerName server) throws IOException {
    LOG.info("Killing zookeeper node:" + server);
    cluster.killZkNode(server);
    cluster.waitForZkNodeToStop(server, killZkNodeTimeout);
    LOG.info("Killed zookeeper node:" + server + ". Reported num of rs:"
      + cluster.getClusterStatus().getServersSize());
  }

  protected void startZKNode(ServerName server) throws IOException {
    LOG.info("Starting zookeeper node:" + server.getHostname());
    cluster.startZkNode(server.getHostname(), server.getPort());
    cluster.waitForZkNodeToStart(server, startZkNodeTimeout);
    LOG.info("Started zookeeper node:" + server);
  }

  protected void killDataNode(ServerName server) throws IOException {
    LOG.info("Killing datanode:" + server);
    cluster.killDataNode(server);
    cluster.waitForDataNodeToStop(server, killDataNodeTimeout);
    LOG.info("Killed datanode:" + server + ". Reported num of rs:"
      + cluster.getClusterStatus().getServersSize());
  }

  protected void startDataNode(ServerName server) throws IOException {
    LOG.info("Starting datanode:" + server.getHostname());
    cluster.startDataNode(server);
    cluster.waitForDataNodeToStart(server, startDataNodeTimeout);
    LOG.info("Started datanode:" + server);
  }

  protected void unbalanceRegions(ClusterStatus clusterStatus,
      List<ServerName> fromServers, List<ServerName> toServers,
      double fractionOfRegions) throws Exception {
    List<byte[]> victimRegions = new LinkedList<>();
    for (ServerName server : fromServers) {
      ServerLoad serverLoad = clusterStatus.getLoad(server);
      // Ugh.
      List<byte[]> regions = new LinkedList<>(serverLoad.getRegionsLoad().keySet());
      int victimRegionCount = (int)Math.ceil(fractionOfRegions * regions.size());
      LOG.debug("Removing " + victimRegionCount + " regions from " + server.getServerName());
      for (int i = 0; i < victimRegionCount; ++i) {
        int victimIx = RandomUtils.nextInt(regions.size());
        String regionId = HRegionInfo.encodeRegionName(regions.remove(victimIx));
        victimRegions.add(Bytes.toBytes(regionId));
      }
    }

    LOG.info("Moving " + victimRegions.size() + " regions from " + fromServers.size()
        + " servers to " + toServers.size() + " different servers");
    Admin admin = this.context.getHBaseIntegrationTestingUtility().getAdmin();
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
    Admin admin = this.context.getHBaseIntegrationTestingUtility().getAdmin();
    boolean result = false;
    try {
      result = admin.balancer();
    } catch (Exception e) {
      LOG.warn("Got exception while doing balance ", e);
    }
    if (!result) {
      LOG.error("Balancer didn't succeed");
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
