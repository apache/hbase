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
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.chaos.factories.MonkeyConstants;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
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
  protected ClusterMetrics initialStatus;
  protected ServerName[] initialServers;
  protected Properties monkeyProps;

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
  protected boolean skipMetaRS;

  /**
   * Retrieve the instance's {@link Logger}, for use throughout the class hierarchy.
   */
  protected abstract Logger getLogger();

  public void init(ActionContext context) throws IOException {
    this.context = context;
    cluster = context.getHBaseCluster();
    initialStatus = cluster.getInitialClusterMetrics();
    Collection<ServerName> regionServers = initialStatus.getLiveServerMetrics().keySet();
    initialServers = regionServers.toArray(new ServerName[0]);

    monkeyProps = context.getMonkeyProps();
    if (monkeyProps == null){
      monkeyProps = new Properties();
      IntegrationTestBase.loadMonkeyProperties(monkeyProps, cluster.getConf());
    }

    killMasterTimeout = Long.parseLong(monkeyProps.getProperty(
      KILL_MASTER_TIMEOUT_KEY, KILL_MASTER_TIMEOUT_DEFAULT + ""));
    startMasterTimeout = Long.parseLong(monkeyProps.getProperty(START_MASTER_TIMEOUT_KEY,
      START_MASTER_TIMEOUT_DEFAULT + ""));
    killRsTimeout = Long.parseLong(monkeyProps.getProperty(KILL_RS_TIMEOUT_KEY,
      KILL_RS_TIMEOUT_DEFAULT + ""));
    startRsTimeout = Long.parseLong(monkeyProps.getProperty(START_RS_TIMEOUT_KEY,
      START_RS_TIMEOUT_DEFAULT+ ""));
    killZkNodeTimeout = Long.parseLong(monkeyProps.getProperty(KILL_ZK_NODE_TIMEOUT_KEY,
      KILL_ZK_NODE_TIMEOUT_DEFAULT + ""));
    startZkNodeTimeout = Long.parseLong(monkeyProps.getProperty(START_ZK_NODE_TIMEOUT_KEY,
      START_ZK_NODE_TIMEOUT_DEFAULT + ""));
    killDataNodeTimeout = Long.parseLong(monkeyProps.getProperty(KILL_DATANODE_TIMEOUT_KEY,
      KILL_DATANODE_TIMEOUT_DEFAULT + ""));
    startDataNodeTimeout = Long.parseLong(monkeyProps.getProperty(START_DATANODE_TIMEOUT_KEY,
      START_DATANODE_TIMEOUT_DEFAULT + ""));
    killNameNodeTimeout = Long.parseLong(monkeyProps.getProperty(KILL_NAMENODE_TIMEOUT_KEY,
      KILL_NAMENODE_TIMEOUT_DEFAULT + ""));
    startNameNodeTimeout = Long.parseLong(monkeyProps.getProperty(START_NAMENODE_TIMEOUT_KEY,
      START_NAMENODE_TIMEOUT_DEFAULT + ""));
    skipMetaRS = Boolean.parseBoolean(monkeyProps.getProperty(MonkeyConstants.SKIP_META_RS,
      MonkeyConstants.DEFAULT_SKIP_META_RS + ""));
  }

  public void perform() throws Exception { }

  /** Returns current region servers - active master */
  protected ServerName[] getCurrentServers() throws IOException {
    ClusterMetrics clusterStatus = cluster.getClusterMetrics();
    Collection<ServerName> regionServers = clusterStatus.getLiveServerMetrics().keySet();
    int count = regionServers.size();
    if (count <= 0) {
      return new ServerName [] {};
    }
    ServerName master = clusterStatus.getMasterName();
    Set<ServerName> masters = new HashSet<>();
    masters.add(master);
    masters.addAll(clusterStatus.getBackupMasterNames());
    ArrayList<ServerName> tmp = new ArrayList<>(count);
    tmp.addAll(regionServers);
    tmp.removeAll(masters);

    if(skipMetaRS){
      ServerName metaServer = cluster.getServerHoldingMeta();
      tmp.remove(metaServer);
    }

    return tmp.toArray(new ServerName[0]);
  }

  protected void killMaster(ServerName server) throws IOException {
    getLogger().info("Killing master {}", server);
    cluster.killMaster(server);
    cluster.waitForMasterToStop(server, killMasterTimeout);
    getLogger().info("Killed master " + server);
  }

  protected void startMaster(ServerName server) throws IOException {
    getLogger().info("Starting master {}", server.getHostname());
    cluster.startMaster(server.getHostname(), server.getPort());
    cluster.waitForActiveAndReadyMaster(startMasterTimeout);
    getLogger().info("Started master " + server.getHostname());
  }

  protected void stopRs(ServerName server) throws IOException {
    getLogger().info("Stopping regionserver {}", server);
    cluster.stopRegionServer(server);
    cluster.waitForRegionServerToStop(server, killRsTimeout);
    getLogger().info("Stopping regionserver {}. Reported num of rs:{}", server,
      cluster.getClusterMetrics().getLiveServerMetrics().size());
  }

  protected void suspendRs(ServerName server) throws IOException {
    getLogger().info("Suspending regionserver {}", server);
    cluster.suspendRegionServer(server);
    if(!(cluster instanceof MiniHBaseCluster)){
      cluster.waitForRegionServerToStop(server, killRsTimeout);
    }
    getLogger().info("Suspending regionserver {}. Reported num of rs:{}", server,
      cluster.getClusterMetrics().getLiveServerMetrics().size());
  }

  protected void resumeRs(ServerName server) throws IOException {
    getLogger().info("Resuming regionserver {}", server);
    cluster.resumeRegionServer(server);
    if(!(cluster instanceof MiniHBaseCluster)){
      cluster.waitForRegionServerToStart(server.getHostname(), server.getPort(), startRsTimeout);
    }
    getLogger().info("Resuming regionserver {}. Reported num of rs:{}", server,
      cluster.getClusterMetrics().getLiveServerMetrics().size());
  }

  protected void killRs(ServerName server) throws IOException {
    getLogger().info("Killing regionserver {}", server);
    cluster.killRegionServer(server);
    cluster.waitForRegionServerToStop(server, killRsTimeout);
    getLogger().info("Killed regionserver {}. Reported num of rs:{}", server,
      cluster.getClusterMetrics().getLiveServerMetrics().size());
  }

  protected void startRs(ServerName server) throws IOException {
    getLogger().info("Starting regionserver {}", server.getAddress());
    cluster.startRegionServer(server.getHostname(), server.getPort());
    cluster.waitForRegionServerToStart(server.getHostname(), server.getPort(), startRsTimeout);
    getLogger().info("Started regionserver {}. Reported num of rs:{}", server.getAddress(),
      cluster.getClusterMetrics().getLiveServerMetrics().size());
  }

  protected void killZKNode(ServerName server) throws IOException {
    getLogger().info("Killing zookeeper node {}", server);
    cluster.killZkNode(server);
    cluster.waitForZkNodeToStop(server, killZkNodeTimeout);
    getLogger().info("Killed zookeeper node {}. Reported num of rs:{}", server,
      cluster.getClusterMetrics().getLiveServerMetrics().size());
  }

  protected void startZKNode(ServerName server) throws IOException {
    getLogger().info("Starting zookeeper node {}", server.getHostname());
    cluster.startZkNode(server.getHostname(), server.getPort());
    cluster.waitForZkNodeToStart(server, startZkNodeTimeout);
    getLogger().info("Started zookeeper node {}", server);
  }

  protected void killDataNode(ServerName server) throws IOException {
    getLogger().info("Killing datanode {}", server);
    cluster.killDataNode(server);
    cluster.waitForDataNodeToStop(server, killDataNodeTimeout);
    getLogger().info("Killed datanode {}. Reported num of rs:{}", server,
      cluster.getClusterMetrics().getLiveServerMetrics().size());
  }

  protected void startDataNode(ServerName server) throws IOException {
    getLogger().info("Starting datanode {}", server.getHostname());
    cluster.startDataNode(server);
    cluster.waitForDataNodeToStart(server, startDataNodeTimeout);
    getLogger().info("Started datanode {}", server);
  }

  protected void killNameNode(ServerName server) throws IOException {
    getLogger().info("Killing namenode :-{}", server.getHostname());
    cluster.killNameNode(server);
    cluster.waitForNameNodeToStop(server, killNameNodeTimeout);
    getLogger().info("Killed namenode:{}. Reported num of rs:{}", server,
        cluster.getClusterMetrics().getLiveServerMetrics().size());
  }

  protected void startNameNode(ServerName server) throws IOException {
    getLogger().info("Starting Namenode :-{}", server.getHostname());
    cluster.startNameNode(server);
    cluster.waitForNameNodeToStart(server, startNameNodeTimeout);
    getLogger().info("Started namenode:{}", server);
  }
  protected void unbalanceRegions(ClusterMetrics clusterStatus,
      List<ServerName> fromServers, List<ServerName> toServers,
      double fractionOfRegions) throws Exception {
    List<byte[]> victimRegions = new LinkedList<>();
    for (Map.Entry<ServerName, ServerMetrics> entry
      : clusterStatus.getLiveServerMetrics().entrySet()) {
      ServerName sn = entry.getKey();
      ServerMetrics serverLoad = entry.getValue();
      // Ugh.
      List<byte[]> regions = new LinkedList<>(serverLoad.getRegionMetrics().keySet());
      int victimRegionCount = (int)Math.ceil(fractionOfRegions * regions.size());
      getLogger().debug("Removing {} regions from {}", victimRegionCount, sn);
      Random rand = ThreadLocalRandom.current();
      for (int i = 0; i < victimRegionCount; ++i) {
        int victimIx = rand.nextInt(regions.size());
        String regionId = RegionInfo.encodeRegionName(regions.remove(victimIx));
        victimRegions.add(Bytes.toBytes(regionId));
      }
    }

    getLogger().info("Moving {} regions from {} servers to {} different servers",
      victimRegions.size(), fromServers.size(), toServers.size());
    Admin admin = this.context.getHBaseIntegrationTestingUtility().getAdmin();
    Random rand = ThreadLocalRandom.current();
    for (byte[] victimRegion : victimRegions) {
      // Don't keep moving regions if we're
      // trying to stop the monkey.
      if (context.isStopping()) {
        break;
      }
      int targetIx = rand.nextInt(toServers.size());
      admin.move(victimRegion, toServers.get(targetIx));
    }
  }

  protected void forceBalancer() throws Exception {
    Admin admin = this.context.getHBaseIntegrationTestingUtility().getAdmin();
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
    Admin admin = this.context.getHBaseIntegrationTestingUtility().getAdmin();
    try {
      admin.balancerSwitch(onOrOff, synchronous);
    } catch (Exception e) {
      getLogger().warn("Got exception while switching balance ", e);
    }
  }

  public Configuration getConf() {
    return cluster.getConf();
  }

  /**
   * Apply a transform to all columns in a given table. If there are no columns in a table
   * or if the context is stopping does nothing.
   * @param tableName the table to modify
   * @param transform the modification to perform. Callers will have the
   *                  column name as a string and a column family builder available to them
   */
  protected void modifyAllTableColumns(TableName tableName,
    BiConsumer<String, ColumnFamilyDescriptorBuilder> transform) throws IOException {
    HBaseTestingUtility util = this.context.getHBaseIntegrationTestingUtility();
    Admin admin = util.getAdmin();

    TableDescriptor tableDescriptor = admin.getDescriptor(tableName);
    ColumnFamilyDescriptor[] columnDescriptors = tableDescriptor.getColumnFamilies();

    if (columnDescriptors == null || columnDescriptors.length == 0) {
      return;
    }

    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableDescriptor);
    for (ColumnFamilyDescriptor descriptor : columnDescriptors) {
      ColumnFamilyDescriptorBuilder cfd = ColumnFamilyDescriptorBuilder.newBuilder(descriptor);
      transform.accept(descriptor.getNameAsString(), cfd);
      builder.modifyColumnFamily(cfd.build());
    }

    // Don't try the modify if we're stopping
    if (this.context.isStopping()) {
      return;
    }
    admin.modifyTable(builder.build());
  }

  /**
   * Apply a transform to all columns in a given table.
   * If there are no columns in a table or if the context is stopping does nothing.
   * @param tableName the table to modify
   * @param transform the modification to perform on each column family descriptor builder
   */
  protected void modifyAllTableColumns(TableName tableName,
    Consumer<ColumnFamilyDescriptorBuilder> transform) throws IOException {
    modifyAllTableColumns(tableName, (name, cfd) -> transform.accept(cfd));
  }

  /**
   * Context for Action's
   */
  public static class ActionContext {
    private IntegrationTestingUtility util;
    private Properties monkeyProps = null;

    public ActionContext(IntegrationTestingUtility util) {
      this.util = util;
    }

    public ActionContext(Properties monkeyProps, IntegrationTestingUtility util) {
      this.util = util;
      this.monkeyProps = monkeyProps;
    }

    public Properties getMonkeyProps(){
      return monkeyProps;
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
