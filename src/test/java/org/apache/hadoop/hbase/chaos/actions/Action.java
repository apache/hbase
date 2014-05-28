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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseDistributedCluster;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * A (possibly mischievous) action that the ChaosMonkey can perform.
 */
public class Action {

  protected static Log LOG = LogFactory.getLog(Action.class);

  protected ActionContext context;
  protected HBaseDistributedCluster cluster;
  protected ClusterStatus initialStatus;
  protected HServerAddress[] initialServers;

  public void init(ActionContext context) throws IOException {
    this.context = context;
    cluster = context.getHBaseCluster();
    initialStatus = cluster.getInitialClusterStatus();
    Collection<HServerAddress> regionServers = getServerAddresses(initialStatus);
    initialServers = regionServers.toArray(new HServerAddress[regionServers.size()]);
  }

  protected Collection<HServerAddress> getServerAddresses(ClusterStatus status) {
    Collection<HServerAddress> regionServers = new ArrayList<HServerAddress>(status.getServers());
    for (HServerInfo info : status.getServerInfo()) {
      regionServers.add(info.getServerAddress());
    }
    return regionServers;
  }

  public void perform() throws Exception {
  }

  /**
   * Returns current region servers
   */
  protected HServerAddress[] getCurrentServers() throws IOException {
    Collection<HServerAddress> regionServers = getServerAddresses(cluster.getClusterStatus());
    if (regionServers == null || regionServers.size() <= 0) return new HServerAddress[] { };
    return regionServers.toArray(new HServerAddress[regionServers.size()]);
  }

  protected void killMaster(HServerAddress server) throws IOException {
    LOG.info("Killing master:" + server);
    cluster.killMaster(server);
    cluster.waitForMasterToStop(server, PolicyBasedChaosMonkey.TIMEOUT);
    LOG.info("Killed master server:" + server);
  }

  protected void startMaster(HServerAddress server) throws IOException {
    LOG.info("Starting master:" + server.getHostname());
    cluster.startMaster(server.getHostname());
    cluster.waitForActiveAndReadyMaster(PolicyBasedChaosMonkey.TIMEOUT);
    LOG.info("Started master: " + server);
  }

  protected void killRs(HServerAddress server) throws IOException {
    LOG.info("Killing region server:" + server);
    cluster.killRegionServer(server);
    cluster.waitForRegionServerToStop(server, PolicyBasedChaosMonkey.TIMEOUT);
    LOG.info("Killed region server:" + server + ". Reported num of rs:"
        + cluster.getClusterStatus().getServers());
  }

  protected void startRs(HServerAddress server) throws IOException {
    LOG.info("Starting region server:" + server.getHostname());
    cluster.startRegionServer(server.getHostname());
    cluster.waitForRegionServerToStart(server.getHostname(), PolicyBasedChaosMonkey.TIMEOUT);
    LOG.info("Started region server:" + server + ". Reported num of rs:"
        + cluster.getClusterStatus().getServers());
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

    public HBaseDistributedCluster getHBaseCluster() {
      return util.getDistributedCluster();
    }
  }
}
