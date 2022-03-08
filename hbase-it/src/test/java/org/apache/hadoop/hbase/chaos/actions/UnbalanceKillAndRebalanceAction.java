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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This action is too specific to put in ChaosMonkey; put it here */
public class UnbalanceKillAndRebalanceAction extends Action {
  private static final Logger LOG =
      LoggerFactory.getLogger(UnbalanceKillAndRebalanceAction.class);
  /** Fractions of servers to get regions and live and die respectively; from all other
   * servers, HOARD_FRC_OF_REGIONS will be removed to the above randomly */
  private static final double FRC_SERVERS_THAT_HOARD_AND_LIVE = 0.1;
  private static final double FRC_SERVERS_THAT_HOARD_AND_DIE = 0.1;
  private static final double HOARD_FRC_OF_REGIONS = 0.8;
  /** Waits between calling unbalance and killing servers, kills and rebalance, and rebalance
   * and restarting the servers; to make sure these events have time to impact the cluster. */
  private final long waitForUnbalanceMilliSec;
  private final long waitForKillsMilliSec;
  private final long waitAfterBalanceMilliSec;
  private final boolean killMetaRs;

  public UnbalanceKillAndRebalanceAction(long waitUnbalance, long waitKill, long waitAfterBalance,
      boolean killMetaRs) {
    super();
    waitForUnbalanceMilliSec = waitUnbalance;
    waitForKillsMilliSec = waitKill;
    waitAfterBalanceMilliSec = waitAfterBalance;
    this.killMetaRs = killMetaRs;
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    ClusterMetrics status = this.cluster.getClusterMetrics();
    List<ServerName> victimServers = new LinkedList<>(status.getLiveServerMetrics().keySet());
    Set<ServerName> killedServers = new HashSet<>();
    int liveCount = (int)Math.ceil(FRC_SERVERS_THAT_HOARD_AND_LIVE * victimServers.size());
    int deadCount = (int)Math.ceil(FRC_SERVERS_THAT_HOARD_AND_DIE * victimServers.size());
    Assert.assertTrue(
        "There are not enough victim servers: " + victimServers.size(),
        liveCount + deadCount < victimServers.size());
    Random rand = ThreadLocalRandom.current();
    List<ServerName> targetServers = new ArrayList<>(liveCount);
    for (int i = 0; i < liveCount + deadCount; ++i) {
      int victimIx = rand.nextInt(victimServers.size());
      targetServers.add(victimServers.remove(victimIx));
    }
    unbalanceRegions(status, victimServers, targetServers, HOARD_FRC_OF_REGIONS);
    Thread.sleep(waitForUnbalanceMilliSec);
    ServerName metaServer = cluster.getServerHoldingMeta();
    for (ServerName targetServer: targetServers) {
      // Don't keep killing servers if we're
      // trying to stop the monkey.
      if (context.isStopping()) {
        break;
      }
      if (killedServers.size() >= liveCount) {
        break;
      }

      if (!killMetaRs && targetServer.equals(metaServer)) {
        getLogger().info("Not killing server because it holds hbase:meta.");
      } else {
        killRs(targetServer);
        killedServers.add(targetServer);
      }
    }

    Thread.sleep(waitForKillsMilliSec);
    forceBalancer();
    Thread.sleep(waitAfterBalanceMilliSec);
    for (ServerName server:killedServers) {
      startRs(server);
    }
  }
}
