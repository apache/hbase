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
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChaosMonkey;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ChaosMonkey.Action;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

/**
 * A system test which does large data ingestion and verify using {@link LoadTestTool},
 * while introducing chaos by hoarding many regions into few servers (unbalancing), then
 * killing some of these servers, and triggering balancer.
 * It's configured using a set of constants on top, which cover this scenario and are
 * reasonable for minicluster. See constants if you want to tweak the test.
 * You can configure how long the test should run by using
 * "hbase.IntegrationTestRebalanceAndKillServersTargeted.runtime" configuration parameter,
 * which is probably most useful on cluster.
 */
@Category(IntegrationTests.class)
public class IntegrationTestRebalanceAndKillServersTargeted extends IngestIntegrationTestBase {
  private static final int NUM_SLAVES_BASE = 4; // number of slaves for the smallest cluster
  private static final long DEFAULT_RUN_TIME = 5 * 60 * 1000; // run for 5 min by default

  /** How often to introduce the chaos. If too frequent, sequence of kills on minicluster
   * can cause test to fail when Put runs out of retries. */
  private static final long CHAOS_EVERY_MS = 65 * 1000;

  private ChaosMonkey monkey;

  /** This action is too specific to put in ChaosMonkey; put it here */
  static class UnbalanceKillAndRebalanceAction extends ChaosMonkey.Action {
    /** Fractions of servers to get regions and live and die respectively; from all other
     * servers, HOARD_FRC_OF_REGIONS will be removed to the above randomly */
    private static final double FRC_SERVERS_THAT_HOARD_AND_LIVE = 0.1;
    private static final double FRC_SERVERS_THAT_HOARD_AND_DIE = 0.1;
    private static final double HOARD_FRC_OF_REGIONS = 0.8;
    /** Waits between calling unbalance and killing servers, kills and rebalance, and rebalance
     * and restarting the servers; to make sure these events have time to impact the cluster. */
    private static final long WAIT_FOR_UNBALANCE_MS = 2 * 1000;
    private static final long WAIT_FOR_KILLS_MS = 2 * 1000;
    private static final long WAIT_AFTER_BALANCE_MS = 5 * 1000;

    @Override
    protected void perform() throws Exception {
      ClusterStatus status = this.cluster.getClusterStatus();
      List<ServerName> victimServers = new LinkedList<ServerName>(status.getServers());
      int liveCount = (int)Math.ceil(FRC_SERVERS_THAT_HOARD_AND_LIVE * victimServers.size());
      int deadCount = (int)Math.ceil(FRC_SERVERS_THAT_HOARD_AND_DIE * victimServers.size());
      Assert.assertTrue((liveCount + deadCount) < victimServers.size());
      List<ServerName> targetServers = new ArrayList<ServerName>(liveCount);
      for (int i = 0; i < liveCount + deadCount; ++i) {
        int victimIx = random.nextInt(victimServers.size());
        targetServers.add(victimServers.remove(victimIx));
      }
      unbalanceRegions(status, victimServers, targetServers, HOARD_FRC_OF_REGIONS);
      Thread.sleep(WAIT_FOR_UNBALANCE_MS);
      for (int i = 0; i < liveCount; ++i) {
        killRs(targetServers.get(i));
      }
      Thread.sleep(WAIT_FOR_KILLS_MS);
      forceBalancer();
      Thread.sleep(WAIT_AFTER_BALANCE_MS);
      for (int i = 0; i < liveCount; ++i) {
        startRs(targetServers.get(i));
      }
    }
  }

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    super.setUp(NUM_SLAVES_BASE);

    ChaosMonkey.Policy chaosPolicy = new ChaosMonkey.PeriodicRandomActionPolicy(
      CHAOS_EVERY_MS, new UnbalanceKillAndRebalanceAction());
    monkey = new ChaosMonkey(util, chaosPolicy);
    monkey.start();
  }

  @After
  public void tearDown() throws Exception {
    if (monkey != null) {
      monkey.stop("tearDown");
      monkey.waitForStop();
    }
    super.tearDown();
  }

  // Disabled until we fix hbase-7520
  @Ignore @Test
  public void testDataIngest() throws Exception {
    runIngestTest(DEFAULT_RUN_TIME, 2500, 10, 100, 20);
  }
}
