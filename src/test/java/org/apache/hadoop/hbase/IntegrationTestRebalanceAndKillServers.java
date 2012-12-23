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

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChaosMonkey;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ChaosMonkey.Action;
import org.apache.hadoop.hbase.util.ChaosMonkey.RestartActiveMaster;
import org.apache.hadoop.hbase.util.ChaosMonkey.RestartRandomRs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import com.google.common.collect.Lists;

/**
 * A system test which does large data ingestion and verify using {@link LoadTestTool},
 * while killing the region servers and the master(s) randomly. You can configure how long
 * should the load test run by using "hbase.IntegrationTestRebalanceAndKillServers  s.runtime"
 * configuration parameter.
 */
@Category(IntegrationTests.class)
public class IntegrationTestRebalanceAndKillServers extends IngestIntegrationTestBase {
  private static final int NUM_SLAVES_BASE = 4; // number of slaves for the smallest cluster
  private static final long DEFAULT_RUN_TIME = 5 * 60 * 1000; // run for 5 min by default

  private static final long KILL_SERVICE_EVERY_MS = 45 * 1000;
  private static final int SERVER_PER_MASTER_KILL = 3;
  private static final long KILL_SERVER_FOR_MS = 5 * 1000;
  private static final long KILL_MASTER_FOR_MS = 100;

  private static final long UNBALANCE_REGIONS_EVERY_MS = 30 * 1000;
  /** @see ChaosMonkey.UnbalanceRegionsAction#UnbalanceRegionsAction(double, double) */
  private static final double UNBALANCE_TO_FRC_OF_SERVERS = 0.5;
  /** @see ChaosMonkey.UnbalanceRegionsAction#UnbalanceRegionsAction(double, double) */
  private static final double UNBALANCE_FRC_OF_REGIONS = 0.5;

  private static final long BALANCE_REGIONS_EVERY_MS = 10 * 1000;

  private ChaosMonkey monkey;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    super.setUp(NUM_SLAVES_BASE);

    ChaosMonkey.Policy killPolicy = new ChaosMonkey.PeriodicRandomActionPolicy(
      KILL_SERVICE_EVERY_MS,
      new Pair<Action,Integer>(new ChaosMonkey.RestartActiveMaster(KILL_MASTER_FOR_MS), 1),
      new Pair<Action,Integer>(new ChaosMonkey.RestartRandomRs(KILL_SERVER_FOR_MS), SERVER_PER_MASTER_KILL));

    ChaosMonkey.Policy unbalancePolicy = new ChaosMonkey.PeriodicRandomActionPolicy(
      UNBALANCE_REGIONS_EVERY_MS,
      new ChaosMonkey.UnbalanceRegionsAction(UNBALANCE_FRC_OF_REGIONS, UNBALANCE_TO_FRC_OF_SERVERS));

    ChaosMonkey.Policy balancePolicy = new ChaosMonkey.PeriodicRandomActionPolicy(
      BALANCE_REGIONS_EVERY_MS, new ChaosMonkey.ForceBalancerAction());

    monkey = new ChaosMonkey(util, killPolicy, unbalancePolicy, balancePolicy);
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

  @Test
  public void testDataIngest() throws Exception {
    runIngestTest(DEFAULT_RUN_TIME, 2500, 10, 100, 20);
  }
}
