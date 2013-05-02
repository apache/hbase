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

import org.apache.hadoop.hbase.util.ChaosMonkey;
import org.apache.hadoop.hbase.util.ChaosMonkey.BatchRestartRs;
import org.apache.hadoop.hbase.util.ChaosMonkey.RestartActiveMaster;
import org.apache.hadoop.hbase.util.ChaosMonkey.RestartRandomRs;
import org.apache.hadoop.hbase.util.ChaosMonkey.RestartRsHoldingMeta;
import org.apache.hadoop.hbase.util.ChaosMonkey.RestartRsHoldingRoot;
import org.apache.hadoop.hbase.util.ChaosMonkey.RollingBatchRestartRs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A system test which does large data ingestion and verify using {@link LoadTestTool}.
 * It performs a set of actions deterministically using ChaosMonkey, then starts killing
 * things randomly. You can configure how long should the load test run by using 
 * "hbase.IntegrationTestDataIngestSlowDeterministic.runtime" configuration parameter.
 */
@Category(IntegrationTests.class)
public class IntegrationTestDataIngestSlowDeterministic extends IngestIntegrationTestBase {
  private static final int SERVER_COUNT = 3; // number of slaves for the smallest cluster
  private static final long DEFAULT_RUN_TIME = 30 * 60 * 1000;
  private static final long CHAOS_EVERY_MS = 150 * 1000; // Chaos every 2.5 minutes.

  private ChaosMonkey monkey;

  @Before
  public void setUp() throws Exception {
    super.setUp(SERVER_COUNT);
    ChaosMonkey.Action[] actions = new ChaosMonkey.Action[] {
        new RestartRandomRs(60000),
        new BatchRestartRs(5000, 0.5f),
        new RestartActiveMaster(5000),
        new RollingBatchRestartRs(5000, 1.0f),
        new RestartRsHoldingMeta(35000),
        new RestartRsHoldingRoot(35000)
    };
    monkey = new ChaosMonkey(util, new ChaosMonkey.CompositeSequentialPolicy(
            new ChaosMonkey.DoActionsOncePolicy(CHAOS_EVERY_MS, actions),
            new ChaosMonkey.PeriodicRandomActionPolicy(CHAOS_EVERY_MS, actions)));
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
    runIngestTest(DEFAULT_RUN_TIME, 2500, 10, 100, 5);
  }
}
