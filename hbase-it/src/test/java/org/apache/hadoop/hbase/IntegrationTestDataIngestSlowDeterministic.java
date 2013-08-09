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
import org.apache.hadoop.hbase.util.ChaosMonkey.Action;
import org.apache.hadoop.hbase.util.ChaosMonkey.BatchRestartRs;
import org.apache.hadoop.hbase.util.ChaosMonkey.CompactRandomRegionOfTable;
import org.apache.hadoop.hbase.util.ChaosMonkey.CompactTable;
import org.apache.hadoop.hbase.util.ChaosMonkey.CompositeSequentialPolicy;
import org.apache.hadoop.hbase.util.ChaosMonkey.DoActionsOncePolicy;
import org.apache.hadoop.hbase.util.ChaosMonkey.FlushRandomRegionOfTable;
import org.apache.hadoop.hbase.util.ChaosMonkey.FlushTable;
import org.apache.hadoop.hbase.util.ChaosMonkey.MergeRandomAdjacentRegionsOfTable;
import org.apache.hadoop.hbase.util.ChaosMonkey.MoveRandomRegionOfTable;
import org.apache.hadoop.hbase.util.ChaosMonkey.MoveRegionsOfTable;
import org.apache.hadoop.hbase.util.ChaosMonkey.PeriodicRandomActionPolicy;
import org.apache.hadoop.hbase.util.ChaosMonkey.RestartActiveMaster;
import org.apache.hadoop.hbase.util.ChaosMonkey.RestartRandomRs;
import org.apache.hadoop.hbase.util.ChaosMonkey.RestartRsHoldingMeta;
import org.apache.hadoop.hbase.util.ChaosMonkey.RollingBatchRestartRs;
import org.apache.hadoop.hbase.util.ChaosMonkey.SnapshotTable;
import org.apache.hadoop.hbase.util.ChaosMonkey.SplitRandomRegionOfTable;
import org.apache.hadoop.hbase.util.LoadTestTool;
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
  private static final int SERVER_COUNT = 4; // number of slaves for the smallest cluster
  private static final long DEFAULT_RUN_TIME = 20 * 60 * 1000;

  private ChaosMonkey monkey;

  @Before
  public void setUp() throws Exception {
    super.setUp(SERVER_COUNT);

    // Actions such as compact/flush a table/region,
    // move one region around. They are not so destructive,
    // can be executed more frequently.
    Action[] actions1 = new Action[] {
      new CompactTable(tableName, 0.5f),
      new CompactRandomRegionOfTable(tableName, 0.6f),
      new FlushTable(tableName),
      new FlushRandomRegionOfTable(tableName),
      new MoveRandomRegionOfTable(tableName)
    };

    // Actions such as split/merge/snapshot.
    // They should not cause data loss, or unreliability
    // such as region stuck in transition.
    Action[] actions2 = new Action[] {
      new SplitRandomRegionOfTable(tableName),
      new MergeRandomAdjacentRegionsOfTable(tableName),
      new SnapshotTable(tableName),
      new ChaosMonkey.AddColumnAction(tableName),
      new ChaosMonkey.RemoveColumnAction(tableName),
      new ChaosMonkey.ChangeEncodingAction(tableName),
      new ChaosMonkey.ChangeVersionsAction(tableName)
    };

    // Destructive actions to mess things around.
    Action[] actions3 = new Action[] {
      new MoveRegionsOfTable(tableName),
      new RestartRandomRs(60000),
      new BatchRestartRs(5000, 0.5f),
      new RestartActiveMaster(5000),
      new RollingBatchRestartRs(5000, 1.0f),
      new RestartRsHoldingMeta(35000)
    };

    monkey = new ChaosMonkey(util,
      new PeriodicRandomActionPolicy(60 * 1000, actions1),
      new PeriodicRandomActionPolicy(90 * 1000, actions2),
      new CompositeSequentialPolicy(
        new DoActionsOncePolicy(150 * 1000, actions3),
        new PeriodicRandomActionPolicy(150 * 1000, actions3)));
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
    runIngestTest(DEFAULT_RUN_TIME, 2500, 10, 1024, 10);
  }
}
