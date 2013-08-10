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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.ChaosMonkey;
import org.apache.hadoop.hbase.util.ChaosMonkey.*;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A system test which does large data ingestion and verify using {@link LoadTestTool},
 * while killing the region servers and the master(s) randomly. You can configure how long
 * should the load test run by using "hbase.IntegrationTestDataIngestWithChaosMonkey.runtime"
 * configuration parameter.
 */
@Category(IntegrationTests.class)
public class IntegrationTestDataIngestWithChaosMonkey extends IngestIntegrationTestBase {

  private static int NUM_SLAVES_BASE = 4; //number of slaves for the smallest cluster

  // run for 10 min by default
  private static final long DEFAULT_RUN_TIME = 10 * 60 * 1000;

  private ChaosMonkey monkey;

  @Before
  public void setUp() throws Exception {
    util= getTestingUtil(null);
    Configuration conf = util.getConfiguration();
    if (conf.getBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY,
      HConstants.DEFAULT_DISTRIBUTED_LOG_REPLAY_CONFIG)) {
      // when distributedLogReplay is enabled, we need to make sure rpc timeout & retires are
      // smaller enough in order for the replay can complete before ChaosMonkey kills another region
      // server
      conf.setInt("hbase.log.replay.retries.number", 2);
      conf.setInt("hbase.log.replay.rpc.timeout", 2000);
      conf.setBoolean(HConstants.DISALLOW_WRITES_IN_RECOVERING, true);
    } 
    if(!util.isDistributedCluster()) {
      NUM_SLAVES_BASE = 5;
    }
    super.setUp(NUM_SLAVES_BASE);

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
      new MoveRegionsOfTable(tableName),
      new AddColumnAction(tableName),
      new RemoveColumnAction(tableName),
      new ChangeEncodingAction(tableName),
      new ChangeVersionsAction(tableName)
    };

    monkey = new ChaosMonkey(util,
      new PeriodicRandomActionPolicy(30 * 1000, actions1),
      new PeriodicRandomActionPolicy(60 * 1000, actions2),
      ChaosMonkey.getPolicyByName(ChaosMonkey.EVERY_MINUTE_RANDOM_ACTION_POLICY));
    monkey.start();
  }

  @After
  public void tearDown() throws Exception {
    if (monkey != null) {
      monkey.stop("test has finished, that's why");
      monkey.waitForStop();
    }
    super.tearDown();
  }

  @Test
  public void testDataIngest() throws Exception {
    runIngestTest(DEFAULT_RUN_TIME, 2500, 10, 100, 20);
  }
}
