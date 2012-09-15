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
public class IntegrationTestDataIngestWithChaosMonkey {

  private static final String TABLE_NAME = "TestDataIngestWithChaosMonkey";
  private static final int NUM_SLAVES_BASE = 4; //number of slaves for the smallest cluster

  /** A soft limit on how long we should run */
  private static final String RUN_TIME_KEY = "hbase.IntegrationTestDataIngestWithChaosMonkey.runtime";

  //run for 5 min by default
  private static final long DEFAULT_RUN_TIME = 5 * 60 * 1000;

  private static final Log LOG = LogFactory.getLog(IntegrationTestDataIngestWithChaosMonkey.class);
  private IntegrationTestingUtility util;
  private HBaseCluster cluster;
  private ChaosMonkey monkey;

  @Before
  public void setUp() throws Exception {
    util = new IntegrationTestingUtility();

    util.initializeCluster(NUM_SLAVES_BASE);

    cluster = util.getHBaseClusterInterface();
    deleteTableIfNecessary();

    monkey = new ChaosMonkey(util, ChaosMonkey.EVERY_MINUTE_RANDOM_ACTION_POLICY);
    monkey.start();
  }

  @After
  public void tearDown() throws Exception {
    monkey.stop("test has finished, that's why");
    monkey.waitForStop();
    util.restoreCluster();
  }

  private void deleteTableIfNecessary() throws IOException {
    if (util.getHBaseAdmin().tableExists(TABLE_NAME)) {
      util.deleteTable(Bytes.toBytes(TABLE_NAME));
    }
  }

  @Test
  public void testDataIngest() throws Exception {
    LOG.info("Running testDataIngest");
    LOG.info("Cluster size:" + util.getHBaseClusterInterface().getClusterStatus().getServersSize());

    LoadTestTool loadTool = new LoadTestTool();
    loadTool.setConf(util.getConfiguration());

    long start = System.currentTimeMillis();
    long runtime = util.getConfiguration().getLong(RUN_TIME_KEY, DEFAULT_RUN_TIME);
    long startKey = 0;

    long numKeys = estimateDataSize();
    while (System.currentTimeMillis() - start < 0.9 * runtime) {
      LOG.info("Intended run time: " + (runtime/60000) + " min, left:" +
          ((runtime - (System.currentTimeMillis() - start))/60000) + " min");

      int ret = loadTool.run(new String[] {
          "-tn", TABLE_NAME,
          "-write", "10:100:20",
          "-start_key", String.valueOf(startKey),
          "-num_keys", String.valueOf(numKeys)
      });

      //assert that load was successful
      Assert.assertEquals(0, ret);

      ret = loadTool.run(new String[] {
          "-tn", TABLE_NAME,
          "-read", "100:20",
          "-start_key", String.valueOf(startKey),
          "-num_keys", String.valueOf(numKeys)
      });

      //assert that verify was successful
      Assert.assertEquals(0, ret);
      startKey += numKeys;
    }
  }

  /** Estimates a data size based on the cluster size */
  protected long estimateDataSize() throws IOException {
    //base is a 4 slave node cluster.
    ClusterStatus status = cluster.getClusterStatus();
    int numRegionServers = status.getServersSize();
    int multiplication = Math.max(1, numRegionServers / NUM_SLAVES_BASE);

    return 10000 * multiplication;
  }
}
