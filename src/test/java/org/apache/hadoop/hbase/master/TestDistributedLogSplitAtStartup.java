/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.master.TestLogSplitOnMasterFailover.DataLoader;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Stop a cluster with some HLogs left. Restart the cluster and make sure
 * the master correctly does distributed log splitting after startup.
 */
public class TestDistributedLogSplitAtStartup {
  private static final Log LOG = LogFactory.getLog(
      TestDistributedLogSplitAtStartup.class);

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_SPLITTING_KEY, true);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testDistributedLogSplitAtStartup() throws Exception {
    DataLoader dataLoader = new DataLoader(conf);
    new Thread(dataLoader).start();
    dataLoader.waitUntilHalfRowsLoaded();
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    LOG.info("Killing master and regionserver so we have logs to split");
    cluster.killMaster(0);
    cluster.getRegionServer(0).kill();
    Threads.sleepWithoutInterrupt(1000);
    LOG.info("Re-starting the cluster");
    LOG.info("Starting new master");
    cluster.startNewMaster();
    LOG.info("Waiting for master to come up");
    assertTrue(cluster.waitForActiveAndReadyMaster());
    Threads.sleepWithoutInterrupt(1000);

    LOG.info("Starting new region server");
    cluster.startRegionServer();
    dataLoader.waitUntilFinishedOrFailed();
    dataLoader.join();
    dataLoader.assertSuccess();
  }

}
