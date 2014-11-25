/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestMasterShutdown {
  public static final Log LOG = LogFactory.getLog(TestMasterShutdown.class);

  /**
   * Simple test of shutdown.
   * <p>
   * Starts with three masters.  Tells the active master to shutdown the cluster.
   * Verifies that all masters are properly shutdown.
   * @throws Exception
   */
  @Test (timeout=120000)
  public void testMasterShutdown() throws Exception {
    final int NUM_MASTERS = 3;
    final int NUM_RS = 3;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();

    // Start the cluster
    HBaseTestingUtility htu = new HBaseTestingUtility(conf);
    htu.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = htu.getHBaseCluster();

    // get all the master threads
    List<MasterThread> masterThreads = cluster.getMasterThreads();

    // wait for each to come online
    for (MasterThread mt : masterThreads) {
      assertTrue(mt.isAlive());
    }

    // find the active master
    HMaster active = null;
    for (int i = 0; i < masterThreads.size(); i++) {
      if (masterThreads.get(i).getMaster().isActiveMaster()) {
        active = masterThreads.get(i).getMaster();
        break;
      }
    }
    assertNotNull(active);
    // make sure the other two are backup masters
    ClusterStatus status = active.getClusterStatus();
    assertEquals(2, status.getBackupMastersSize());
    assertEquals(2, status.getBackupMasters().size());

    // tell the active master to shutdown the cluster
    active.shutdown();

    for (int i = NUM_MASTERS - 1; i >= 0 ;--i) {
      cluster.waitOnMaster(i);
    }
    // make sure all the masters properly shutdown
    assertEquals(0, masterThreads.size());

    htu.shutdownMiniCluster();
  }

  @Test(timeout = 60000)
  public void testMasterShutdownBeforeStartingAnyRegionServer() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_RS = 0;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.ipc.client.failed.servers.expiry", 200);
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);

    // Start the cluster
    final HBaseTestingUtility util = new HBaseTestingUtility(conf);
    util.startMiniDFSCluster(3);
    util.startMiniZKCluster();
    util.createRootDir();
    final LocalHBaseCluster cluster =
        new LocalHBaseCluster(conf, NUM_MASTERS, NUM_RS, HMaster.class,
            MiniHBaseCluster.MiniHBaseClusterRegionServer.class);
    final int MASTER_INDEX = 0;
    final MasterThread master = cluster.getMasters().get(MASTER_INDEX);
    master.start();
    LOG.info("Called master start on " + master.getName());
    Thread shutdownThread = new Thread() {
      public void run() {
        LOG.info("Before call to shutdown master");
        try {
          try (Connection connection =
              ConnectionFactory.createConnection(util.getConfiguration())) {
            try (Admin admin = connection.getAdmin()) {
              admin.shutdown();
            }
          }
          LOG.info("After call to shutdown master");
          cluster.waitOnMaster(MASTER_INDEX);
        } catch (Exception e) {
        }
      };
    };
    shutdownThread.start();
    LOG.info("Called master join on " + master.getName());
    master.join();
    shutdownThread.join();

    List<MasterThread> masterThreads = cluster.getMasters();
    // make sure all the masters properly shutdown
    assertEquals(0, masterThreads.size());

    util.shutdownMiniZKCluster();
    util.shutdownMiniDFSCluster();
    util.cleanupTestDir();
  }
}
