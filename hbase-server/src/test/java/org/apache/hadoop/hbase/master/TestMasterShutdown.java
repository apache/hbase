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

import org.apache.hadoop.hbase.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestMasterShutdown {
  private static final Log LOG = LogFactory.getLog(TestMasterShutdown.class);

  /**
   * Simple test of shutdown.
   * <p>
   * Starts with three masters.  Tells the active master to shutdown the cluster.
   * Verifies that all masters are properly shutdown.
   * @throws Exception
   */
  @Test (timeout=240000)
  public void testMasterShutdown() throws Exception {

    final int NUM_MASTERS = 3;
    final int NUM_RS = 3;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

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
    assertEquals(0,masterThreads.size());
    
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 180000)
  public void testMasterShutdownBeforeStartingAnyRegionServer() throws Exception {

    final int NUM_MASTERS = 1;
    final int NUM_RS = 0;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();

    // Start the cluster
    final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniDFSCluster(3);
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.createRootDir();
    final LocalHBaseCluster cluster =
        new LocalHBaseCluster(conf, NUM_MASTERS, NUM_RS, HMaster.class,
            MiniHBaseCluster.MiniHBaseClusterRegionServer.class);
    final MasterThread master = cluster.getMasters().get(0);
    master.start();
    Thread shutdownThread = new Thread() {
      public void run() {
        try {
          TEST_UTIL.getHBaseAdmin().shutdown();
          cluster.waitOnMaster(0);
        } catch (Exception e) {
        }
      };
    };
    shutdownThread.start();
    master.join();
    shutdownThread.join();

    List<MasterThread> masterThreads = cluster.getMasters();
    // make sure all the masters properly shutdown
    assertEquals(0, masterThreads.size());

    TEST_UTIL.shutdownMiniZKCluster();
    TEST_UTIL.shutdownMiniDFSCluster();
    TEST_UTIL.cleanupTestDir();
  }

}

