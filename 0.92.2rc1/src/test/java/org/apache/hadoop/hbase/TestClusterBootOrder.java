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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the boot order indifference between regionserver and master
 */
public class TestClusterBootOrder {

  private static final long SLEEP_INTERVAL = 1000;
  private static final long SLEEP_TIME = 4000;

  private HBaseTestingUtility testUtil;
  private LocalHBaseCluster cluster;
  private RegionServerThread rs;
  private MasterThread master;

  @Before
  public void setUp() throws Exception {
    testUtil = new HBaseTestingUtility();
    testUtil.startMiniDFSCluster(1);
    testUtil.startMiniZKCluster(1);
    testUtil.createRootDir(); //manually setup hbase dir to point to minidfscluster
    cluster = new LocalHBaseCluster(testUtil.getConfiguration(), 0, 0);
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
    cluster.join();
    testUtil.shutdownMiniZKCluster();
    testUtil.shutdownMiniDFSCluster();
  }

  private void startRegionServer() throws Exception {
    rs = cluster.addRegionServer();
    rs.start();

    for (int i=0; i * SLEEP_INTERVAL < SLEEP_TIME ;i++) {
      //we cannot block on wait for rs at this point , since master is not up.
      Thread.sleep(SLEEP_INTERVAL);
      assertTrue(rs.isAlive());
    }
  }

  private void startMaster() throws Exception {
    master = cluster.addMaster();
    master.start();

    for (int i=0; i * SLEEP_INTERVAL < SLEEP_TIME ;i++) {
      Thread.sleep(SLEEP_INTERVAL);
      assertTrue(master.isAlive());
    }
  }

  private void waitForClusterOnline() {
    while (true) {
      if (master.getMaster().isInitialized()) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {
        // Keep waiting
      }
    }
    rs.waitForServerOnline();
  }

  /**
   * Tests launching the cluster by first starting regionserver, and then the master
   * to ensure that it does not matter which is started first.
   */
  @Test
  public void testBootRegionServerFirst() throws Exception {
    startRegionServer();
    startMaster();
    waitForClusterOnline();
  }

  /**
   * Tests launching the cluster by first starting master, and then the regionserver
   * to ensure that it does not matter which is started first.
   */
  @Test
  public void testBootMasterFirst() throws Exception {
    startMaster();
    startRegionServer();
    waitForClusterOnline();
  }
}
