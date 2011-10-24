/*
 * Copyright 2011 The Apache Software Foundation
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
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.junit.Test;

public class TestMasterFailover {
  private static final Log LOG = LogFactory.getLog(TestMasterFailover.class);

  /**
   * Simple test of master failover.
   * <p>
   * Starts with three masters.  Kills a backup master.  Then kills the active
   * master.  Ensures the final master becomes active and we can still contact
   * the cluster.
   * @throws Exception
   */
  @Test
  public void testSimpleMasterFailover() throws Exception {

    final int NUM_MASTERS = 3;
    final int NUM_RS = 3;

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

    // get all the master threads
    List<HMaster> HMasters = cluster.getMasters();

    // make sure all masters came online
    for (HMaster mt : HMasters) {
      assertTrue(mt.isAlive());
    }

    // verify only one is the active master and we have right number
    int numActive = 0;
    int activeIndex = -1;
    String activeName = null;
    for (int i = 0; i < HMasters.size(); i++) {
      if (HMasters.get(i).isActiveMaster()) {
        numActive++;
        activeIndex = i;
        activeName = HMasters.get(i).getServerName();
      }
    }
    assertEquals(1, numActive);
    assertEquals(NUM_MASTERS, HMasters.size());

    // attempt to stop one of the inactive masters
    int backupIndex = (activeIndex + 1) % HMasters.size();
    LOG.debug("\n\nStopping backup master (#" + backupIndex + ")\n");
    cluster.killMaster(backupIndex);
    cluster.waitOnMasterStop(backupIndex);

    // verify still one active master and it's the same
    for (int i = 0; i < HMasters.size(); i++) {
      if (HMasters.get(i).isActiveMaster()) {
        assertTrue(activeName.equals(
            HMasters.get(i).getServerName()));
        activeIndex = i;
      }
    }
    assertTrue(activeIndex != -1);
    assertEquals(1, numActive);
    assertEquals(2, HMasters.size());

    // kill the active master
    LOG.debug("\n\nStopping the active master (#" + activeIndex + ")\n");
    cluster.killMaster(activeIndex);
    cluster.waitOnMasterStop(activeIndex);

    // wait for an active master to show up and be ready
    assertTrue(cluster.waitForActiveAndReadyMaster());

    LOG.debug("\n\nVerifying backup master is now active\n");
    // should only have one master now
    assertEquals(1, HMasters.size());
    // and he should be active
    assertTrue(HMasters.get(0).isActiveMaster());

    // Stop the cluster
    TEST_UTIL.shutdownMiniCluster();
  }

}
