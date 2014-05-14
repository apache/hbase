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

import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMasterFailover extends MultiMasterTest {

  /**
   * Simple test of master failover.
   * <p>
   * Starts with three masters.  Kills a backup master.  Then kills the active
   * master.  Ensures the final master becomes active and we can still contact
   * the miniCluster().
   * @throws Exception
   */
  @Test(timeout=240000)
  public void testSimpleMasterFailover() throws Exception {
    ZooKeeperWrapper.setNamespaceForTesting();
    header("Starting simple master failover test");

    final int numMasters = 3;
    final int numRS = 3;

    startMiniCluster(numMasters, numRS);

    ensureMastersAreUp(numMasters);
    final int activeIndex = getActiveMasterIndex();

    final List<HMaster> masters = miniCluster().getMasters();
    final String activeName = masters.get(activeIndex).getServerName();

    // attempt to stop one of the inactive masters
    int backupIndex = (activeIndex + 1) % masters.size();

    header("Stopping backup master (#" + backupIndex + ")");
    killMasterAndWaitToStop(backupIndex);

    // Verify there is still one active master and it is the same.
    // We must compare server names, because indexes might have shifted.
    final int newActiveIndex = getActiveMasterIndex();
    assertTrue(activeName.equals(masters.get(newActiveIndex).getServerName()));
    assertEquals(2, masters.size());

    header("Stopping the active master (#" + newActiveIndex + "). Keep " +
        "in mind that the old master was removed, so the indexes might have " +
        "shifted.");

    killActiveMasterAndWaitToStop();
    assertEquals(1, masters.size());

    waitForActiveMasterAndVerify();
  }

}
 
