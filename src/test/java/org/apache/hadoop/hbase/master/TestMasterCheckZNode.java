/**
 * Copyright 2014 The Apache Software Foundation
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

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Testcases related to znode checking.
 */
public class TestMasterCheckZNode {
  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.useLFS();
    TEST_UTIL.startMiniCluster(2, 1);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * This testcase tries to remove the election znode to make both master
   * running. The original active master should kill itself.
   */
  @Test(timeout = 300000)
  public void testCheckingOfZKNode() throws Exception {
    ZooKeeperWrapper zk =
        ZooKeeperWrapper.createInstance(TEST_UTIL.getConfiguration(),
            "testCheckingOfZKNode");
    HMaster primary = null;
    HMaster secondary = null;
    for (HMaster master : TEST_UTIL.getMiniHBaseCluster().getMasters()) {
      if (master.isActiveMaster()) {
        primary = master;
      } else {
        secondary = master;
      }
    }
    Assert.assertNotNull("Cannot find primary master", primary);
    Assert.assertNotNull("Cannot find secondary master", secondary);

    // Delete the election znode to make HBase crazy
    zk.deleteMasterAddress();

    // Wait for the original primary to die
    long start = System.currentTimeMillis();
    while (true) {
      if (primary.isClosed()) {
        break;
      }
      if (System.currentTimeMillis() - start > 30 * 1000) {
        throw new Exception("The original primary doesn't kill itself!");
      }
      Thread.sleep(100);
    }

    // Wait for the secondary to be also active
    start = System.currentTimeMillis();
    while (true) {
      if (secondary.isActiveMaster()) {
        break;
      }
      if (System.currentTimeMillis() - start > 120 * 1000) {
        throw new Exception("The secondary is not started!");
      }
      Thread.sleep(100);
    }

    Assert.assertTrue("new active master should be still alive",
        secondary.isAlive() && secondary.isActiveMaster());
  }

}
