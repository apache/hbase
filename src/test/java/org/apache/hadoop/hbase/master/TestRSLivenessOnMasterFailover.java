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

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.util.TagRunner;
import org.apache.hadoop.hbase.util.TestTag;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests that the regionserver correctly picks up the new master location and
 * does not get stuck in a retry loop trying to contact the old master.
 */
@RunWith(TagRunner.class)
public class TestRSLivenessOnMasterFailover extends MultiMasterTest {

  // Marked as unstable and recorded in 3921469
  @TestTag({ "unstable" })
  @Test(timeout=60000)
  public void testAgainstRSDeadlock() throws IOException,
      InterruptedException, KeeperException {
    // Use low RPC timeout because the regionserver will try to talk to a
    // master that is not there.
    testUtil.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 3000);
    startMiniCluster(1, 1);

    HServerAddress killedMasterAddress = killActiveMasterAndWaitToStop();

    // Write the address of the dead master to ZK to confuse the RS and make
    // it go into a retry loop trying to talk to this master. If the RS does
    // not reload the master address from ZK, it will get stuck in an infinite
    // loop, which is the bug this unit test is trying to catch.
    ZooKeeperWrapper zkw = ZooKeeperWrapper.createInstance(
        testUtil.getConfiguration(), "spoofMasterAddress");
    assertNotNull(zkw);
    zkw.writeMasterAddress(killedMasterAddress);
    miniCluster().startRegionServerNoWait();

    // Let the regionserver start up and attempt to check in with the master.
    // Unfortunately, there does not seem to be an easy way to reliably wait
    // for this specific condition.
    Threads.sleepWithoutInterrupt(3000);

    // Delete the fake master address that we created to confuse the RS.
    zkw.deleteMasterAddress();

    miniCluster().startNewMaster();
    waitForActiveMasterAndVerify();

    zkw.close();
  }

}
