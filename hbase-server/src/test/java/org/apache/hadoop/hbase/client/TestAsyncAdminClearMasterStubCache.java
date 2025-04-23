/*
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.Socket;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Testcase for HBASE-29214
 */
@RunWith(Parameterized.class)
@Category({ ClientTests.class, MediumTests.class })
public class TestAsyncAdminClearMasterStubCache extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncAdminClearMasterStubCache.class);

  @Before
  public void waitMasterReady() throws Exception {
    assertTrue(TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster(30000));
  }

  @After
  public void clearPortConfig() {
    TEST_UTIL.getHBaseCluster().getConf().setInt(HConstants.MASTER_PORT, 0);
  }

  @Test
  public void testClearMasterStubCache() throws Exception {
    // cache master stub
    assertNotNull(FutureUtils.get(admin.getClusterMetrics()));
    // stop the active master
    SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    MasterThread mt = cluster.getMasterThread();
    ServerName sn = mt.getMaster().getServerName();
    mt.getMaster().abort("for testing");
    mt.join();
    // wait for new active master
    assertTrue(TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster(30000));
    // restart master on the same port, this is important for getting a RemoteException
    cluster.getConf().setInt(HConstants.MASTER_PORT, sn.getPort());
    cluster.startMaster();
    // make sure the master is up so we will not get a connect exception
    TEST_UTIL.waitFor(30000, () -> {
      try (Socket socket = new Socket(sn.getHostname(), sn.getPort())) {
        return true;
      } catch (IOException e) {
        return false;
      }
    });
    // we should switch to the new active master
    assertNotNull(FutureUtils.get(admin.getClusterMetrics()));
  }
}
