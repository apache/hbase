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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, MediumTests.class })
public class TestNewStartedRegionServerVersion {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestNewStartedRegionServerVersion.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestNewStartedRegionServerVersion.class);

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws InterruptedException {
    // Start 3 new region server
    Thread t = new Thread(() -> {
      for (int i = 0; i < 3; i++) {
        try {
          JVMClusterUtil.RegionServerThread newRS = UTIL.getMiniHBaseCluster().startRegionServer();
          newRS.waitForServerOnline();
        } catch (IOException e) {
          LOG.error("Failed to start a new RS", e);
        }
      }
    });
    t.start();

    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    while (t.isAlive()) {
      List<ServerName> serverNames = master.getServerManager().getOnlineServersList();
      for (ServerName serverName : serverNames) {
        assertNotEquals(0, master.getServerManager().getVersionNumber(serverName));
      }
      Thread.sleep(100);
    }
  }
}
