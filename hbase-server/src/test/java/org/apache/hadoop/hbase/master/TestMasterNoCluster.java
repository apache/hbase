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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Standup the master and fake it to test various aspects of master function.
 * Does NOT spin up a mini hbase nor mini dfs cluster testing master (it does
 * put up a zk cluster but this is usually pretty fast compared).  Also, should
 * be possible to inject faults at points difficult to get at in cluster context.
 * TODO: Speed up the zk connection by Master.  It pauses 5 seconds establishing
 * session.
 */
@Category({MasterTests.class, MediumTests.class})
public class TestMasterNoCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterNoCluster.class);

  private static final HBaseTestingUtility TESTUTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration c = TESTUTIL.getConfiguration();
    // We use local filesystem.  Set it so it writes into the testdir.
    FSUtils.setRootDir(c, TESTUTIL.getDataTestDir());
    DefaultMetricsSystem.setMiniClusterMode(true);
    // Startup a mini zk cluster.
    TESTUTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TESTUTIL.shutdownMiniZKCluster();
  }

  @After
  public void tearDown()
  throws KeeperException, ZooKeeperConnectionException, IOException {
    // Make sure zk is clean before we run the next test.
    ZKWatcher zkw = new ZKWatcher(TESTUTIL.getConfiguration(),
        "@Before", new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        throw new RuntimeException(why, e);
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    });
    ZKUtil.deleteNodeRecursively(zkw, zkw.getZNodePaths().baseZNode);
    zkw.close();
  }

  /**
   * Test starting master then stopping it before its fully up.
   */
  @Test
  public void testStopDuringStart()
  throws IOException, KeeperException, InterruptedException {
    HMaster master = new HMaster(TESTUTIL.getConfiguration());
    master.start();
    // Immediately have it stop.  We used hang in assigning meta.
    master.stopMaster();
    master.join();
  }

  @Test
  public void testMasterInitWithSameClientServerZKQuorum() throws Exception {
    Configuration conf = new Configuration(TESTUTIL.getConfiguration());
    conf.set(HConstants.CLIENT_ZOOKEEPER_QUORUM, HConstants.LOCALHOST);
    conf.setInt(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, TESTUTIL.getZkCluster().getClientPort());
    HMaster master = new HMaster(conf);
    master.start();
    // the master will abort due to IllegalArgumentException so we should finish within 60 seconds
    master.join();
  }

  @Test
  public void testMasterInitWithObserverModeClientZKQuorum() throws Exception {
    Configuration conf = new Configuration(TESTUTIL.getConfiguration());
    Assert.assertFalse(Boolean.getBoolean(HConstants.CLIENT_ZOOKEEPER_OBSERVER_MODE));
    // set client ZK to some non-existing address and make sure server won't access client ZK
    // (server start should not be affected)
    conf.set(HConstants.CLIENT_ZOOKEEPER_QUORUM, HConstants.LOCALHOST);
    conf.setInt(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT,
      TESTUTIL.getZkCluster().getClientPort() + 1);
    // settings to allow us not to start additional RS
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    conf.setBoolean(LoadBalancer.TABLES_ON_MASTER, true);
    // main setting for this test case
    conf.setBoolean(HConstants.CLIENT_ZOOKEEPER_OBSERVER_MODE, true);
    HMaster master = new HMaster(conf);
    master.start();
    while (!master.isInitialized()) {
      Threads.sleep(200);
    }
    Assert.assertNull(master.metaLocationSyncer);
    Assert.assertNull(master.masterAddressSyncer);
    master.stopMaster();
    master.join();
  }
}
