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
package org.apache.hadoop.hbase.zookeeper;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseZKTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ZKTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ ZKTests.class, MediumTests.class })
public class TestRegionServerAddressTracker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionServerAddressTracker.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionServerAddressTracker.class);

  private static final HBaseZKTestingUtility TEST_UTIL = new HBaseZKTestingUtility();

  private ZKWatcher zk;

  private RegionServerAddressTracker tracker;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Before
  public void setUp() throws ZooKeeperConnectionException, IOException, KeeperException {
    TEST_UTIL.getConfiguration().set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/" + name.getMethodName());
    zk = new ZKWatcher(TEST_UTIL.getConfiguration(), name.getMethodName(), null);
    ZKUtil.createWithParents(zk, zk.getZNodePaths().rsZNode);
    tracker = new RegionServerAddressTracker(zk, new WarnOnlyAbortable());
  }

  @After
  public void tearDown() throws IOException {
    Closeables.close(zk, true);
  }

  @Test
  public void test() throws KeeperException {
    ServerName rs1 = ServerName.valueOf("127.0.0.1", 16000, EnvironmentEdgeManager.currentTime());
    ZKUtil.createWithParents(zk, ZNodePaths.joinZNode(zk.getZNodePaths().rsZNode, rs1.toString()));
    TEST_UTIL.waitFor(10000, () -> tracker.getRegionServers().size() == 1);
    assertEquals(rs1, tracker.getRegionServers().get(0));

    ServerName rs2 = ServerName.valueOf("127.0.0.2", 16000, EnvironmentEdgeManager.currentTime());
    ZKUtil.createWithParents(zk, ZNodePaths.joinZNode(zk.getZNodePaths().rsZNode, rs2.toString()));
    TEST_UTIL.waitFor(10000, () -> tracker.getRegionServers().size() == 2);
    assertThat(tracker.getRegionServers(), hasItems(rs1, rs2));

    ZKUtil.deleteNode(zk, ZNodePaths.joinZNode(zk.getZNodePaths().rsZNode, rs1.toString()));
    TEST_UTIL.waitFor(10000, () -> tracker.getRegionServers().size() == 1);
    assertEquals(rs2, tracker.getRegionServers().get(0));

    ZKUtil.deleteNode(zk, ZNodePaths.joinZNode(zk.getZNodePaths().rsZNode, rs2.toString()));
    TEST_UTIL.waitFor(10000, () -> tracker.getRegionServers().isEmpty());
  }

  private static final class WarnOnlyAbortable implements Abortable {
    @Override
    public void abort(String why, Throwable e) {
      LOG.warn("RegionServerAddressTracker received abort, ignoring.  Reason: {}", why, e);
    }

    @Override
    public boolean isAborted() {
      return false;
    }
  }
}
