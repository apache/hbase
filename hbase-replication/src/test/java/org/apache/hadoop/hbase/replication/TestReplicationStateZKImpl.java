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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseZKTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationStateZKImpl extends TestReplicationStateBasic {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationStateZKImpl.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationStateZKImpl.class);

  private static Configuration conf;
  private static HBaseZKTestingUtility utility;
  private static ZKWatcher zkw;
  private static String replicationZNode;
  private ReplicationQueuesZKImpl rqZK;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    utility = new HBaseZKTestingUtility();
    utility.startMiniZKCluster();
    conf = utility.getConfiguration();
    conf.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, true);
    zkw = utility.getZooKeeperWatcher();
    String replicationZNodeName = conf.get("zookeeper.znode.replication", "replication");
    replicationZNode = ZNodePaths.joinZNode(zkw.znodePaths.baseZNode, replicationZNodeName);
    KEY_ONE = initPeerClusterState("/hbase1");
    KEY_TWO = initPeerClusterState("/hbase2");
  }

  private static String initPeerClusterState(String baseZKNode)
      throws IOException, KeeperException {
    // Add a dummy region server and set up the cluster id
    Configuration testConf = new Configuration(conf);
    testConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, baseZKNode);
    ZKWatcher zkw1 = new ZKWatcher(testConf, "test1", null);
    String fakeRs = ZNodePaths.joinZNode(zkw1.znodePaths.rsZNode, "hostname1.example.org:1234");
    ZKUtil.createWithParents(zkw1, fakeRs);
    ZKClusterId.setClusterId(zkw1, new ClusterId());
    return ZKConfig.getZooKeeperClusterKey(testConf);
  }

  @Before
  public void setUp() {
    zkTimeoutCount = 0;
    WarnOnlyAbortable abortable = new WarnOnlyAbortable();
    try {
      rq1 = ReplicationFactory
          .getReplicationQueues(new ReplicationQueuesArguments(conf, abortable, zkw));
      rq2 = ReplicationFactory
          .getReplicationQueues(new ReplicationQueuesArguments(conf, abortable, zkw));
      rq3 = ReplicationFactory
          .getReplicationQueues(new ReplicationQueuesArguments(conf, abortable, zkw));
      rqs = ReplicationStorageFactory.getReplicationQueueStorage(zkw, conf);
    } catch (Exception e) {
      // This should not occur, because getReplicationQueues() only throws for
      // TableBasedReplicationQueuesImpl
      fail("ReplicationFactory.getReplicationQueues() threw an IO Exception");
    }
    rp = ReplicationFactory.getReplicationPeers(zkw, conf, zkw);
    OUR_KEY = ZKConfig.getZooKeeperClusterKey(conf);
    rqZK = new ReplicationQueuesZKImpl(zkw, conf, abortable);
  }

  @After
  public void tearDown() throws KeeperException, IOException {
    ZKUtil.deleteNodeRecursively(zkw, replicationZNode);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    utility.shutdownMiniZKCluster();
  }

  @Test
  public void testIsPeerPath_PathToParentOfPeerNode() {
    assertFalse(rqZK.isPeerPath(rqZK.peersZNode));
  }

  @Test
  public void testIsPeerPath_PathToChildOfPeerNode() {
    String peerChild = ZNodePaths.joinZNode(ZNodePaths.joinZNode(rqZK.peersZNode, "1"), "child");
    assertFalse(rqZK.isPeerPath(peerChild));
  }

  @Test
  public void testIsPeerPath_ActualPeerPath() {
    String peerPath = ZNodePaths.joinZNode(rqZK.peersZNode, "1");
    assertTrue(rqZK.isPeerPath(peerPath));
  }

  private static class WarnOnlyAbortable implements Abortable {

    @Override
    public void abort(String why, Throwable e) {
      LOG.warn("TestReplicationStateZKImpl received abort, ignoring.  Reason: " + why);
      if (LOG.isDebugEnabled()) {
        LOG.debug(e.toString(), e);
      }
    }

    @Override
    public boolean isAborted() {
      return false;
    }
  }
}
