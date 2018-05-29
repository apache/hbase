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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
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
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationStateZKImpl extends TestReplicationStateBasic {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationStateZKImpl.class);

  private static Configuration conf;
  private static HBaseZKTestingUtility utility;
  private static ZKWatcher zkw;
  private static String replicationZNode;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    utility = new HBaseZKTestingUtility();
    utility.startMiniZKCluster();
    conf = utility.getConfiguration();
    conf.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, true);
    zkw = utility.getZooKeeperWatcher();
    String replicationZNodeName = conf.get("zookeeper.znode.replication", "replication");
    replicationZNode = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, replicationZNodeName);
    KEY_ONE = initPeerClusterState("/hbase1");
    KEY_TWO = initPeerClusterState("/hbase2");
  }

  private static String initPeerClusterState(String baseZKNode)
      throws IOException, KeeperException {
    // Add a dummy region server and set up the cluster id
    Configuration testConf = new Configuration(conf);
    testConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, baseZKNode);
    ZKWatcher zkw1 = new ZKWatcher(testConf, "test1", null);
    String fakeRs = ZNodePaths.joinZNode(zkw1.getZNodePaths().rsZNode,
            "hostname1.example.org:1234");
    ZKUtil.createWithParents(zkw1, fakeRs);
    ZKClusterId.setClusterId(zkw1, new ClusterId());
    return ZKConfig.getZooKeeperClusterKey(testConf);
  }

  @Before
  public void setUp() {
    zkTimeoutCount = 0;
    rqs = ReplicationStorageFactory.getReplicationQueueStorage(zkw, conf);
    rp = ReplicationFactory.getReplicationPeers(zkw, conf);
    OUR_KEY = ZKConfig.getZooKeeperClusterKey(conf);
  }

  @After
  public void tearDown() throws KeeperException, IOException {
    ZKUtil.deleteNodeRecursively(zkw, replicationZNode);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    utility.shutdownMiniZKCluster();
  }
}
