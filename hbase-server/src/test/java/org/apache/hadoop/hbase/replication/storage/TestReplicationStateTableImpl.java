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

package org.apache.hadoop.hbase.replication.storage;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.TableReplicationPeerStorage;
import org.apache.hadoop.hbase.replication.TableReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.TableReplicationStorageBase;
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
public class TestReplicationStateTableImpl extends TestReplicationStateBasic {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationStateTableImpl.class);

  private static Configuration conf;
  private static HBaseTestingUtility utility = new HBaseTestingUtility();
  private static ZKWatcher zkw;
  private static Connection connection;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = utility.getConfiguration();
    conf.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, true);
    conf.set(HConstants.REPLICATION_CLUSTER_ID, "12345");
    utility.startMiniCluster();

    // After the HBase Mini cluster startup, we set the storage implementation to table based
    // implementation. Otherwise, we cannot setup the HBase Mini Cluster because the master will
    // list peers before finish its initialization, and if master cannot finish initialization, the
    // meta cannot be online, in other hand, if meta cannot be online, the list peers never success
    // when using table based replication. a dead loop happen.
    // Our UTs are written for testing storage layer, so no problem here.
    conf.set(ReplicationStorageFactory.REPLICATION_PEER_STORAGE_IMPL,
      TableReplicationPeerStorage.class.getName());
    conf.set(ReplicationStorageFactory.REPLICATION_QUEUE_STORAGE_IMPL,
      TableReplicationQueueStorage.class.getName());

    zkw = utility.getZooKeeperWatcher();
    connection = ConnectionFactory.createConnection(conf);

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
  public void setUp() throws IOException {
    rqs = ReplicationStorageFactory.getReplicationQueueStorage(zkw, conf);
    rp = ReplicationFactory.getReplicationPeers(zkw, conf);
    OUR_KEY = ZKConfig.getZooKeeperClusterKey(conf);

    // Create hbase:replication meta table.
    try (Admin admin = connection.getAdmin()) {
      TableDescriptor table =
          TableReplicationStorageBase.createReplicationTableDescBuilder(conf).build();
      admin.createTable(table);
    }
  }

  @After
  public void tearDown() throws KeeperException, IOException {
    // Drop the hbase:replication meta table.
    utility.deleteTable(TableReplicationStorageBase.REPLICATION_TABLE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (connection != null) {
      IOUtils.closeQuietly(connection);
    }
    utility.shutdownMiniZKCluster();
  }
}
