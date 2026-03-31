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
package org.apache.hadoop.hbase.replication;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestMigrateRepliationPeerStorageOnline {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMigrateRepliationPeerStorageOnline.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeClass
  public static void setUp() throws Exception {
    // use zookeeper first, and then migrate to filesystem
    UTIL.getConfiguration().set(ReplicationStorageFactory.REPLICATION_PEER_STORAGE_IMPL,
      ReplicationPeerStorageType.ZOOKEEPER.name());
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMigrate() throws Exception {
    Admin admin = UTIL.getAdmin();
    ReplicationPeerConfig rpc =
      ReplicationPeerConfig.newBuilder().setClusterKey(UTIL.getRpcConnnectionURI() + "-test")
        .setReplicationEndpointImpl(DummyReplicationEndpoint.class.getName()).build();
    admin.addReplicationPeer("1", rpc);

    // disable peer modification
    admin.replicationPeerModificationSwitch(false, true);

    // migrate replication peer data
    Configuration conf = new Configuration(UTIL.getConfiguration());
    assertEquals(0, ToolRunner.run(conf, new CopyReplicationPeers(conf),
      new String[] { "zookeeper", "filesystem" }));
    conf.set(ReplicationStorageFactory.REPLICATION_PEER_STORAGE_IMPL,
      ReplicationPeerStorageType.FILESYSTEM.name());
    // confirm that we have copied the data
    ReplicationPeerStorage fsPeerStorage = ReplicationStorageFactory
      .getReplicationPeerStorage(UTIL.getTestFileSystem(), UTIL.getZooKeeperWatcher(), conf);
    assertNotNull(fsPeerStorage.getPeerConfig("1"));

    for (MasterThread mt : UTIL.getMiniHBaseCluster().getMasterThreads()) {
      Configuration newConf = new Configuration(mt.getMaster().getConfiguration());
      newConf.set(ReplicationStorageFactory.REPLICATION_PEER_STORAGE_IMPL,
        ReplicationPeerStorageType.FILESYSTEM.name());
      mt.getMaster().getConfigurationManager().notifyAllObservers(newConf);
    }
    for (RegionServerThread rt : UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      Configuration newConf = new Configuration(rt.getRegionServer().getConfiguration());
      newConf.set(ReplicationStorageFactory.REPLICATION_PEER_STORAGE_IMPL,
        ReplicationPeerStorageType.FILESYSTEM.name());
      rt.getRegionServer().getConfigurationManager().notifyAllObservers(newConf);
    }

    admin.replicationPeerModificationSwitch(true);
    admin.removeReplicationPeer("1");

    // confirm that we will operation on the new peer storage
    assertThat(fsPeerStorage.listPeerIds(), empty());
  }
}
