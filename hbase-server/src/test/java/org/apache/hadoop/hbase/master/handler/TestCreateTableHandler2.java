/**
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
package org.apache.hadoop.hbase.master.handler;

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestCreateTableHandler2 {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestCreateTableHandler2.class);

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testMasterRestartAfterNameSpaceEnablingNodeIsCreated() throws Exception {
    // Step 1: start mini zk cluster.
    MiniZooKeeperCluster zkCluster;
    zkCluster = TEST_UTIL.startMiniZKCluster();
    // Step 2: add an orphaned system table ZNODE
    TableName tableName = TableName.valueOf("hbase:namespace");
    ZooKeeperWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    String znode = ZKUtil.joinZNode(zkw.tableZNode, tableName.getNameAsString());
    ZooKeeperProtos.Table.Builder builder = ZooKeeperProtos.Table.newBuilder();
    builder.setState(ZooKeeperProtos.Table.State.ENABLED);
    byte [] data = ProtobufUtil.prependPBMagic(builder.build().toByteArray());
    ZKUtil.createSetData(zkw, znode, data);
    LOG.info("Create an orphaned Znode " + znode + " with data " + data);
    // Step 3: link the zk cluster to hbase cluster
    TEST_UTIL.setZkCluster(zkCluster);
    // Step 4: start hbase cluster and expect master to start successfully.
    TEST_UTIL.startMiniCluster();
    assertTrue(TEST_UTIL.getHBaseCluster().getLiveMasterThreads().size() == 1);
  }
}
