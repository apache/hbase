/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the default table lock manager
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestTableStateManager {

  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 60000)
  public void testUpgradeFromZk() throws Exception {
    TableName tableName =
        TableName.valueOf("testUpgradeFromZk");
    TEST_UTIL.startMiniCluster(2, 1);
    TEST_UTIL.shutdownMiniHBaseCluster();
    ZooKeeperWatcher watcher = TEST_UTIL.getZooKeeperWatcher();
    setTableStateInZK(watcher, tableName, ZooKeeperProtos.DeprecatedTableState.State.DISABLED);
    TEST_UTIL.restartHBaseCluster(1);

    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    Assert.assertEquals(
        master.getTableStateManager().getTableState(tableName),
        TableState.State.DISABLED);
  }

  private void setTableStateInZK(ZooKeeperWatcher watcher, final TableName tableName,
      final ZooKeeperProtos.DeprecatedTableState.State state)
      throws KeeperException, IOException {
    String znode = ZKUtil.joinZNode(watcher.tableZNode, tableName.getNameAsString());
    if (ZKUtil.checkExists(watcher, znode) == -1) {
      ZKUtil.createAndFailSilent(watcher, znode);
    }
    ZooKeeperProtos.DeprecatedTableState.Builder builder =
        ZooKeeperProtos.DeprecatedTableState.newBuilder();
    builder.setState(state);
    byte[] data = ProtobufUtil.prependPBMagic(builder.build().toByteArray());
    ZKUtil.setData(watcher, znode, data);
  }

}
