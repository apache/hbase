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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;

@Category({ MasterTests.class, MediumTests.class })
public class TestSplitOrMergeStateStore extends MasterStateStoreTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSplitOrMergeStateStore.class);

  @After
  public void tearDown() throws Exception {
    cleanup();
    ZKUtil.deleteNodeRecursively(UTIL.getZooKeeperWatcher(),
      UTIL.getZooKeeperWatcher().getZNodePaths().switchZNode);
  }

  @Test
  public void testSplit() throws Exception {
    testReadWrite(MasterSwitchType.SPLIT);
  }

  @Test
  public void testMerge() throws Exception {
    testReadWrite(MasterSwitchType.MERGE);
  }

  @Test
  public void testSplitMigrate() throws Exception {
    testMigrate(MasterSwitchType.SPLIT,
      ZNodePaths.joinZNode(UTIL.getZooKeeperWatcher().getZNodePaths().switchZNode,
        UTIL.getConfiguration().get("zookeeper.znode.switch.split", "split")));
  }

  @Test
  public void testMergeMigrate() throws Exception {
    testMigrate(MasterSwitchType.MERGE,
      ZNodePaths.joinZNode(UTIL.getZooKeeperWatcher().getZNodePaths().switchZNode,
        UTIL.getConfiguration().get("zookeeper.znode.switch.merge", "merge")));
  }

  private void testReadWrite(MasterSwitchType type) throws Exception {
    SplitOrMergeStateStore store =
      new SplitOrMergeStateStore(REGION, UTIL.getZooKeeperWatcher(), UTIL.getConfiguration());
    assertTrue(store.isSplitOrMergeEnabled(type));
    store.setSplitOrMergeEnabled(false, type);
    assertFalse(store.isSplitOrMergeEnabled(type));

    // restart
    store = new SplitOrMergeStateStore(REGION, UTIL.getZooKeeperWatcher(), UTIL.getConfiguration());
    assertFalse(store.isSplitOrMergeEnabled(type));
    store.setSplitOrMergeEnabled(true, type);
    assertTrue(store.isSplitOrMergeEnabled(type));
  }

  private void testMigrate(MasterSwitchType type, String zkPath) throws Exception {
    // prepare data on zk which set snapshot cleanup enabled to false, since the default value is
    // true
    byte[] zkData = ProtobufUtil.prependPBMagic(
      ZooKeeperProtos.SwitchState.newBuilder().setEnabled(false).build().toByteArray());
    ZKUtil.createSetData(UTIL.getZooKeeperWatcher(), zkPath, zkData);

    SplitOrMergeStateStore store =
      new SplitOrMergeStateStore(REGION, UTIL.getZooKeeperWatcher(), UTIL.getConfiguration());
    assertFalse(store.isSplitOrMergeEnabled(type));
    // should have deleted the node on zk
    assertEquals(-1, ZKUtil.checkExists(UTIL.getZooKeeperWatcher(), zkPath));
  }
}
