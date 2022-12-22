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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.master.MasterStateStoreTestBase;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LoadBalancerProtos;

@Category({ MasterTests.class, MediumTests.class })
public class TestLoadBalancerStateStore extends MasterStateStoreTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLoadBalancerStateStore.class);

  @After
  public void tearDown() throws Exception {
    cleanup();
    ZKUtil.deleteNodeFailSilent(UTIL.getZooKeeperWatcher(),
      UTIL.getZooKeeperWatcher().getZNodePaths().balancerZNode);
  }

  @Test
  public void testReadWrite() throws Exception {
    LoadBalancerStateStore store = new LoadBalancerStateStore(REGION, UTIL.getZooKeeperWatcher());
    assertTrue(store.get());
    store.set(false);
    assertFalse(store.get());

    // restart
    store = new LoadBalancerStateStore(REGION, UTIL.getZooKeeperWatcher());
    assertFalse(store.get());
    store.set(true);
    assertTrue(store.get());
  }

  @Test
  public void testMigrate() throws Exception {
    // prepare data on zk which set balancer on to false, since the default value is true
    byte[] zkData = ProtobufUtil.prependPBMagic(
      LoadBalancerProtos.LoadBalancerState.newBuilder().setBalancerOn(false).build().toByteArray());
    ZKUtil.createSetData(UTIL.getZooKeeperWatcher(),
      UTIL.getZooKeeperWatcher().getZNodePaths().balancerZNode, zkData);

    LoadBalancerStateStore store = new LoadBalancerStateStore(REGION, UTIL.getZooKeeperWatcher());
    assertFalse(store.get());
    // should have deleted the node on zk
    assertEquals(-1, ZKUtil.checkExists(UTIL.getZooKeeperWatcher(),
      UTIL.getZooKeeperWatcher().getZNodePaths().balancerZNode));
  }
}
