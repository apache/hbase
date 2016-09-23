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

package org.apache.hadoop.hbase.zookeeper;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestZooKeeperWatcher {

  @Test
  public void testIsClientReadable() throws ZooKeeperConnectionException, IOException {
    ZooKeeperWatcher watcher = new ZooKeeperWatcher(HBaseConfiguration.create(),
        "testIsClientReadable", null, false);

    assertTrue(watcher.isClientReadable(watcher.znodePaths.baseZNode));
    assertTrue(watcher.isClientReadable(watcher.znodePaths.getZNodeForReplica(0)));
    assertTrue(watcher.isClientReadable(watcher.znodePaths.masterAddressZNode));
    assertTrue(watcher.isClientReadable(watcher.znodePaths.clusterIdZNode));
    assertTrue(watcher.isClientReadable(watcher.znodePaths.tableZNode));
    assertTrue(watcher.isClientReadable(ZKUtil.joinZNode(watcher.znodePaths.tableZNode, "foo")));
    assertTrue(watcher.isClientReadable(watcher.znodePaths.rsZNode));

    assertFalse(watcher.isClientReadable(watcher.znodePaths.tableLockZNode));
    assertFalse(watcher.isClientReadable(watcher.znodePaths.balancerZNode));
    assertFalse(watcher.isClientReadable(watcher.znodePaths.regionNormalizerZNode));
    assertFalse(watcher.isClientReadable(watcher.znodePaths.clusterStateZNode));
    assertFalse(watcher.isClientReadable(watcher.znodePaths.drainingZNode));
    assertFalse(watcher.isClientReadable(watcher.znodePaths.recoveringRegionsZNode));
    assertFalse(watcher.isClientReadable(watcher.znodePaths.splitLogZNode));
    assertFalse(watcher.isClientReadable(watcher.znodePaths.backupMasterAddressesZNode));

    watcher.close();
  }

}
