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

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestZooKeeperWatcher {
  private final static Log LOG = LogFactory.getLog(TestZooKeeperWatcher.class);
  
  @Test
  public void testIsClientReadable() throws ZooKeeperConnectionException, IOException {
    ZooKeeperWatcher watcher = new ZooKeeperWatcher(HBaseConfiguration.create(),
      "testIsClientReadable", null, false);

    assertTrue(watcher.isClientReadable(watcher.baseZNode));
    assertTrue(watcher.isClientReadable(watcher.getZNodeForReplica(0)));
    assertTrue(watcher.isClientReadable(watcher.getMasterAddressZNode()));
    assertTrue(watcher.isClientReadable(watcher.clusterIdZNode));
    assertTrue(watcher.isClientReadable(watcher.tableZNode));
    assertTrue(watcher.isClientReadable(ZKUtil.joinZNode(watcher.tableZNode, "foo")));
    assertTrue(watcher.isClientReadable(watcher.rsZNode));


    assertFalse(watcher.isClientReadable(watcher.tableLockZNode));
    assertFalse(watcher.isClientReadable(watcher.balancerZNode));
    assertFalse(watcher.isClientReadable(watcher.clusterStateZNode));
    assertFalse(watcher.isClientReadable(watcher.drainingZNode));
    assertFalse(watcher.isClientReadable(watcher.recoveringRegionsZNode));
    assertFalse(watcher.isClientReadable(watcher.splitLogZNode));
    assertFalse(watcher.isClientReadable(watcher.backupMasterAddressesZNode));

    watcher.close();
  }
  
  @Test
  public void testConnectionEvent() throws ZooKeeperConnectionException, IOException {
    long zkSessionTimeout = 15000l;
    Configuration conf = HBaseConfiguration.create();
    conf.set("zookeeper.session.timeout", "15000");

    Abortable abortable = new Abortable() {
      boolean aborted = false;

      @Override
      public void abort(String why, Throwable e) {
        aborted = true;
        LOG.error(why, e);
      }

      @Override
      public boolean isAborted() {
        return aborted;
      }
    };
    ZooKeeperWatcher watcher = new ZooKeeperWatcher(conf, "testConnectionEvent", abortable, false);

    WatchedEvent event =
        new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected, null);

    long startTime = EnvironmentEdgeManager.currentTime();
    while (!abortable.isAborted()
        && (EnvironmentEdgeManager.currentTime() - startTime < zkSessionTimeout)) {
      watcher.process(event);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    assertTrue(abortable.isAborted());
    watcher.close();
  }
}
