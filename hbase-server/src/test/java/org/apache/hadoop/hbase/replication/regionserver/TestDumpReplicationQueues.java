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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for DumpReplicationQueues tool
 */
@Category({ ReplicationTests.class, SmallTests.class})
public class TestDumpReplicationQueues {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDumpReplicationQueues.class);

  /**
   * Makes sure dumpQueues returns wals znodes ordered chronologically.
   * @throws Exception if dumpqueues finds any error while handling list of znodes.
   */
  @Test
  public void testDumpReplicationReturnsWalSorted() throws Exception {
    Configuration config = HBaseConfiguration.create();
    ZKWatcher zkWatcherMock = mock(ZKWatcher.class);
    ZNodePaths zNodePath = new ZNodePaths(config);
    RecoverableZooKeeper recoverableZooKeeperMock = mock(RecoverableZooKeeper.class);
    when(zkWatcherMock.getRecoverableZooKeeper()).thenReturn(recoverableZooKeeperMock);
    when(zkWatcherMock.getZNodePaths()).thenReturn(zNodePath);
    List<String> nodes = new ArrayList<>();
    String server = "rs1,60030,"+System.currentTimeMillis();
    nodes.add(server);
    when(recoverableZooKeeperMock.getChildren("/hbase/rs", null)).thenReturn(nodes);
    when(recoverableZooKeeperMock.getChildren("/hbase/replication/rs", null)).
        thenReturn(nodes);
    List<String> queuesIds = new ArrayList<>();
    queuesIds.add("1");
    when(recoverableZooKeeperMock.getChildren("/hbase/replication/rs/"+server, null)).
        thenReturn(queuesIds);
    List<String> wals = new ArrayList<>();
    wals.add("rs1%2C60964%2C1549394085556.1549394101427");
    wals.add("rs1%2C60964%2C1549394085556.1549394101426");
    wals.add("rs1%2C60964%2C1549394085556.1549394101428");
    when(recoverableZooKeeperMock.getChildren("/hbase/replication/rs/"+server+"/1",
        null)).thenReturn(wals);
    DumpReplicationQueues dumpQueues = new DumpReplicationQueues();
    Set<String> peerIds = new HashSet<>();
    peerIds.add("1");
    dumpQueues.setConf(config);
    String dump = dumpQueues.dumpQueues(zkWatcherMock, peerIds, false);
    String[] parsedDump = dump.split("Replication position for");
    assertEquals("Parsed dump should have 4 parts.", 4, parsedDump.length);
    assertTrue("First wal should be rs1%2C60964%2C1549394085556.1549394101426, but got: "
        + parsedDump[1],
        parsedDump[1].indexOf("rs1%2C60964%2C1549394085556.1549394101426")>=0);
    assertTrue("Second wal should be rs1%2C60964%2C1549394085556.1549394101427, but got: "
            + parsedDump[2],
        parsedDump[2].indexOf("rs1%2C60964%2C1549394085556.1549394101427")>=0);
    assertTrue("Third wal should be rs1%2C60964%2C1549394085556.1549394101428, but got: "
            + parsedDump[3],
        parsedDump[3].indexOf("rs1%2C60964%2C1549394085556.1549394101428")>=0);
  }

}
