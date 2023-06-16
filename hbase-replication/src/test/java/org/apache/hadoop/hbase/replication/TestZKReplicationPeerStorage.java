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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseZKTestingUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestZKReplicationPeerStorage extends ReplicationPeerStorageTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestZKReplicationPeerStorage.class);

  private static final HBaseZKTestingUtil UTIL = new HBaseZKTestingUtil();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniZKCluster();
    STORAGE = new ZKReplicationPeerStorage(UTIL.getZooKeeperWatcher(), UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniZKCluster();
  }

  @Override
  protected void removePeerSyncRelicationState(String peerId) throws Exception {
    ZKReplicationPeerStorage storage = (ZKReplicationPeerStorage) STORAGE;
    ZKUtil.deleteNode(UTIL.getZooKeeperWatcher(), storage.getSyncReplicationStateNode(peerId));
    ZKUtil.deleteNode(UTIL.getZooKeeperWatcher(), storage.getNewSyncReplicationStateNode(peerId));
  }

  @Override
  protected void assertPeerSyncReplicationStateCreate(String peerId) throws Exception {
    ZKReplicationPeerStorage storage = (ZKReplicationPeerStorage) STORAGE;
    assertNotEquals(-1,
      ZKUtil.checkExists(UTIL.getZooKeeperWatcher(), storage.getSyncReplicationStateNode(peerId)));
    assertNotEquals(-1, ZKUtil.checkExists(UTIL.getZooKeeperWatcher(),
      storage.getNewSyncReplicationStateNode(peerId)));
  }

  @Override
  protected void assertPeerNameControlException(ReplicationException e) {
    assertThat(e.getCause(), instanceOf(KeeperException.NodeExistsException.class));
  }
}
