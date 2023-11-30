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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerStorage;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.HbckErrorReporter.ERROR_CODE;
import org.apache.hadoop.hbase.util.hbck.HbckTestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestHBaseFsckReplication {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHBaseFsckReplication.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  @Rule
  public final TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    UTIL.getConfiguration().setBoolean("hbase.write.hbck1.lock.file", false);
    UTIL.startMiniCluster(1);
    TableName tableName = TableName.valueOf("replication_" + name.getMethodName());
    UTIL.getAdmin()
      .createTable(ReplicationStorageFactory.createReplicationQueueTableDescriptor(tableName));
    UTIL.getConfiguration().set(ReplicationStorageFactory.REPLICATION_QUEUE_TABLE_NAME,
      tableName.getNameAsString());
  }

  @After
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    ReplicationPeerStorage peerStorage = ReplicationStorageFactory.getReplicationPeerStorage(
      UTIL.getTestFileSystem(), UTIL.getZooKeeperWatcher(), UTIL.getConfiguration());
    ReplicationQueueStorage queueStorage = ReplicationStorageFactory
      .getReplicationQueueStorage(UTIL.getConnection(), UTIL.getConfiguration());

    String peerId1 = "1";
    String peerId2 = "2";
    peerStorage.addPeer(peerId1, ReplicationPeerConfig.newBuilder().setClusterKey("key").build(),
      true, SyncReplicationState.NONE);
    peerStorage.addPeer(peerId2, ReplicationPeerConfig.newBuilder().setClusterKey("key").build(),
      true, SyncReplicationState.NONE);
    ReplicationQueueId queueId = null;
    for (int i = 0; i < 10; i++) {
      queueId = new ReplicationQueueId(getServerName(i), peerId1);
      queueStorage.setOffset(queueId, "group-" + i,
        new ReplicationGroupOffset("file-" + i, i * 100), Collections.emptyMap());
    }
    queueId = new ReplicationQueueId(getServerName(0), peerId2);
    queueStorage.setOffset(queueId, "group-" + 0, new ReplicationGroupOffset("file-" + 0, 100),
      Collections.emptyMap());
    HBaseFsck fsck = HbckTestingUtil.doFsck(UTIL.getConfiguration(), true);
    HbckTestingUtil.assertNoErrors(fsck);

    // should not remove anything since the replication peer is still alive
    assertEquals(10, queueStorage.listAllReplicators().size());
    peerStorage.removePeer(peerId1);
    // there should be orphan queues
    assertEquals(10, queueStorage.listAllReplicators().size());
    fsck = HbckTestingUtil.doFsck(UTIL.getConfiguration(), false);
    HbckTestingUtil.assertErrors(fsck, Stream.generate(() -> {
      return ERROR_CODE.UNDELETED_REPLICATION_QUEUE;
    }).limit(10).toArray(ERROR_CODE[]::new));

    // should not delete anything when fix is false
    assertEquals(10, queueStorage.listAllReplicators().size());

    fsck = HbckTestingUtil.doFsck(UTIL.getConfiguration(), true);
    HbckTestingUtil.assertErrors(fsck, Stream.generate(() -> {
      return ERROR_CODE.UNDELETED_REPLICATION_QUEUE;
    }).limit(10).toArray(HbckErrorReporter.ERROR_CODE[]::new));

    List<ServerName> replicators = queueStorage.listAllReplicators();
    // should not remove the server with queue for peerId2
    assertEquals(1, replicators.size());
    assertEquals(ServerName.valueOf("localhost", 10000, 100000), replicators.get(0));
    for (ReplicationQueueId qId : queueStorage.listAllQueueIds(replicators.get(0))) {
      assertEquals(peerId2, qId.getPeerId());
    }
  }

  private ServerName getServerName(int i) {
    return ServerName.valueOf("localhost", 10000 + i, 100000 + i);
  }
}
