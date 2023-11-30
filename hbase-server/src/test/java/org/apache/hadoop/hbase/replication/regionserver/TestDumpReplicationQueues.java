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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfigBuilder;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Tests for DumpReplicationQueues tool
 */
@Category({ ReplicationTests.class, SmallTests.class })
public class TestDumpReplicationQueues {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDumpReplicationQueues.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static Configuration CONF;
  private static FileSystem FS = null;
  private Path root;
  private Path logDir;
  @Rule
  public final TestName name = new TestName();

  @Before
  public void setup() throws Exception {
    UTIL.startMiniCluster(3);
    CONF = UTIL.getConfiguration();
    TableName tableName = TableName.valueOf("replication_" + name.getMethodName());
    UTIL.getAdmin()
      .createTable(ReplicationStorageFactory.createReplicationQueueTableDescriptor(tableName));
    CONF.set(ReplicationStorageFactory.REPLICATION_QUEUE_TABLE_NAME, tableName.getNameAsString());
    FS = FileSystem.get(CONF);
    root = UTIL.getDataTestDirOnTestFS("hbase");
    logDir = new Path(root, HConstants.HREGION_LOGDIR_NAME);
    FS.mkdirs(logDir);
    CommonFSUtils.setRootDir(CONF, root);
    CommonFSUtils.setWALRootDir(CONF, root);
  }

  @Test
  public void testDumpReplication() throws Exception {
    String peerId = "1";
    String serverNameStr = "rs1,12345,123";
    addPeer(peerId, "hbase");
    ServerName serverName = ServerName.valueOf(serverNameStr);
    String walName = "rs1%2C12345%2C123.10";
    Path walPath = new Path(logDir, serverNameStr + "/" + walName);
    FS.createNewFile(walPath);

    ReplicationQueueId queueId = new ReplicationQueueId(serverName, peerId);
    ReplicationQueueStorage queueStorage =
      ReplicationStorageFactory.getReplicationQueueStorage(UTIL.getConnection(), CONF);
    queueStorage.setOffset(queueId, "wal-group",
      new ReplicationGroupOffset(FS.listStatus(walPath)[0].getPath().toString(), 123),
      Collections.emptyMap());

    DumpReplicationQueues dumpQueues = new DumpReplicationQueues();
    Set<String> peerIds = new HashSet<>();
    peerIds.add(peerId);
    List<String> wals = new ArrayList<>();
    wals.add("rs1%2C12345%2C123.12");
    wals.add("rs1%2C12345%2C123.15");
    wals.add("rs1%2C12345%2C123.11");
    for (String wal : wals) {
      Path wPath = new Path(logDir, serverNameStr + "/" + wal);
      FS.createNewFile(wPath);
    }

    String dump = dumpQueues.dumpQueues(UTIL.getConnection(), peerIds, false, CONF);
    assertTrue(dump.indexOf("Queue id: 1-rs1,12345,123") > 0);
    assertTrue(dump.indexOf("Number of WALs in replication queue: 4") > 0);
    // test for 'Returns wal sorted'
    String[] parsedDump = dump.split("Replication position for");
    assertTrue("First wal should be rs1%2C12345%2C123.10: 123, but got: " + parsedDump[1],
      parsedDump[1].indexOf("rs1%2C12345%2C123.10: 123") >= 0);
    assertTrue("Second wal should be rs1%2C12345%2C123.11: 0, but got: " + parsedDump[2],
      parsedDump[2].indexOf("rs1%2C12345%2C123.11: 0 (not started or nothing to replicate)") >= 0);
    assertTrue("Third wal should be rs1%2C12345%2C123.12: 0, but got: " + parsedDump[3],
      parsedDump[3].indexOf("rs1%2C12345%2C123.12: 0 (not started or nothing to replicate)") >= 0);
    assertTrue("Fourth wal should be rs1%2C12345%2C123.15: 0, but got: " + parsedDump[4],
      parsedDump[4].indexOf("rs1%2C12345%2C123.15: 0 (not started or nothing to replicate)") >= 0);

    Path file1 = new Path("testHFile1");
    Path file2 = new Path("testHFile2");
    List<Pair<Path, Path>> files = new ArrayList<>(1);
    files.add(new Pair<>(null, file1));
    files.add(new Pair<>(null, file2));
    queueStorage.addHFileRefs(peerId, files);
    // test for 'Dump Replication via replication table'
    String dump2 = dumpQueues.dumpReplicationViaTable(UTIL.getConnection(), CONF);
    assertTrue(dump2.indexOf("peers/1/peer-state: ENABLED") > 0);
    assertTrue(dump2.indexOf("rs1,12345,123/rs1%2C12345%2C123.10: 123") >= 0);
    assertTrue(dump2.indexOf("hfile-refs/1/testHFile1,testHFile2") >= 0);
  }

  /**
   * Add a peer
   */
  private void addPeer(String peerId, String clusterKey) throws IOException {
    ReplicationPeerConfigBuilder builder = ReplicationPeerConfig.newBuilder()
      .setClusterKey(UTIL.getZkCluster().getAddress().toString() + ":/" + clusterKey)
      .setReplicationEndpointImpl(
        TestReplicationSourceManager.ReplicationEndpointForTest.class.getName());
    UTIL.getAdmin().addReplicationPeer(peerId, builder.build(), true);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }
}
