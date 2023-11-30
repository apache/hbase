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

import static org.apache.hadoop.hbase.replication.ReplicationPeerConfigTestUtil.assertConfigEquals;
import static org.apache.hadoop.hbase.replication.ReplicationPeerConfigTestUtil.getConfig;
import static org.junit.Assert.assertEquals;

import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseZKTestingUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestCopyReplicationPeers {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCopyReplicationPeers.class);

  private static final HBaseZKTestingUtil UTIL = new HBaseZKTestingUtil();

  private static FileSystem FS;

  private static Path DIR;

  private static ReplicationPeerStorage SRC;

  private static ReplicationPeerStorage DST;

  @BeforeClass
  public static void setUp() throws Exception {
    DIR = UTIL.getDataTestDir("test_peer_migration");
    CommonFSUtils.setRootDir(UTIL.getConfiguration(), DIR);
    FS = FileSystem.get(UTIL.getConfiguration());
    UTIL.startMiniZKCluster();
    SRC = new ZKReplicationPeerStorage(UTIL.getZooKeeperWatcher(), UTIL.getConfiguration());
    DST = new FSReplicationPeerStorage(FS, UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniZKCluster();
    UTIL.cleanupTestDir();
  }

  @Test
  public void testMigrate() throws Exception {
    // invalid args
    assertEquals(-1,
      ToolRunner.run(new CopyReplicationPeers(UTIL.getConfiguration()), new String[] {}));
    int peerCount = 10;
    for (int i = 0; i < peerCount; i++) {
      SRC.addPeer(Integer.toString(i), getConfig(i), i % 2 == 0,
        SyncReplicationState.valueOf(i % 4));
    }
    // migrate
    assertEquals(0, ToolRunner.run(new CopyReplicationPeers(UTIL.getConfiguration()),
      new String[] { SRC.getClass().getName(), DST.getClass().getName() }));
    // verify the replication peer data in dst storage
    List<String> peerIds = DST.listPeerIds();
    assertEquals(peerCount, peerIds.size());
    for (String peerId : peerIds) {
      int seed = Integer.parseInt(peerId);
      assertConfigEquals(getConfig(seed), DST.getPeerConfig(peerId));
    }
  }
}
