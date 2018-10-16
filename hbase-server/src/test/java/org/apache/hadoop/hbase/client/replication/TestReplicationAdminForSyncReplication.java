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
package org.apache.hadoop.hbase.client.replication;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfigBuilder;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MediumTests.class, ClientTests.class})
public class TestReplicationAdminForSyncReplication {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationAdminForSyncReplication.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestReplicationAdminForSyncReplication.class);

  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private static Admin hbaseAdmin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    TEST_UTIL.startMiniCluster();
    hbaseAdmin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    hbaseAdmin.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testAddPeerWithSameTable() throws Exception {
    TableName tableName = TableName.valueOf("testAddPeerWithSameTable");
    TEST_UTIL.createTable(tableName, Bytes.toBytes("family"));

    boolean[] success = { true, true, true, true, true, true };
    Thread[] threads = new Thread[5];
    for (int i = 0; i < 5; i++) {
      String peerId = "id" + i;
      String clusterKey = "127.0.0.1:2181:/hbase" + i;
      int index = i;
      threads[i] = new Thread(() -> {
        try {
          hbaseAdmin
              .addReplicationPeer(peerId, buildSyncReplicationPeerConfig(clusterKey, tableName));
        } catch (IOException e) {
          LOG.error("Failed to add replication peer " + peerId);
          success[index] = false;
        }
      });
    }
    for (int i = 0; i < 5; i++) {
      threads[i].start();
    }
    for (int i = 0; i < 5; i++) {
      threads[i].join();
    }

    int successCount = 0;
    for (int i = 0; i < 5; i++) {
      if (success[i]) {
        successCount++;
      }
    }
    assertEquals("Only one peer can be added successfully", 1, successCount);
  }

  private ReplicationPeerConfig buildSyncReplicationPeerConfig(String clusterKey,
      TableName tableName) throws IOException {
    Path rootDir = TEST_UTIL.getDataTestDirOnTestFS("remoteWAL");
    ReplicationPeerConfigBuilder builder = ReplicationPeerConfig.newBuilder();
    builder.setClusterKey(clusterKey);
    builder.setRemoteWALDir(rootDir.makeQualified(TEST_UTIL.getTestFileSystem().getUri(),
        TEST_UTIL.getTestFileSystem().getWorkingDirectory()).toString());
    builder.setReplicateAllUserTables(false);
    Map<TableName, List<String>> tableCfs = new HashMap<>();
    tableCfs.put(tableName, new ArrayList<>());
    builder.setTableCFsMap(tableCfs);
    return builder.build();
  }
}
