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

package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationQueuesArguments;
import org.apache.hadoop.hbase.replication.ReplicationQueuesZKImpl;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestReplicationZKNodeCleaner {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final String ID_ONE = "1";
  private final String SERVER_ONE = "server1";
  private final String ID_TWO = "2";
  private final String SERVER_TWO = "server2";

  private final Configuration conf;
  private final ZooKeeperWatcher zkw;
  private final ReplicationQueues repQueues;

  public TestReplicationZKNodeCleaner() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    zkw = new ZooKeeperWatcher(conf, "TestReplicationZKNodeCleaner", null);
    repQueues = ReplicationFactory.getReplicationQueues(new ReplicationQueuesArguments(conf, null,
        zkw));
    assertTrue(repQueues instanceof ReplicationQueuesZKImpl);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.cleaner.interval", 10000);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testReplicationZKNodeCleaner() throws Exception {
    repQueues.init(SERVER_ONE);
    // add queue for ID_ONE which isn't exist
    repQueues.addLog(ID_ONE, "file1");

    ReplicationZKNodeCleaner cleaner = new ReplicationZKNodeCleaner(conf, zkw, null);
    Map<String, List<String>> undeletedQueues = cleaner.getUnDeletedQueues();
    assertEquals(1, undeletedQueues.size());
    assertTrue(undeletedQueues.containsKey(SERVER_ONE));
    assertEquals(1, undeletedQueues.get(SERVER_ONE).size());
    assertTrue(undeletedQueues.get(SERVER_ONE).contains(ID_ONE));

    // add a recovery queue for ID_TWO which isn't exist
    repQueues.addLog(ID_TWO + "-" + SERVER_TWO, "file2");

    undeletedQueues = cleaner.getUnDeletedQueues();
    assertEquals(1, undeletedQueues.size());
    assertTrue(undeletedQueues.containsKey(SERVER_ONE));
    assertEquals(2, undeletedQueues.get(SERVER_ONE).size());
    assertTrue(undeletedQueues.get(SERVER_ONE).contains(ID_ONE));
    assertTrue(undeletedQueues.get(SERVER_ONE).contains(ID_TWO + "-" + SERVER_TWO));

    cleaner.removeQueues(undeletedQueues);
    undeletedQueues = cleaner.getUnDeletedQueues();
    assertEquals(0, undeletedQueues.size());
  }

  @Test
  public void testReplicationZKNodeCleanerChore() throws Exception {
    repQueues.init(SERVER_ONE);
    // add queue for ID_ONE which isn't exist
    repQueues.addLog(ID_ONE, "file1");
    // add a recovery queue for ID_TWO which isn't exist
    repQueues.addLog(ID_TWO + "-" + SERVER_TWO, "file2");

    // Wait the cleaner chore to run
    Thread.sleep(20000);

    ReplicationZKNodeCleaner cleaner = new ReplicationZKNodeCleaner(conf, zkw, null);
    assertEquals(0, cleaner.getUnDeletedQueues().size());
  }
}
