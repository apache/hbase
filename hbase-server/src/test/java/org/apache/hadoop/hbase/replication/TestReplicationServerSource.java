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
package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestReplicationServerSource extends TestReplicationBase {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationServerSource.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationServerSource.class);

  private static HReplicationServer replicationServer;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL1.getConfiguration().setBoolean(HConstants.REPLICATION_OFFLOAD_ENABLE_KEY, true);
    TestReplicationBase.setUpBeforeClass();
    replicationServer = new HReplicationServer(UTIL1.getConfiguration());
    replicationServer.start();
    UTIL1.waitFor(60000, () -> replicationServer.isOnline());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    replicationServer.stop("Tear down after test");
    TestReplicationBase.tearDownAfterClass();
  }

  @Test
  public void test() throws Exception {
    try {
      // Only start one region server in source cluster
      ServerName producer = UTIL1.getMiniHBaseCluster().getRegionServer(0).getServerName();
      replicationServer.startReplicationSource(producer, PEER_ID2);
    } catch (Throwable e) {
      LOG.info("Failed to start replicaiton source", e);
    }
    runSmallBatchTest();
  }
}
