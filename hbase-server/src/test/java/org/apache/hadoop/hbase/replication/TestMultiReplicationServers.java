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

import static org.apache.hadoop.hbase.master.ReplicationServerManager.REPLICATION_SERVER_REFRESH_PERIOD;
import static org.apache.hadoop.hbase.replication.ReplicationServerRpcServices.REPLICATION_SERVER_PORT;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
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
public class TestMultiReplicationServers extends TestReplicationBase{
  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMultiReplicationServers.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMultiReplicationServers.class);

  private static HReplicationServer[] replicationServers;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // 3 RegionServers and 3 ReplicationServers
    NUM_SLAVES1 = 3;
    UTIL1.getConfiguration().setInt(REPLICATION_SERVER_PORT, 0);
    UTIL1.getConfiguration().setBoolean(HConstants.REPLICATION_OFFLOAD_ENABLE_KEY, true);
    UTIL1.getConfiguration().setLong(REPLICATION_SERVER_REFRESH_PERIOD, 10000);
    TestReplicationBase.setUpBeforeClass();
    replicationServers = new HReplicationServer[NUM_SLAVES1];
    for (int i = 0; i < NUM_SLAVES1; i++) {
      replicationServers[i] = new HReplicationServer(UTIL1.getConfiguration());
      replicationServers[i].start();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    for (int i = 0; i < NUM_SLAVES1; i++) {
      replicationServers[i].stop("Tear down after test");
    }
    TestReplicationBase.tearDownAfterClass();
  }

  @Test
  public void test() throws Exception {
    runSmallBatchTest();
  }
}
