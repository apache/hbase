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
package org.apache.hadoop.hbase.client.replication;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfigBuilder;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
public class TestBadReplicationPeer {
  private static final Logger LOG = LoggerFactory.getLogger(TestBadReplicationPeer.class);
  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static Configuration conf;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    TEST_UTIL.getConfiguration().setBoolean("replication.source.regionserver.abort", false);
    TEST_UTIL.startMiniCluster();
    conf = TEST_UTIL.getConfiguration();
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /*
   * Add dummy peer and make sure that we are able to remove that peer.
   */
  @Test
  public void testRemovePeerSucceeds() throws IOException {
    String peerId = "dummypeer_1";
    try (Connection connection = ConnectionFactory.createConnection(conf);
      Admin admin = connection.getAdmin()) {
      ReplicationPeerConfigBuilder rpcBuilder = ReplicationPeerConfig.newBuilder();
      String quorum = TEST_UTIL.getHBaseCluster().getMaster().getZooKeeper().getQuorum();
      rpcBuilder.setClusterKey(quorum + ":/1");
      ReplicationPeerConfig rpc = rpcBuilder.build();
      admin.addReplicationPeer(peerId, rpc);
      LOG.info("Added replication peer with peer id: {}", peerId);
    } finally {
      LOG.info("Removing replication peer with peer id: {}", peerId);
      cleanPeer(peerId);
    }
  }

  private static void cleanPeer(String peerId) throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(conf);
      Admin admin = connection.getAdmin()) {
      admin.removeReplicationPeer(peerId);
    }
  }
}
