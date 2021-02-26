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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Testcase for HBASE-20296.
 */
@Category({ ReplicationTests.class, MediumTests.class })
public class TestRemoveFromSerialReplicationPeer extends SerialReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRemoveFromSerialReplicationPeer.class);

  @Before
  public void setUp() throws IOException, StreamLacksCapabilityException {
    setupWALWriter();
  }

  private void waitUntilHasLastPushedSequenceId(RegionInfo region) throws Exception {
    ReplicationQueueStorage queueStorage =
      UTIL.getMiniHBaseCluster().getMaster().getReplicationPeerManager().getQueueStorage();
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return queueStorage.getLastSequenceId(region.getEncodedName(), PEER_ID) > 0;
      }

      @Override
      public String explainFailure() throws Exception {
        return "Still no last pushed sequence id for " + region;
      }
    });
  }

  @Test
  public void testRemoveTable() throws Exception {
    TableName tableName = createTable();
    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setClusterKey("127.0.0.1:2181:/hbase")
      .setReplicationEndpointImpl(LocalReplicationEndpoint.class.getName())
      .setReplicateAllUserTables(false)
      .setTableCFsMap(ImmutableMap.of(tableName, Collections.emptyList())).setSerial(true).build();
    UTIL.getAdmin().addReplicationPeer(PEER_ID, peerConfig, true);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    RegionInfo region = UTIL.getMiniHBaseCluster().getRegions(tableName).get(0).getRegionInfo();
    waitUntilHasLastPushedSequenceId(region);

    UTIL.getAdmin().updateReplicationPeerConfig(PEER_ID,
      ReplicationPeerConfig.newBuilder(peerConfig).setTableCFsMap(Collections.emptyMap()).build());

    ReplicationQueueStorage queueStorage =
      UTIL.getMiniHBaseCluster().getMaster().getReplicationPeerManager().getQueueStorage();
    assertEquals(HConstants.NO_SEQNUM,
      queueStorage.getLastSequenceId(region.getEncodedName(), PEER_ID));
  }

  @Test
  public void testRemoveSerialFlag() throws Exception {
    TableName tableName = createTable();
    addPeer(true);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    RegionInfo region = UTIL.getMiniHBaseCluster().getRegions(tableName).get(0).getRegionInfo();
    waitUntilHasLastPushedSequenceId(region);
    UTIL.getAdmin().updateReplicationPeerConfig(PEER_ID, ReplicationPeerConfig
      .newBuilder(UTIL.getAdmin().getReplicationPeerConfig(PEER_ID)).setSerial(false).build());
    waitUntilReplicationDone(100);

    ReplicationQueueStorage queueStorage =
      UTIL.getMiniHBaseCluster().getMaster().getReplicationPeerManager().getQueueStorage();
    assertEquals(HConstants.NO_SEQNUM,
      queueStorage.getLastSequenceId(region.getEncodedName(), PEER_ID));
  }
}
