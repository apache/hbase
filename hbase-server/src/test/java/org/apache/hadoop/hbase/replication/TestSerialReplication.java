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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestSerialReplication extends SerialReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSerialReplication.class);

  @Before
  public void setUp() throws IOException, StreamLacksCapabilityException {
    setupWALWriter();
    // add in disable state, so later when enabling it all sources will start push together.
    addPeer(false);
  }

  @Test
  public void testRegionMove() throws Exception {
    TableName tableName = createTable();
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    RegionInfo region = UTIL.getAdmin().getRegions(tableName).get(0);
    HRegionServer rs = UTIL.getOtherRegionServer(UTIL.getRSForFirstRegionInTable(tableName));
    moveRegion(region, rs);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 100; i < 200; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    enablePeerAndWaitUntilReplicationDone(200);
    checkOrder(200);
  }

  @Test
  public void testRegionSplit() throws Exception {
    TableName tableName = createTable();
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    UTIL.flush(tableName);
    RegionInfo region = UTIL.getAdmin().getRegions(tableName).get(0);
    UTIL.getAdmin().splitRegionAsync(region.getEncodedNameAsBytes(), Bytes.toBytes(50)).get(30,
      TimeUnit.SECONDS);
    UTIL.waitUntilNoRegionsInTransition(30000);
    List<RegionInfo> regions = UTIL.getAdmin().getRegions(tableName);
    assertEquals(2, regions.size());
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    enablePeerAndWaitUntilReplicationDone(200);
    Map<String, Long> regionsToSeqId = new HashMap<>();
    regionsToSeqId.put(region.getEncodedName(), -1L);
    regions.stream().map(RegionInfo::getEncodedName).forEach(n -> regionsToSeqId.put(n, -1L));
    try (WAL.Reader reader =
      WALFactory.createReader(UTIL.getTestFileSystem(), logPath, UTIL.getConfiguration())) {
      int count = 0;
      for (Entry entry;;) {
        entry = reader.next();
        if (entry == null) {
          break;
        }
        String encodedName = Bytes.toString(entry.getKey().getEncodedRegionName());
        Long seqId = regionsToSeqId.get(encodedName);
        assertNotNull(
          "Unexcepted entry " + entry + ", expected regions " + region + ", or " + regions, seqId);
        assertTrue("Sequence id go backwards from " + seqId + " to " +
          entry.getKey().getSequenceId() + " for " + encodedName,
          entry.getKey().getSequenceId() >= seqId.longValue());
        if (count < 100) {
          assertEquals(encodedName + " is pushed before parent " + region.getEncodedName(),
            region.getEncodedName(), encodedName);
        } else {
          assertNotEquals(region.getEncodedName(), encodedName);
        }
        count++;
      }
      assertEquals(200, count);
    }
  }

  @Test
  public void testRegionMerge() throws Exception {
    byte[] splitKey = Bytes.toBytes(50);
    TableName tableName = TableName.valueOf(name.getMethodName());
    UTIL.getAdmin().createTable(
      TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF)
          .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .build(),
      new byte[][] { splitKey });
    UTIL.waitTableAvailable(tableName);
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    List<RegionInfo> regions = UTIL.getAdmin().getRegions(tableName);
    UTIL.getAdmin()
      .mergeRegionsAsync(
        regions.stream().map(RegionInfo::getEncodedNameAsBytes).toArray(byte[][]::new), false)
      .get(30, TimeUnit.SECONDS);
    UTIL.waitUntilNoRegionsInTransition(30000);
    List<RegionInfo> regionsAfterMerge = UTIL.getAdmin().getRegions(tableName);
    assertEquals(1, regionsAfterMerge.size());
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    enablePeerAndWaitUntilReplicationDone(200);
    Map<String, Long> regionsToSeqId = new HashMap<>();
    RegionInfo region = regionsAfterMerge.get(0);
    regionsToSeqId.put(region.getEncodedName(), -1L);
    regions.stream().map(RegionInfo::getEncodedName).forEach(n -> regionsToSeqId.put(n, -1L));
    try (WAL.Reader reader =
      WALFactory.createReader(UTIL.getTestFileSystem(), logPath, UTIL.getConfiguration())) {
      int count = 0;
      for (Entry entry;;) {
        entry = reader.next();
        if (entry == null) {
          break;
        }
        String encodedName = Bytes.toString(entry.getKey().getEncodedRegionName());
        Long seqId = regionsToSeqId.get(encodedName);
        assertNotNull(
          "Unexcepted entry " + entry + ", expected regions " + region + ", or " + regions, seqId);
        assertTrue("Sequence id go backwards from " + seqId + " to " +
          entry.getKey().getSequenceId() + " for " + encodedName,
          entry.getKey().getSequenceId() >= seqId.longValue());
        if (count < 100) {
          assertNotEquals(
            encodedName + " is pushed before parents " +
              regions.stream().map(RegionInfo::getEncodedName).collect(Collectors.joining(" and ")),
            region.getEncodedName(), encodedName);
        } else {
          assertEquals(region.getEncodedName(), encodedName);
        }
        count++;
      }
      assertEquals(200, count);
    }
  }

  @Test
  public void testRemovePeerNothingReplicated() throws Exception {
    TableName tableName = createTable();
    String encodedRegionName =
      UTIL.getMiniHBaseCluster().getRegions(tableName).get(0).getRegionInfo().getEncodedName();
    ReplicationQueueStorage queueStorage =
      UTIL.getMiniHBaseCluster().getMaster().getReplicationPeerManager().getQueueStorage();
    assertEquals(HConstants.NO_SEQNUM, queueStorage.getLastSequenceId(encodedRegionName, PEER_ID));
    UTIL.getAdmin().removeReplicationPeer(PEER_ID);
    assertEquals(HConstants.NO_SEQNUM, queueStorage.getLastSequenceId(encodedRegionName, PEER_ID));
  }

  @Test
  public void testRemovePeer() throws Exception {
    TableName tableName = createTable();
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    enablePeerAndWaitUntilReplicationDone(100);
    checkOrder(100);
    String encodedRegionName =
      UTIL.getMiniHBaseCluster().getRegions(tableName).get(0).getRegionInfo().getEncodedName();
    ReplicationQueueStorage queueStorage =
      UTIL.getMiniHBaseCluster().getMaster().getReplicationPeerManager().getQueueStorage();
    assertTrue(queueStorage.getLastSequenceId(encodedRegionName, PEER_ID) > 0);
    UTIL.getAdmin().removeReplicationPeer(PEER_ID);
    // confirm that we delete the last pushed sequence id
    assertEquals(HConstants.NO_SEQNUM, queueStorage.getLastSequenceId(encodedRegionName, PEER_ID));
  }

  @Test
  public void testRemoveSerialFlag() throws Exception {
    TableName tableName = createTable();
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    enablePeerAndWaitUntilReplicationDone(100);
    checkOrder(100);
    String encodedRegionName =
      UTIL.getMiniHBaseCluster().getRegions(tableName).get(0).getRegionInfo().getEncodedName();
    ReplicationQueueStorage queueStorage =
      UTIL.getMiniHBaseCluster().getMaster().getReplicationPeerManager().getQueueStorage();
    assertTrue(queueStorage.getLastSequenceId(encodedRegionName, PEER_ID) > 0);
    ReplicationPeerConfig peerConfig = UTIL.getAdmin().getReplicationPeerConfig(PEER_ID);
    UTIL.getAdmin().updateReplicationPeerConfig(PEER_ID,
      ReplicationPeerConfig.newBuilder(peerConfig).setSerial(false).build());
    // confirm that we delete the last pushed sequence id
    assertEquals(HConstants.NO_SEQNUM, queueStorage.getLastSequenceId(encodedRegionName, PEER_ID));
  }
}
