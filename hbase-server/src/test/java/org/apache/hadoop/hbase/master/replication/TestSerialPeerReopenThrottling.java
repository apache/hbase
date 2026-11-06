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
package org.apache.hadoop.hbase.master.replication;

import static org.apache.hadoop.hbase.master.procedure.ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY;
import static org.apache.hadoop.hbase.master.procedure.ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_SIZE_MAX_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.procedure.ReopenTableRegionsProcedure;
import org.apache.hadoop.hbase.replication.DummyReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestSerialPeerReopenThrottling {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final TableName TABLE_NAME = TableName.valueOf("serialPeerReopen");
  private static final byte[] CF = Bytes.toBytes("cf");
  private static final int REGION_COUNT = 5;

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(PROGRESSIVE_BATCH_SIZE_MAX_KEY, 1);
    UTIL.getConfiguration().setLong(PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY, 500);
    UTIL.startMiniCluster(1);
    UTIL.getAdmin().createTable(
      TableDescriptorBuilder.newBuilder(TABLE_NAME)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF)
          .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .build(),
      Bytes.toBytes("a"), Bytes.toBytes("z"), REGION_COUNT);
    UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testChangeToSerialReopensRegionsInBatches() throws Exception {
    Admin admin = UTIL.getAdmin();
    ReplicationPeerConfig peerConfig = peerConfig(false);
    admin.addReplicationPeer("changeToSerial", peerConfig, true);

    assertReopenedInBatches(admin.updateReplicationPeerConfigAsync("changeToSerial",
      ReplicationPeerConfig.newBuilder(peerConfig).setSerial(true).build()));
  }

  @Test
  public void testAddSerialPeerReopensRegionsInBatches() throws Exception {
    assertReopenedInBatches(
      UTIL.getAdmin().addReplicationPeerAsync("addSerial", peerConfig(true), true));
  }

  private static ReplicationPeerConfig peerConfig(boolean serial) {
    return ReplicationPeerConfig.newBuilder()
      .setReplicationEndpointImpl(DummyReplicationEndpoint.class.getName())
      .setReplicateAllUserTables(false)
      .setTableCFsMap(Collections.singletonMap(TABLE_NAME, Collections.emptyList()))
      .setSerial(serial).build();
  }

  private static void assertReopenedInBatches(Future<Void> peerChange) throws Exception {
    // Completed child procedures are not retained, so capture the reopen while it runs.
    AtomicReference<ReopenTableRegionsProcedure> reopen = new AtomicReference<>();
    UTIL.waitFor(30_000, 10, () -> {
      findReopen().ifPresent(reopen::set);
      return reopen.get() != null;
    });
    peerChange.get(60, TimeUnit.SECONDS);

    assertEquals(REGION_COUNT, reopen.get().getRegionsReopened());
    assertEquals(REGION_COUNT, reopen.get().getBatchesProcessed());
  }

  private static Optional<ReopenTableRegionsProcedure> findReopen() {
    return UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getProcedures()
      .stream().filter(ReopenTableRegionsProcedure.class::isInstance)
      .map(ReopenTableRegionsProcedure.class::cast).filter(p -> p.getTableName().equals(TABLE_NAME))
      .findFirst();
  }
}
