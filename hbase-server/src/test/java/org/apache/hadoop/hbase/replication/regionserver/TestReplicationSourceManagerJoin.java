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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.replication.TestReplicationBaseNoBeforeAll;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.BeforeClass;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(ReplicationTests.TAG)
@Tag(MediumTests.TAG)
public class TestReplicationSourceManagerJoin extends TestReplicationBaseNoBeforeAll {

  @BeforeAll
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // NUM_SLAVES1 is presumed 2 in below.
    NUM_SLAVES1 = 2;
    configureClusters(UTIL1, UTIL2);
    startClusters();
  }

  @Test
  public void testReplicationSourcesTerminate(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    // Create table in source cluster only, let TableNotFoundException block peer to avoid
    // recovered source end.
    TableName tableName = TableName.valueOf(testName);
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(famName)
        .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(noRepfamName)).build();
    hbaseAdmin.createTable(td);
    assertFalse(UTIL2.getAdmin().tableExists(tableName));
    Table table = UTIL1.getConnection().getTable(tableName);
    // load data
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      table.put(new Put(Bytes.toBytes(i)).addColumn(famName, row, row));
    }
    // Kill rs holding table region. There are only TWO servers. We depend on it.
    Optional<HRegionServer> server = UTIL1.getMiniHBaseCluster().getLiveRegionServerThreads()
      .stream().map(JVMClusterUtil.RegionServerThread::getRegionServer)
      .filter(rs -> !rs.getRegions(tableName).isEmpty()).findAny();
    assertTrue(server.isPresent());
    server.get().abort("stopping for test");

    UTIL1.waitFor(60000, () -> 1 == UTIL1.getMiniHBaseCluster().getNumLiveRegionServers());
    UTIL1.waitTableAvailable(tableName);
    // Wait for recovered source running
    HRegionServer rs =
      UTIL1.getMiniHBaseCluster().getLiveRegionServerThreads().get(0).getRegionServer();
    ReplicationSourceManager manager = rs.getReplicationSourceService().getReplicationManager();
    UTIL1.waitFor(60000, () -> !manager.getOldSources().isEmpty());

    assertFalse(manager.getSources().isEmpty());
    assertFalse(manager.getOldSources().isEmpty());

    // Check all sources running before manager.join(), terminated after manager.join().
    Stream.concat(manager.getSources().stream(), manager.getOldSources().stream())
      .filter(src -> src instanceof ReplicationSource)
      .forEach(src -> assertTrue(((ReplicationSource) src).sourceRunning));
    manager.join();
    Stream.concat(manager.getSources().stream(), manager.getOldSources().stream())
      .filter(src -> src instanceof ReplicationSource)
      .forEach(src -> assertFalse(((ReplicationSource) src).sourceRunning));
  }

}
