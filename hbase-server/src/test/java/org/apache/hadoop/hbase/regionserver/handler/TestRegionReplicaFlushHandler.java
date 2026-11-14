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
package org.apache.hadoop.hbase.regionserver.handler;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
public class TestRegionReplicaFlushHandler {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final byte[] FAMILY = Bytes.toBytes("cf");

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(StartTestingClusterOption.builder().numRegionServers(2).build());
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private static List<HRegion> getRegions(TableName tableName) {
    return UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(RegionServerThread::getRegionServer).flatMap(rs -> rs.getRegions(tableName).stream())
      .collect(Collectors.toList());
  }

  /**
   * The table is disabled and dropped while the last flush attempt against the primary replica is
   * in flight. The flush then fails with UnknownRegionException, as the region is gone from meta,
   * and the follow up isTableDisabled check fails with TableNotFoundException. The handler should
   * treat this as a dropped table and return, instead of aborting the region server.
   */
  @Test
  public void testTableDroppedDuringLastFlushAttempt(TestInfo testInfo) throws Exception {
    TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    UTIL.getAdmin().createTable(TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(2)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build());
    UTIL.waitFor(30000, () -> getRegions(tableName).size() == 2);
    HRegion secondary = getRegions(tableName).stream()
      .filter(r -> !ServerRegionReplicaUtil.isDefaultReplica(r.getRegionInfo())).findFirst().get();
    byte[] primaryRegionName = ServerRegionReplicaUtil
      .getRegionInfoForDefaultReplica(secondary.getRegionInfo()).getRegionName();

    HRegionServer rs = UTIL.getMiniHBaseCluster().getRegionServer(0);
    AsyncClusterConnection realConn = rs.getAsyncClusterConnection();
    // no retries, so the first flush attempt is also the last one
    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
    AsyncClusterConnection conn = mock(AsyncClusterConnection.class);
    when(conn.getConfiguration()).thenReturn(conf);
    when(conn.getAdmin()).thenAnswer(inv -> realConn.getAdmin());
    when(conn.flush(any(), anyBoolean())).thenAnswer(inv -> {
      // drop the table after the handler has checked that the region is open
      UTIL.getAdmin().disableTable(tableName);
      UTIL.getAdmin().deleteTable(tableName);
      return realConn.flush(inv.getArgument(0), inv.getArgument(1));
    });
    Server server = mock(Server.class);
    when(server.getConfiguration()).thenReturn(conf);
    when(server.getAsyncClusterConnection()).thenReturn(conn);

    new RegionReplicaFlushHandler(server, secondary).run();

    assertThrows(UnknownRegionException.class,
      () -> FutureUtils.get(realConn.flush(primaryRegionName, true)));
    assertThrows(TableNotFoundException.class,
      () -> FutureUtils.get(realConn.getAdmin().isTableDisabled(tableName)));
    verify(server, never()).abort(anyString(), any());
  }
}
