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
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;

@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
public class TestShortCircuitConnection {

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testShortCircuitConnection(TestInfo testInfo)
    throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    HTableDescriptor htd = UTIL.createTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes("cf"));
    htd.addFamily(hcd);
    UTIL.createTable(htd, null);
    HRegionServer regionServer = UTIL.getRSForFirstRegionInTable(tableName);
    ClusterConnection connection = regionServer.getClusterConnection();
    Table tableIf = connection.getTable(tableName);
    assertTrue(tableIf instanceof HTable);
    HTable table = (HTable) tableIf;
    assertTrue(table.getConnection() == connection);
    AdminService.BlockingInterface admin = connection.getAdmin(regionServer.getServerName());
    ClientService.BlockingInterface client = connection.getClient(regionServer.getServerName());
    assertTrue(admin instanceof RSRpcServices);
    assertTrue(
      client instanceof ServerConnectionUtils.ShortCircuitingClusterConnection.ClientServiceBlockingInterfaceWrapper);
    ServerName anotherSn = ServerName.valueOf(regionServer.getServerName().getAddress(),
      EnvironmentEdgeManager.currentTime());
    admin = connection.getAdmin(anotherSn);
    client = connection.getClient(anotherSn);
    assertFalse(admin instanceof RSRpcServices);
    assertFalse(client instanceof RSRpcServices);
    assertTrue(connection.getAdmin().getConnection() == connection);
  }
}
