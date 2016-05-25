/**
 *
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestShortCircuitConnection {

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testShortCircuitConnection() throws IOException, InterruptedException {
    TableName tn = TableName.valueOf("testShortCircuitConnection");
    HTableDescriptor htd = UTIL.createTableDescriptor(tn);
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes("cf"));
    htd.addFamily(hcd);
    UTIL.createTable(htd, null);
    HRegionServer regionServer = UTIL.getRSForFirstRegionInTable(tn);
    ClusterConnection connection = regionServer.getClusterConnection();
    Table tableIf = connection.getTable(tn);
    assertTrue(tableIf instanceof HTable);
    HTable table = (HTable) tableIf;
    assertTrue(table.getConnection() == connection);
    AdminService.BlockingInterface admin = connection.getAdmin(regionServer.getServerName());
    ClientService.BlockingInterface client = connection.getClient(regionServer.getServerName());
    assertTrue(admin instanceof RSRpcServices);
    assertTrue(client instanceof RSRpcServices);
    ServerName anotherSn = ServerName.valueOf(regionServer.getServerName().getHostAndPort(),
      EnvironmentEdgeManager.currentTime());
    admin = connection.getAdmin(anotherSn);
    client = connection.getClient(anotherSn);
    assertFalse(admin instanceof RSRpcServices);
    assertFalse(client instanceof RSRpcServices);
    assertTrue(connection.getAdmin().getConnection() == connection);
  }
}
