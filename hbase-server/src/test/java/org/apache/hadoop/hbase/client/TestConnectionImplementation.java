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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests that we fail fast when hostname resolution is not working and do not cache
 * unresolved InetSocketAddresses.
 */
@Category({MediumTests.class, ClientTests.class})
public class TestConnectionImplementation {
  private static HBaseTestingUtility testUtil;
  private static ConnectionManager.HConnectionImplementation conn;
  private static HBaseProtos.RegionSpecifier specifier;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    testUtil = HBaseTestingUtility.createLocalHTU();
    testUtil.startMiniCluster();
    conn = (ConnectionManager.HConnectionImplementation) testUtil.getConnection();
    specifier = HBaseProtos.RegionSpecifier.
      newBuilder().setValue(ByteStringer.wrap(Bytes.toBytes("region")))
      .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME).build();
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    conn.close();
    testUtil.shutdownMiniCluster();
  }

  @Test
  public void testGetAdminBadHostname() throws Exception {
    // verify that we can get an instance with the cluster hostname
    ServerName master = testUtil.getHBaseCluster().getMaster().getServerName();
    HBaseRpcController controller = conn.getRpcControllerFactory().newController();

    AdminProtos.GetRegionInfoRequest request =
      AdminProtos.GetRegionInfoRequest.newBuilder().setRegion(specifier).build();
    AdminProtos.AdminService.BlockingInterface goodAdmin = conn.getAdmin(master);
    verifyAdminCall(goodAdmin, controller, request, false);

    // test that we fail to get a client to an unresolvable hostname, which
    // means it won't be cached
    ServerName badHost = ServerName
      .valueOf("unknownhost.invalid:" + HConstants.DEFAULT_MASTER_PORT, System.currentTimeMillis());
    AdminProtos.AdminService.BlockingInterface badAdmin = conn.getAdmin(badHost);
    verifyAdminCall(badAdmin, controller, request, true);
  }

  private void verifyAdminCall(AdminProtos.AdminService.BlockingInterface admin,
    HBaseRpcController rpcController, AdminProtos.GetRegionInfoRequest request,
    boolean shouldHaveHostException) {

    try {
      admin.getRegionInfo(rpcController, request);
    } catch (ServiceException se) {
      assertEquals(shouldHaveHostException, se.getCause() instanceof UnknownHostException);
    } catch (Exception e) {
      assertEquals(!shouldHaveHostException, e instanceof NotServingRegionException);
    }
  }

  @Test
  public void testGetClientBadHostname()
    throws Exception {
    // verify that we can get an instance with the cluster hostname
    ServerName rs = testUtil.getHBaseCluster().getRegionServer(0).getServerName();
    HBaseRpcController controller = conn.getRpcControllerFactory().newController();
    ClientProtos.Get get = ClientProtos.Get.newBuilder()
      .setRow(ByteStringer.wrap(Bytes.toBytes("r"))).build();
    ClientProtos.GetRequest request = ClientProtos.GetRequest.newBuilder().setGet(get)
      .setRegion(specifier).build();

    ClientProtos.ClientService.BlockingInterface goodClient = conn.getClient(rs);
    verifyClientCall(goodClient, controller, request, false);

    ServerName badHost = ServerName
      .valueOf("unknownhost.invalid:" + HConstants.DEFAULT_REGIONSERVER_PORT,
        System.currentTimeMillis());
    ClientProtos.ClientService.BlockingInterface badClient = conn.getClient(badHost);
    verifyClientCall(badClient, controller, request, true);
  }

  private void verifyClientCall(ClientProtos.ClientService.BlockingInterface client,
    HBaseRpcController rpcController, ClientProtos.GetRequest request,
    boolean shouldHaveHostException) {
    try {
      client.get(rpcController, request);
    } catch (ServiceException se) {
      assertEquals(shouldHaveHostException, se.getCause() instanceof UnknownHostException);
    } catch (Exception e) {
      assertEquals(!shouldHaveHostException, e instanceof NotServingRegionException);
    }
  }

  @Test
  public void testLocateRegionsWithRegionReplicas() throws IOException {
    int regionReplication = 3;
    byte[] family = Bytes.toBytes("cf");
    TableName tableName = TableName.valueOf("testLocateRegionsWithRegionReplicas");

    // Create a table with region replicas
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(family));
    desc.setRegionReplication(regionReplication);
    testUtil.getConnection().getAdmin().createTable(desc);

    try (ConnectionManager.HConnectionImplementation con =
      (ConnectionManager.HConnectionImplementation) ConnectionFactory.
        createConnection(testUtil.getConfiguration())) {

      // Get locations of the regions of the table
      List<HRegionLocation> locations = con.locateRegions(tableName, false, false);

      // The size of the returned locations should be 3
      assertEquals(regionReplication, locations.size());

      // The replicaIds of the returned locations should be 0, 1 and 2
      Set<Integer> expectedReplicaIds = new HashSet<>(Arrays.asList(0, 1, 2));
      for (HRegionLocation location : locations) {
        assertTrue(expectedReplicaIds.remove(location.getRegionInfo().getReplicaId()));
      }
    } finally {
      testUtil.deleteTable(tableName);
    }
  }
}
