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
import static org.junit.Assert.fail;

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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
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

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    testUtil = HBaseTestingUtility.createLocalHTU();
    testUtil.startMiniCluster();
    conn = (ConnectionManager.HConnectionImplementation) testUtil.getConnection();
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    conn.close();
    testUtil.shutdownMiniCluster();
  }

  @Test(expected = UnknownHostException.class)
  public void testGetAdminBadHostname() throws Exception {
    // verify that we can get an instance with the cluster hostname
    ServerName master = testUtil.getHBaseCluster().getMaster().getServerName();
    try {
      conn.getAdmin(master);
    } catch (UnknownHostException uhe) {
      fail("Obtaining admin to the cluster master should have succeeded");
    }

    // test that we fail to get a client to an unresolvable hostname, which
    // means it won't be cached
    ServerName badHost =
        ServerName.valueOf("unknownhost.invalid:" + HConstants.DEFAULT_MASTER_PORT,
        System.currentTimeMillis());
    conn.getAdmin(badHost);
    fail("Obtaining admin to unresolvable hostname should have failed");
  }

  @Test(expected = UnknownHostException.class)
  public void testGetClientBadHostname() throws Exception {
    // verify that we can get an instance with the cluster hostname
    ServerName rs = testUtil.getHBaseCluster().getRegionServer(0).getServerName();
    try {
      conn.getClient(rs);
    } catch (UnknownHostException uhe) {
      fail("Obtaining client to the cluster regionserver should have succeeded");
    }

    // test that we fail to get a client to an unresolvable hostname, which
    // means it won't be cached
    ServerName badHost =
        ServerName.valueOf("unknownhost.invalid:" + HConstants.DEFAULT_REGIONSERVER_PORT,
        System.currentTimeMillis());
    conn.getAdmin(badHost);
    fail("Obtaining client to unresolvable hostname should have failed");
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
