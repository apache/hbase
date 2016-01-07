/*
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

package org.apache.hadoop.hbase.rest.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.rest.HBaseRESTTestingUtility;
import org.apache.hadoop.hbase.rest.model.StorageClusterStatusModel;
import org.apache.hadoop.hbase.rest.model.TableModel;
import org.apache.hadoop.hbase.rest.model.VersionModel;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestRemoteAdmin {
  private static final HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST_UTIL =
    new HBaseRESTTestingUtility();
  private static final String TABLE_1 = "TestRemoteAdmin_Table_1";
  private static final String TABLE_2 = TABLE_1 + System.currentTimeMillis();
  private static final byte[] COLUMN_1 = Bytes.toBytes("a");
  static final HTableDescriptor DESC_1 =  new HTableDescriptor(TABLE_1);
  static final HTableDescriptor DESC_2 =  new HTableDescriptor(TABLE_2);
  private static RemoteAdmin remoteAdmin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    DESC_1.addFamily(new HColumnDescriptor(COLUMN_1));

    TEST_UTIL.startMiniCluster();
    REST_TEST_UTIL.startServletContainer(TEST_UTIL.getConfiguration());

    remoteAdmin = new RemoteAdmin(new Client(
      new Cluster().add("localhost", REST_TEST_UTIL.getServletPort())),
      TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REST_TEST_UTIL.shutdownServletContainer();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreateAnDeleteTable() throws Exception {
    assertFalse(remoteAdmin.isTableAvailable(TABLE_1));
    remoteAdmin.createTable(DESC_1);
    assertTrue(remoteAdmin.isTableAvailable(TABLE_1));
    remoteAdmin.deleteTable(TABLE_1);
    assertFalse(remoteAdmin.isTableAvailable(TABLE_1));
  }

  @Test
  public void testGetRestVersion() throws Exception {

    VersionModel RETURNED_REST_VERSION = remoteAdmin.getRestVersion();
    System.out.print("Returned version is: " + RETURNED_REST_VERSION);

    // Assert that it contains info about rest version, OS, JVM
    assertTrue("Returned REST version did not contain info about rest.",
        RETURNED_REST_VERSION.toString().contains("rest"));
    assertTrue("Returned REST version did not contain info about the JVM.",
        RETURNED_REST_VERSION.toString().contains("JVM"));
    assertTrue("Returned REST version did not contain info about OS.",
        RETURNED_REST_VERSION.toString().contains("OS"));
  }

  @Test
  public void testClusterVersion() throws Exception {
    // testing the /version/cluster endpoint
    final String HBASE_VERSION = TEST_UTIL.getHBaseCluster().getClusterStatus()
        .getHBaseVersion();
    assertEquals("Cluster status from REST API did not match. ", HBASE_VERSION,
        remoteAdmin.getClusterVersion().getVersion());
  }

  @Test
  public void testClusterStatus() throws Exception {

    ClusterStatus status = TEST_UTIL.getHBaseClusterInterface()
        .getClusterStatus();
    StorageClusterStatusModel returnedStatus = remoteAdmin.getClusterStatus();
    assertEquals(
        "Region count from cluster status and returned status did not match up. ",
        status.getRegionsCount(), returnedStatus.getRegions());
    assertEquals(
        "Dead server count from cluster status and returned status did not match up. ",
        status.getDeadServers(), returnedStatus.getDeadNodes().size());
  }

  @Test
  public void testListTables() throws Exception {

    remoteAdmin.createTable(DESC_2);
    List<TableModel> tableList = remoteAdmin.getTableList().getTables();
    System.out.println("List of tables is: ");
    boolean found = false;
    for (TableModel tm : tableList) {

      if (tm.getName().equals(TABLE_2)) {
        found = true;
        break;
      }
    }
    assertTrue("Table " + TABLE_2 + " was not found by get request to '/'",
        found);
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}
