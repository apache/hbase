/**
 * Copyright The Apache Software Foundation
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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Create table only on a set of regionservers instead of whole cluster
 * JIRA:
 *
 * https://issues.apache.org/jira/browse/HBASE-10425
 * https://issues.apache.org/jira/browse/HBASE-10865
 */
public class TestCreateTableOnServers {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private HBaseAdmin admin;
  private static final int NUM_REGION_SERVER = 7;
  /**
   * Number of regionservers that we will remove in the test from {@link #NUM_REGION_SERVER}
   */
  private static final int NUM_REMOVED_RS = 4;
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set(HConstants.LOAD_BALANCER_IMPL,
        "org.apache.hadoop.hbase.master.RegionManager$AssignmentLoadBalancer");
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.startMiniCluster(NUM_REGION_SERVER);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    this.admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
  }

  /**
   * Create two tables only on specific number of regionservers (subset of all
   * regionservers). Check after the creation whether the regions are assigned
   * only to those ones.
   *
   * @throws IOException
   */
  @Test
  public void testCreateTableOnRegionServers() throws IOException {
    byte[] tableName = Bytes.toBytes("testCreateTableWithRegionsOnServers");
    byte[][] splitKeys = {
        new byte[]{1, 1, 1},
        new byte[]{2, 2, 2},
        new byte[]{3, 3, 3},
        new byte[]{4, 4, 4},
        new byte[]{5, 5, 5},
        new byte[]{6, 6, 6},
        new byte[]{7, 7, 7},
        new byte[]{8, 8, 8},
        new byte[]{9, 9, 9},
    };
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    List<HServerAddress> keptServers = TEST_UTIL.getHBaseCluster().getRegionServers();
    System.out.println("initial number of servers: " + keptServers.size());
    // remove some regionservers
    for (int i = 0; i < NUM_REMOVED_RS; i++) {
      keptServers.remove(0);
    }
    // create a table on the remaining regionservers
    // refresh Admin to pickup new configuration about using favored nodes
    desc.setServers(keptServers);
    admin.createTable(desc, splitKeys);
    HTable ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    Map<HRegionInfo,HServerAddress> regions = ht.getRegionsInfo();
    int expectedRegions = splitKeys.length + 1;
    assertEquals("Tried to create " + expectedRegions + " regions " +
            "but only found " + regions.size(),
        expectedRegions, regions.size()
    );
     //verify if we use only a subset of the regionservers
    Set<HServerAddress> serversForTable = new HashSet<HServerAddress>();
    serversForTable.addAll(regions.values());
    assertEquals(NUM_REGION_SERVER - NUM_REMOVED_RS, keptServers.size());
    assertEquals(keptServers.size(), serversForTable.size());

    // create one more table only on allServers
    tableName = Bytes.toBytes("testCreateTableWithRegionsOnServers2");
    byte[][] splitKeys2 = {
        new byte[]{1, 1, 1},
        new byte[]{2, 2, 2}
    };
    desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    desc.setServers(keptServers);
    admin.createTable(desc, splitKeys2);
    ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    regions = ht.getRegionsInfo();
    expectedRegions = splitKeys2.length + 1;
    assertEquals("Tried to create " + expectedRegions + " regions " +
        "but only found " + regions.size(),
        expectedRegions, regions.size());
     //verify if we use only a subset of the regionservers
    serversForTable = new HashSet<HServerAddress>();
    serversForTable.addAll(regions.values());
    assertEquals(NUM_REGION_SERVER - NUM_REMOVED_RS, keptServers.size());
    assertEquals(keptServers.size(), serversForTable.size());
  }

  /**
   * Test to make sure that HTD without servers work as expected. And the server list is not
   * stored without action from the user.
   * @throws IOException
   */
  @Test
  public void testCreateTableNoServers() throws IOException {
    String tn = "globalAssignmentDomainTable";
    HTableDescriptor htd = new HTableDescriptor(tn);
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("d")));
    admin.createTable(htd);

    HTableDescriptor createdHTD = admin.getTableDescriptor(Bytes.toBytes(tn));
    assertEquals(null, createdHTD.getServers());

  }
}
