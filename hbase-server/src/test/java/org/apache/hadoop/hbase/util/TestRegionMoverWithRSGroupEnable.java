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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.rsgroup.RSGroupUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.RegionMover.RegionMoverBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for rsgroup enable, unloaded regions from decommissoned host of a rsgroup should be assigned
 * to those regionservers belonging to the same rsgroup.
 */
@Category({ MiscTests.class, MediumTests.class })
public class TestRegionMoverWithRSGroupEnable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionMoverWithRSGroupEnable.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionMoverWithRSGroupEnable.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final String TEST_RSGROUP = "test";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    RSGroupUtil.enableRSGroup(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(5);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private final List<Address> rsservers = new ArrayList<>(2);

  @Before
  public void setUp() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();

    // Add a new rsgroup and assign two servers to it.
    admin.addRSGroup(TEST_RSGROUP);
    Collection<ServerName> allServers = admin.getRegionServers();
    // Remove rs contains hbase:meta, otherwise test looks unstable and buggy in test env.
    ServerName rsContainMeta = TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(t -> t.getRegionServer())
      .filter(rs -> rs.getRegions(connection.getMetaTableName()).size() > 0).findFirst().get()
      .getServerName();
    LOG.info("{} contains hbase:meta", rsContainMeta);
    List<ServerName> modifiable = new ArrayList<>(allServers);
    modifiable.remove(rsContainMeta);
    int i = 0;
    for (ServerName server : modifiable) {
      if (i == 2) break;
      rsservers.add(Address.fromParts(server.getHostname(), server.getPort()));
      i++;
    }
    admin.moveServersToRSGroup(new HashSet<>(rsservers), TEST_RSGROUP);
    LOG.info("Servers in {} are {}", TEST_RSGROUP, rsservers);
    assertEquals(3, admin.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().size());
    assertEquals(2, admin.getRSGroup(TEST_RSGROUP).getServers().size());

    // Create a pre-split table in test rsgroup
    TableName tableName = TableName.valueOf("testRegionMoverWithRSGroupEnable");
    if (admin.tableExists(tableName)) {
      TEST_UTIL.deleteTable(tableName);
    }
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).setRegionServerGroup(TEST_RSGROUP)
      .build();
    String startKey = "a";
    String endKey = "z";
    admin.createTable(tableDesc, Bytes.toBytes(startKey), Bytes.toBytes(endKey), 9);
  }

  @Test
  public void testUnloadRegions() throws Exception {
    Address decommission = rsservers.get(0);
    Address online = rsservers.get(1);
    String filename = new Path(TEST_UTIL.getDataTestDir(), "testRSGroupUnload").toString();
    RegionMoverBuilder builder =
      new RegionMoverBuilder(decommission.toString(), TEST_UTIL.getConfiguration());
    try (RegionMover rm = builder.filename(filename).ack(true).build()) {
      LOG.info("Unloading " + decommission.getHostname());
      rm.unload();
    }
    HRegionServer onlineRS = TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(JVMClusterUtil.RegionServerThread::getRegionServer)
      .filter(rs -> rs.getServerName().getAddress().equals(online)).findFirst().get();
    assertEquals(9, onlineRS.getNumberOfOnlineRegions());
  }

}
