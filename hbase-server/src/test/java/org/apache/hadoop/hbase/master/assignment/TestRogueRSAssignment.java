/**
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
package org.apache.hadoop.hbase.master.assignment;

import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;

/**
 * Tests to verify master/ assignment manager functionality against rogue RS
 */
@Category({MasterTests.class, MediumTests.class})
public class TestRogueRSAssignment {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRogueRSAssignment.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRogueRSAssignment.class);

  @Rule
  public final TestName name = new TestName();

  @Rule
  public ExpectedException exception = ExpectedException.none();
  private static final int initialRegionCount = 3;
  private final static byte[] FAMILY = Bytes.toBytes("FAMILY");

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Configuration conf = UTIL.getConfiguration();
  private static Admin admin;
  private static MiniHBaseCluster cluster;
  private static HMaster master;

  private static void setupConf(Configuration conf) {
    // Reduce the maximum attempts to speed up the test
    conf.setInt("hbase.assignment.maximum.attempts", 3);
    conf.setInt("hbase.master.maximum.ping.server.attempts", 3);
    conf.setInt("hbase.master.ping.server.retry.sleep.interval", 1);
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(conf);
    UTIL.startMiniCluster(2);

    cluster = UTIL.getHBaseCluster();
    assertNotNull(cluster);

    admin = UTIL.getAdmin();
    assertNotNull(admin);

    master = cluster.getMaster();
    assertNotNull(master);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
      cluster = null;
      admin = null;
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Before
  public void setup() throws IOException {
    // Turn off balancer
    admin.setBalancerRunning(false, true);
  }

  @After
  public void tearDown() throws Exception {
    for (TableDescriptor td: UTIL.getAdmin().listTableDescriptors()) {
      LOG.info("Tear down, remove table=" + td.getTableName());
      UTIL.deleteTable(td.getTableName());
    }
    // Turn on balancer
    admin.setBalancerRunning(true, false);
  }

  /**
   * Ignore this test, see HBASE-21421
   */
  @Test
  @Ignore
  public void testReportRSWithWrongRegion() throws Exception {
    final TableName tableName = TableName.valueOf(this.name.getMethodName());

    List<HRegionInfo> tableRegions = createTable(tableName);

    final ServerName sn = ServerName.parseVersionedServerName(
        ServerName.valueOf("1.example.org", 1, System.currentTimeMillis()).getVersionedBytes());

    // make fake request with a region assigned to different RS
    RegionServerStatusProtos.RegionServerReportRequest.Builder request =
        makeRSReportRequestWithRegions(sn, tableRegions.get(1));

    // sending fake request to master
    // TODO: replace YouAreDeadException with appropriate exception as and when necessary
    exception.expect(ServiceException.class);
    exception.expectCause(isA(YouAreDeadException.class));
    RegionServerStatusProtos.RegionServerReportResponse response =
        master.getMasterRpcServices().regionServerReport(null, request.build());
  }

  private RegionServerStatusProtos.RegionServerReportRequest.Builder
      makeRSReportRequestWithRegions(final ServerName sn, HRegionInfo... regions) {
    ClusterStatusProtos.ServerLoad.Builder sl = ClusterStatusProtos.ServerLoad.newBuilder();
    for (int i = 0; i < regions.length; i++) {
      HBaseProtos.RegionSpecifier.Builder rs = HBaseProtos.RegionSpecifier.newBuilder();
      rs.setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME);
      rs.setValue(UnsafeByteOperations.unsafeWrap(regions[i].getRegionName()));

      ClusterStatusProtos.RegionLoad.Builder rl = ClusterStatusProtos.RegionLoad.newBuilder()
          .setRegionSpecifier(rs.build());

      sl.addRegionLoads(i, rl.build());
    }

    return RegionServerStatusProtos.RegionServerReportRequest.newBuilder()
              .setServer(ProtobufUtil.toServerName(sn))
              .setLoad(sl);
  }

  private List<HRegionInfo> createTable(final TableName tableName) throws Exception {
    TableDescriptorBuilder tdBuilder = TableDescriptorBuilder.newBuilder(tableName);
    tdBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build());

    byte[][] rows = new byte[initialRegionCount - 1][];
    for (int i = 0; i < rows.length; ++i) {
      rows[i] = Bytes.toBytes(String.format("%d", i));
    }
    admin.createTable(tdBuilder.build(), rows);
    return assertRegionCount(tableName, initialRegionCount);
  }

  private List<HRegionInfo> assertRegionCount(final TableName tableName, final int nregions)
      throws Exception {
    UTIL.waitUntilNoRegionsInTransition();
    List<HRegionInfo> tableRegions = admin.getTableRegions(tableName);
    assertEquals(nregions, tableRegions.size());
    return tableRegions;
  }
}
