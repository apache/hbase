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

import static org.apache.hadoop.hbase.master.HMaster.HBASE_MASTER_RSPROC_DISPATCHER_CLASS;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.hbck.HbckChore;
import org.apache.hadoop.hbase.master.hbck.HbckReport;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Testing custom RSProcedureDispatcher to ensure retry limit can be imposed on certain errors.
 */
@Category({ MiscTests.class, LargeTests.class })
public class TestProcDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(TestProcDispatcher.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestProcDispatcher.class);

  @Rule
  public TestName name = new TestName();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static ServerName rs0;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set(HBASE_MASTER_RSPROC_DISPATCHER_CLASS,
      RSProcDispatcher.class.getName());
    TEST_UTIL.startMiniCluster(3);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    rs0 = cluster.getRegionServer(0).getServerName();
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("fam1")).build();
    int startKey = 0;
    int endKey = 80000;
    TEST_UTIL.getAdmin().createTable(tableDesc, Bytes.toBytes(startKey), Bytes.toBytes(endKey), 9);
  }

  @Test
  public void testRetryLimitOnConnClosedErrors() throws Exception {
    HbckChore hbckChore = new HbckChore(TEST_UTIL.getHBaseCluster().getMaster());
    final TableName tableName = TableName.valueOf(name.getMethodName());
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    Admin admin = TEST_UTIL.getAdmin();
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    List<Put> puts = IntStream.range(10, 50000).mapToObj(i -> new Put(Bytes.toBytes(i))
      .addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("q1"), Bytes.toBytes("val_" + i)))
      .collect(Collectors.toList());
    table.put(puts);
    admin.flush(tableName);
    admin.compact(tableName);
    Thread.sleep(3000);
    HRegionServer hRegionServer0 = cluster.getRegionServer(0);
    HRegionServer hRegionServer1 = cluster.getRegionServer(1);
    HRegionServer hRegionServer2 = cluster.getRegionServer(2);
    int numRegions0 = hRegionServer0.getNumberOfOnlineRegions();
    int numRegions1 = hRegionServer1.getNumberOfOnlineRegions();
    int numRegions2 = hRegionServer2.getNumberOfOnlineRegions();

    hbckChore.choreForTesting();
    HbckReport hbckReport = hbckChore.getLastReport();
    Assert.assertEquals(0, hbckReport.getInconsistentRegions().size());
    Assert.assertEquals(0, hbckReport.getOrphanRegionsOnFS().size());
    Assert.assertEquals(0, hbckReport.getOrphanRegionsOnRS().size());

    HRegion region0 = !hRegionServer0.getRegions().isEmpty()
      ? hRegionServer0.getRegions().get(0)
      : hRegionServer1.getRegions().get(0);
    // move all regions from server1 to server0
    for (HRegion region : hRegionServer1.getRegions()) {
      TEST_UTIL.getAdmin().move(region.getRegionInfo().getEncodedNameAsBytes(), rs0);
    }
    TEST_UTIL.getAdmin().move(region0.getRegionInfo().getEncodedNameAsBytes());
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();

    // Ensure:
    // 1. num of regions before and after scheduling SCP remain same
    // 2. all procedures including SCPs are successfully completed
    // 3. two servers have SCPs scheduled
    TEST_UTIL.waitFor(5000, 1000, () -> {
      LOG.info("numRegions0: {} , numRegions1: {} , numRegions2: {}", numRegions0, numRegions1,
        numRegions2);
      LOG.info("Online regions - server0 : {} , server1: {} , server2: {}",
        cluster.getRegionServer(0).getNumberOfOnlineRegions(),
        cluster.getRegionServer(1).getNumberOfOnlineRegions(),
        cluster.getRegionServer(2).getNumberOfOnlineRegions());
      LOG.info("Num of successfully completed procedures: {} , num of all procedures: {}",
        master.getMasterProcedureExecutor().getProcedures().stream()
          .filter(masterProcedureEnvProcedure -> masterProcedureEnvProcedure.getState()
              == ProcedureProtos.ProcedureState.SUCCESS)
          .count(),
        master.getMasterProcedureExecutor().getProcedures().size());
      LOG.info("Num of SCPs: " + master.getMasterProcedureExecutor().getProcedures().stream()
        .filter(proc -> proc instanceof ServerCrashProcedure).count());
      return (numRegions0 + numRegions1 + numRegions2)
          == (cluster.getRegionServer(0).getNumberOfOnlineRegions()
            + cluster.getRegionServer(1).getNumberOfOnlineRegions()
            + cluster.getRegionServer(2).getNumberOfOnlineRegions())
        && master.getMasterProcedureExecutor().getProcedures().stream()
          .filter(masterProcedureEnvProcedure -> masterProcedureEnvProcedure.getState()
              == ProcedureProtos.ProcedureState.SUCCESS)
          .count() == master.getMasterProcedureExecutor().getProcedures().size()
        && master.getMasterProcedureExecutor().getProcedures().stream()
          .filter(proc -> proc instanceof ServerCrashProcedure).count() > 0;
    });

    // Ensure we have no inconsistent regions
    TEST_UTIL.waitFor(5000, 1000, () -> {
      hbckChore.choreForTesting();
      HbckReport report = hbckChore.getLastReport();
      return report.getInconsistentRegions().isEmpty() && report.getOrphanRegionsOnFS().isEmpty()
        && report.getOrphanRegionsOnRS().isEmpty();
    });

  }

}
