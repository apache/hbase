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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.hbck.HbckChore;
import org.apache.hadoop.hbase.master.hbck.HbckReport;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.wal.WAL;
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
 * MasterRegion related test that ensures the operations continue even when Procedure state update
 * encounters IO errors.
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestMasterRegionMutation {

  private static final Logger LOG = LoggerFactory.getLogger(TestMasterRegionMutation.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterRegionMutation.class);

  @Rule
  public TestName name = new TestName();

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static ServerName rs0;

  private static final AtomicBoolean ERROR_OUT = new AtomicBoolean(false);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setClass(HConstants.REGION_IMPL, TestRegion.class, HRegion.class);
    StartTestingClusterOption.Builder builder = StartTestingClusterOption.builder();
    builder.numMasters(4).numRegionServers(3);
    TEST_UTIL.startMiniCluster(builder.build());
    SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
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
  public void testMasterRegionMutations() throws Exception {
    HbckChore hbckChore = new HbckChore(TEST_UTIL.getHBaseCluster().getMaster());
    SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

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

    // procedure state store update failure needs to trigger active master abort and hence master
    // failover
    ERROR_OUT.set(true);

    // move one region from server 1 to server 0
    TEST_UTIL.getAdmin()
      .move(hRegionServer1.getRegions().get(0).getRegionInfo().getEncodedNameAsBytes(), rs0);

    // procedure state store update failure needs to trigger active master abort and hence master
    // failover
    ERROR_OUT.set(true);

    // move one region from server 2 to server 0
    TEST_UTIL.getAdmin()
      .move(hRegionServer2.getRegions().get(0).getRegionInfo().getEncodedNameAsBytes(), rs0);

    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();

    // Ensure:
    // 1. num of regions before and after master abort remain same
    // 2. all procedures are successfully completed
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
      return (numRegions0 + numRegions1 + numRegions2)
          == (cluster.getRegionServer(0).getNumberOfOnlineRegions()
            + cluster.getRegionServer(1).getNumberOfOnlineRegions()
            + cluster.getRegionServer(2).getNumberOfOnlineRegions())
        && master.getMasterProcedureExecutor().getProcedures().stream()
          .filter(masterProcedureEnvProcedure -> masterProcedureEnvProcedure.getState()
              == ProcedureProtos.ProcedureState.SUCCESS)
          .count() == master.getMasterProcedureExecutor().getProcedures().size();
    });

    // Ensure we have no inconsistent regions
    TEST_UTIL.waitFor(5000, 1000, () -> {
      HbckChore hbck = new HbckChore(TEST_UTIL.getHBaseCluster().getMaster());
      hbck.choreForTesting();
      HbckReport report = hbck.getLastReport();
      return report.getInconsistentRegions().isEmpty() && report.getOrphanRegionsOnFS().isEmpty()
        && report.getOrphanRegionsOnRS().isEmpty();
    });

  }

  public static class TestRegion extends HRegion {

    public TestRegion(Path tableDir, WAL wal, FileSystem fs, Configuration confParam,
      RegionInfo regionInfo, TableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
    }

    public TestRegion(HRegionFileSystem fs, WAL wal, Configuration confParam, TableDescriptor htd,
      RegionServerServices rsServices) {
      super(fs, wal, confParam, htd, rsServices);
    }

    @Override
    public OperationStatus[] batchMutate(Mutation[] mutations, boolean atomic, long nonceGroup,
      long nonce) throws IOException {
      if (
        MasterRegionFactory.TABLE_NAME.equals(getTableDescriptor().getTableName())
          && ERROR_OUT.get()
      ) {
        ERROR_OUT.set(false);
        throw new IOException("test error");
      }
      return super.batchMutate(mutations, atomic, nonceGroup, nonce);
    }
  }

}
