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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * MasterRegion related test that ensures the operations continue even when Procedure state update
 * encounters non-retriable IO errors.
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestMasterRegionMutation2 extends TestMasterRegionMutation1 {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterRegionMutation2.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setClass(HConstants.REGION_IMPL, TestRegion1.class, HRegion.class);
    StartTestingClusterOption.Builder builder = StartTestingClusterOption.builder();
    // 2 masters are expected to be aborted with this test
    builder.numMasters(3).numRegionServers(3);
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
    super.setUp();
  }

  @Test
  public void testMasterRegionMutations() throws Exception {
    super.testMasterRegionMutations();
  }

  public static class TestRegion1 extends HRegion {

    public TestRegion1(Path tableDir, WAL wal, FileSystem fs, Configuration confParam,
      RegionInfo regionInfo, TableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
    }

    public TestRegion1(HRegionFileSystem fs, WAL wal, Configuration confParam, TableDescriptor htd,
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
