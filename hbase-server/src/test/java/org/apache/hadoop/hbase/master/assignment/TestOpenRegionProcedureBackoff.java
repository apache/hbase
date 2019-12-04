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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ProcedureTestUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-23079.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestOpenRegionProcedureBackoff {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestOpenRegionProcedureBackoff.class);

  private static volatile boolean FAIL = false;

  private static final class AssignmentManagerForTest extends AssignmentManager {

    public AssignmentManagerForTest(MasterServices master) {
      super(master);
    }

    @Override
    void persistToMeta(RegionStateNode regionNode) throws IOException {
      if (FAIL) {
        throw new IOException("Inject Error!");
      }
      super.persistToMeta(regionNode);
    }
  }

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected AssignmentManager createAssignmentManager(MasterServices master) {
      return new AssignmentManagerForTest(master);
    }
  }

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName NAME = TableName.valueOf("Open");

  private static byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setClass(HConstants.MASTER_IMPL, HMasterForTest.class, HMaster.class);
    UTIL.startMiniCluster(1);
    UTIL.waitFor(10000, () -> {
      try (
        RegionLocator locator = UTIL.getConnection().getRegionLocator(TableName.META_TABLE_NAME)) {
        return locator.getRegionLocation(HConstants.EMPTY_START_ROW) != null;
      } catch (Exception e) {
        return false;
      }
    });
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private void assertBackoffIncrease() throws IOException, InterruptedException {
    ProcedureTestUtil.waitUntilProcedureWaitingTimeout(UTIL, OpenRegionProcedure.class, 30000);
    ProcedureTestUtil.waitUntilProcedureTimeoutIncrease(UTIL, OpenRegionProcedure.class, 2);
  }

  @Test
  public void testBackoff() throws IOException, InterruptedException, ExecutionException {
    FAIL = true;
    try (AsyncConnection conn =
      ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get()) {
      AsyncAdmin admin = conn.getAdminBuilder().setRpcTimeout(5, TimeUnit.MINUTES)
        .setOperationTimeout(10, TimeUnit.MINUTES).build();
      CompletableFuture<?> future = admin.createTable(TableDescriptorBuilder.newBuilder(NAME)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build());
      assertBackoffIncrease();
      FAIL = false;
      future.get();
      UTIL.waitTableAvailable(NAME);
    }
  }
}
