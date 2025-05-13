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
package org.apache.hadoop.hbase.master.procedure;

import static org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.assertProcNotFailed;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.access.ReadOnlyController;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, LargeTests.class })
public class TestRefreshMetaProcedureIntegration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRefreshMetaProcedureIntegration.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private Admin admin;
  private ProcedureExecutor<MasterProcedureEnv> procExecutor;
  private HMaster master;
  private HRegionServer regionServer;

  @Before
  public void setup() throws Exception {
    // Configure the cluster with ReadOnlyController
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      ReadOnlyController.class.getName());
    TEST_UTIL.getConfiguration().set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      ReadOnlyController.class.getName());
    TEST_UTIL.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      ReadOnlyController.class.getName());

    // Start in active mode
    TEST_UTIL.getConfiguration().setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, false);

    TEST_UTIL.startMiniCluster();
    admin = TEST_UTIL.getAdmin();
    procExecutor = TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    master = TEST_UTIL.getHBaseCluster().getMaster();
    regionServer = TEST_UTIL.getHBaseCluster().getRegionServerThreads().get(0).getRegionServer();
  }

  @After
  public void tearDown() throws Exception {
    if (admin != null) {
      admin.close();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * A test for RefreshMetaProcedure to test the workflow: 1. Write data and create regions in
   * active mode 2. Switch to read-only mode 3. Use refresh_meta to sync meta table with storage and
   * switch back to active mode
   */
  @Test
  public void testRestoreMissingRegionInMeta() throws Exception {

    TableName tableName = TableName.valueOf("replicaTestTable");
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf1"));
    byte[] splitKey = Bytes.toBytes("split_key");
    admin.createTable(builder.build(), new byte[][] { splitKey });

    TEST_UTIL.waitTableAvailable(tableName);

    Table table = TEST_UTIL.getConnection().getTable(tableName);
    for (int i = 0; i < 100; i++) {
      Put put = new Put(Bytes.toBytes("row_" + String.format("%03d", i)));
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("qual"), Bytes.toBytes("value_" + i));
      table.put(put);
    }
    table.close();

    admin.flush(tableName);

    List<RegionInfo> activeRegions = admin.getRegions(tableName);
    assertTrue("Should have at least 2 regions after split", activeRegions.size() >= 2);

    Table metaTable = TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME);
    RegionInfo regionToRemove = activeRegions.get(0);

    org.apache.hadoop.hbase.client.Delete delete =
      new org.apache.hadoop.hbase.client.Delete(regionToRemove.getRegionName());
    metaTable.delete(delete);
    metaTable.close();

    List<RegionInfo> regionsAfterDrift = admin.getRegions(tableName);
    assertEquals("Should have one less region in meta after simulating drift",
      activeRegions.size() - 1, regionsAfterDrift.size());

    setReadOnlyMode(true);

    boolean writeBlocked = false;
    try {
      Table readOnlyTable = TEST_UTIL.getConnection().getTable(tableName);
      Put testPut = new Put(Bytes.toBytes("test_readonly"));
      testPut.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("qual"), Bytes.toBytes("should_fail"));
      readOnlyTable.put(testPut);
      readOnlyTable.close();
    } catch (Exception e) {
      if (e.getMessage().contains("Operation not allowed in Read-Only Mode")) {
        writeBlocked = true;
      }
    }
    assertTrue("Write operations should be blocked in read-only mode", writeBlocked);

    Long procId = admin.refreshMeta();

    assertTrue("Procedure ID should be positive", procId > 0);

    TEST_UTIL.waitFor(3000, () -> {
      try {
        return procExecutor.isFinished(procId);
      } catch (Exception e) {
        return false;
      }
    });

    assertProcNotFailed(procExecutor.getResult(procId));

    List<RegionInfo> regionsAfterRefresh = admin.getRegions(tableName);
    assertEquals("Missing regions should be restored by refresh_meta", activeRegions.size(),
      regionsAfterRefresh.size());

    boolean regionRestored = regionsAfterRefresh.stream()
      .anyMatch(r -> r.getRegionNameAsString().equals(regionToRemove.getRegionNameAsString()));
    assertTrue("Missing region should be restored by refresh_meta", regionRestored);

    setReadOnlyMode(false);

    Table activeTable = TEST_UTIL.getConnection().getTable(tableName);
    Put testPut = new Put(Bytes.toBytes("test_active_again"));
    testPut.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("qual"),
      Bytes.toBytes("active_mode_again"));
    activeTable.put(testPut);
    activeTable.close();
  }

  private void setReadOnlyMode(boolean isReadOnly) {
    TEST_UTIL.getConfiguration().setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY,
      isReadOnly);
    notifyConfigurationObservers();
  }

  private void notifyConfigurationObservers() {
    master.getConfigurationManager().notifyAllObservers(TEST_UTIL.getConfiguration());
    regionServer.getConfigurationManager().notifyAllObservers(TEST_UTIL.getConfiguration());
  }
}
