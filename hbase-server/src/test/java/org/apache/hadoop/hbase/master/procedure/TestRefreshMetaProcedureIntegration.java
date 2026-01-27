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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.access.BulkLoadReadOnlyController;
import org.apache.hadoop.hbase.security.access.EndpointReadOnlyController;
import org.apache.hadoop.hbase.security.access.MasterReadOnlyController;
import org.apache.hadoop.hbase.security.access.RegionReadOnlyController;
import org.apache.hadoop.hbase.security.access.RegionServerReadOnlyController;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
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
    // Configure the cluster with ReadOnlyControllers
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      MasterReadOnlyController.class.getName());
    TEST_UTIL.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      String.join(",", RegionReadOnlyController.class.getName(),
        BulkLoadReadOnlyController.class.getName(), EndpointReadOnlyController.class.getName()));
    TEST_UTIL.getConfiguration().set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      RegionServerReadOnlyController.class.getName());

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

  @Test
  public void testRestoreMissingRegionInMeta() throws Exception {

    TableName tableName = TableName.valueOf("replicaTestTable");

    createTableWithData(tableName);

    List<RegionInfo> activeRegions = admin.getRegions(tableName);
    assertTrue("Should have at least 2 regions after split", activeRegions.size() >= 2);

    Table metaTable = TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME);
    RegionInfo regionToRemove = activeRegions.get(0);
    admin.unassign(regionToRemove.getRegionName(), false);
    Thread.sleep(1000);

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

    waitForProcedureCompletion(procId);

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

  @Test
  public void testPhantomTableCleanup() throws Exception {
    TableName table1 = TableName.valueOf("table1");
    TableName phantomTable = TableName.valueOf("phantomTable");
    createTableWithData(table1);
    createTableWithData(phantomTable);

    assertTrue("Table1 should have multiple regions", admin.getRegions(table1).size() >= 2);
    assertTrue("phantomTable should have multiple regions",
      admin.getRegions(phantomTable).size() >= 2);

    deleteTableFromFilesystem(phantomTable);
    List<TableName> tablesBeforeRefresh = Arrays.asList(admin.listTableNames());
    assertTrue("phantomTable should still be listed before refresh_meta",
      tablesBeforeRefresh.contains(phantomTable));
    assertTrue("Table1 should still be listed", tablesBeforeRefresh.contains(table1));

    setReadOnlyMode(true);
    Long procId = admin.refreshMeta();
    waitForProcedureCompletion(procId);

    List<TableName> tablesAfterRefresh = Arrays.asList(admin.listTableNames());

    assertFalse("phantomTable should be removed after refresh_meta",
      tablesAfterRefresh.contains(phantomTable));
    assertTrue("Table1 should still be listed", tablesAfterRefresh.contains(table1));
    assertTrue("phantomTable should have no regions after refresh_meta",
      admin.getRegions(phantomTable).isEmpty());
    setReadOnlyMode(false);
  }

  @Test
  public void testRestoreTableStateForOrphanRegions() throws Exception {
    TableName tableName = TableName.valueOf("t1");
    createTableInFilesystem(tableName);

    assertEquals("No tables should exist", 0,
      Stream.of(admin.listTableNames()).filter(tn -> tn.equals(tableName)).count());

    setReadOnlyMode(true);
    Long procId = admin.refreshMeta();
    waitForProcedureCompletion(procId);

    TableState tableState = MetaTableAccessor.getTableState(admin.getConnection(), tableName);
    assert tableState != null;
    assertEquals("Table state should be ENABLED", TableState.State.ENABLED, tableState.getState());
    assertEquals("The list should show the new table from the FS", 1,
      Stream.of(admin.listTableNames()).filter(tn -> tn.equals(tableName)).count());
    assertFalse("Should have at least 1 region", admin.getRegions(tableName).isEmpty());
    setReadOnlyMode(false);
  }

  private void createTableInFilesystem(TableName tableName) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path rootDir = CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration());
    Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);
    fs.mkdirs(tableDir);

    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    TEST_UTIL.getHBaseCluster().getMaster().getTableDescriptors()
      .update(builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf1")).build(), false);

    Path regionDir = new Path(tableDir, "dab6d1e1c88787c13b97647f11b2c907");
    Path regionInfoFile = new Path(regionDir, HRegionFileSystem.REGION_INFO_FILE);
    fs.mkdirs(regionDir);

    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).setStartKey(new byte[0])
      .setEndKey(new byte[0]).setRegionId(1757100253228L).build();
    byte[] regionInfoContent = RegionInfo.toDelimitedByteArray(regionInfo);
    try (FSDataOutputStream out = fs.create(regionInfoFile, true)) {
      out.write(regionInfoContent);
    }
  }

  private void deleteTableFromFilesystem(TableName tableName) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path rootDir = CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration());
    Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);
    if (fs.exists(tableDir)) {
      fs.delete(tableDir, true);
    }
  }

  private void createTableWithData(TableName tableName) throws Exception {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf1"));
    byte[] splitKeyBytes = Bytes.toBytes("split_key");
    admin.createTable(builder.build(), new byte[][] { splitKeyBytes });
    TEST_UTIL.waitTableAvailable(tableName);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < 100; i++) {
        Put put = new Put(Bytes.toBytes("row_" + String.format("%05d", i)));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("qual"), Bytes.toBytes("value_" + i));
        table.put(put);
      }
    }
    admin.flush(tableName);
  }

  private void waitForProcedureCompletion(Long procId) {
    assertTrue("Procedure ID should be positive", procId > 0);
    TEST_UTIL.waitFor(1000, () -> {
      try {
        return procExecutor.isFinished(procId);
      } catch (Exception e) {
        return false;
      }
    });
    assertProcNotFailed(procExecutor.getResult(procId));
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
