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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;

@Tag(MasterTests.TAG)
@Tag(LargeTests.TAG)
public class TestTruncateTableProcedure extends TestTableDDLProcedureBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestTruncateTableProcedure.class);
  private String testMethodName;

  @BeforeAll
  public static void setupCluster() throws Exception {
    TestTableDDLProcedureBase.setupCluster();
  }

  @AfterAll
  public static void cleanupTest() throws Exception {
    TestTableDDLProcedureBase.cleanupTest();
  }

  @BeforeEach
  public void setTestMethod(TestInfo testInfo) {
    testMethodName = testInfo.getTestMethod().get().getName();
  }

  @Test
  public void testTruncateNotExistentTable() throws Exception {
    final TableName tableName = TableName.valueOf(testMethodName);

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    // HBASE-20178 has us fail-fast, in the constructor, so add try/catch for this case.
    // Keep old way of looking at procedure too.
    Throwable cause = null;
    try {
      long procId = ProcedureTestingUtility.submitAndWait(procExec,
        new TruncateTableProcedure(procExec.getEnvironment(), tableName, true));

      // Second delete should fail with TableNotFound
      Procedure<?> result = procExec.getResult(procId);
      assertTrue(result.isFailed());
      cause = ProcedureTestingUtility.getExceptionCause(result);
    } catch (Throwable t) {
      cause = t;
    }
    LOG.debug("Truncate failed with exception: " + cause);
    assertTrue(cause instanceof TableNotFoundException);
  }

  @Test
  public void testTruncateNotDisabledTable() throws Exception {
    final TableName tableName = TableName.valueOf(testMethodName);

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f");

    // HBASE-20178 has us fail-fast, in the constructor, so add try/catch for this case.
    // Keep old way of looking at procedure too.
    Throwable cause = null;
    try {
      long procId = ProcedureTestingUtility.submitAndWait(procExec,
        new TruncateTableProcedure(procExec.getEnvironment(), tableName, false));

      // Second delete should fail with TableNotDisabled
      Procedure<?> result = procExec.getResult(procId);
      assertTrue(result.isFailed());
      cause = ProcedureTestingUtility.getExceptionCause(result);
    } catch (Throwable t) {
      cause = t;
    }
    LOG.debug("Truncate failed with exception: " + cause);
    assertTrue(cause instanceof TableNotDisabledException);
  }

  @Test
  public void testSimpleTruncatePreserveSplits() throws Exception {
    final TableName tableName = TableName.valueOf(testMethodName);
    testSimpleTruncate(tableName, true);
  }

  @Test
  public void testSimpleTruncateNoPreserveSplits() throws Exception {
    final TableName tableName = TableName.valueOf(testMethodName);
    testSimpleTruncate(tableName, false);
  }

  private void testSimpleTruncate(final TableName tableName, final boolean preserveSplits)
    throws Exception {
    final String[] families = new String[] { "f1", "f2" };
    final byte[][] splitKeys =
      new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c") };

    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(getMasterProcedureExecutor(),
      tableName, splitKeys, families);
    // load and verify that there are rows in the table
    MasterProcedureTestingUtility.loadData(UTIL.getConnection(), tableName, 100, splitKeys,
      families);
    assertEquals(100, UTIL.countRows(tableName));
    // disable the table
    UTIL.getAdmin().disableTable(tableName);

    // truncate the table
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new TruncateTableProcedure(procExec.getEnvironment(), tableName, preserveSplits));
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    // If truncate procedure completed successfully, it means all regions were assigned correctly
    // and table is enabled now.
    UTIL.waitUntilAllRegionsAssigned(tableName);

    // validate the table regions and layout
    regions = UTIL.getAdmin().getRegions(tableName).toArray(new RegionInfo[0]);
    if (preserveSplits) {
      assertEquals(1 + splitKeys.length, regions.length);
    } else {
      assertEquals(1, regions.length);
    }
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, families);

    // verify that there are no rows in the table
    assertEquals(0, UTIL.countRows(tableName));

    // verify that the table is read/writable
    MasterProcedureTestingUtility.loadData(UTIL.getConnection(), tableName, 50, splitKeys,
      families);
    assertEquals(50, UTIL.countRows(tableName));
  }

  @Test
  public void testRecoveryAndDoubleExecutionPreserveSplits() throws Exception {
    final TableName tableName = TableName.valueOf(testMethodName);
    testRecoveryAndDoubleExecution(tableName, true);
  }

  @Test
  public void testRecoveryAndDoubleExecutionNoPreserveSplits() throws Exception {
    final TableName tableName = TableName.valueOf(testMethodName);
    testRecoveryAndDoubleExecution(tableName, false);
  }

  private void testRecoveryAndDoubleExecution(final TableName tableName,
    final boolean preserveSplits) throws Exception {
    final String[] families = new String[] { "f1", "f2" };

    // create the table
    final byte[][] splitKeys =
      new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c") };
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(getMasterProcedureExecutor(),
      tableName, splitKeys, families);
    // load and verify that there are rows in the table
    MasterProcedureTestingUtility.loadData(UTIL.getConnection(), tableName, 100, splitKeys,
      families);
    assertEquals(100, UTIL.countRows(tableName));
    // disable the table
    UTIL.getAdmin().disableTable(tableName);

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillIfHasParent(procExec, false);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Truncate procedure && kill the executor
    long procId = procExec.submitProcedure(
      new TruncateTableProcedure(procExec.getEnvironment(), tableName, preserveSplits));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    UTIL.waitUntilAllRegionsAssigned(tableName);

    // validate the table regions and layout
    regions = UTIL.getAdmin().getRegions(tableName).toArray(new RegionInfo[0]);
    if (preserveSplits) {
      assertEquals(1 + splitKeys.length, regions.length);
    } else {
      assertEquals(1, regions.length);
    }
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, families);

    // verify that there are no rows in the table
    assertEquals(0, UTIL.countRows(tableName));

    // verify that the table is read/writable
    MasterProcedureTestingUtility.loadData(UTIL.getConnection(), tableName, 50, splitKeys,
      families);
    assertEquals(50, UTIL.countRows(tableName));
  }

  @Test
  public void testOnHDFSFailurePreserveSplits() throws Exception {
    final TableName tableName = TableName.valueOf(testMethodName);
    testOnHDFSFailure(tableName, true);
  }

  @Test
  public void testOnHDFSFailureNoPreserveSplits() throws Exception {
    final TableName tableName = TableName.valueOf(testMethodName);
    testOnHDFSFailure(tableName, false);
  }

  public static class TruncateTableProcedureOnHDFSFailure extends TruncateTableProcedure {

    private boolean failOnce = false;

    public TruncateTableProcedureOnHDFSFailure() {
      // Required by the Procedure framework to create the procedure on replay
      super();
    }

    public TruncateTableProcedureOnHDFSFailure(final MasterProcedureEnv env, TableName tableName,
      boolean preserveSplits) throws HBaseIOException {
      super(env, tableName, preserveSplits);
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env,
      MasterProcedureProtos.TruncateTableState state) throws InterruptedException {

      if (
        !failOnce
          && state == MasterProcedureProtos.TruncateTableState.TRUNCATE_TABLE_CREATE_FS_LAYOUT
      ) {
        try {
          // To emulate an HDFS failure, create only the first region directory
          RegionInfo regionInfo = getFirstRegionInfo();
          Configuration conf = env.getMasterConfiguration();
          MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
          Path tempdir = mfs.getTempDir();
          Path tableDir = CommonFSUtils.getTableDir(tempdir, regionInfo.getTable());
          Path regionDir = FSUtils.getRegionDirFromTableDir(tableDir, regionInfo);
          FileSystem fs = FileSystem.get(conf);
          fs.mkdirs(regionDir);

          failOnce = true;
          return Flow.HAS_MORE_STATE;
        } catch (IOException e) {
          fail("failed to create a region directory: " + e);
        }
      }

      return super.executeFromState(env, state);
    }
  }

  private void testOnHDFSFailure(TableName tableName, boolean preserveSplits) throws Exception {
    String[] families = new String[] { "f1", "f2" };
    byte[][] splitKeys =
      new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c") };

    // create a table
    MasterProcedureTestingUtility.createTable(getMasterProcedureExecutor(), tableName, splitKeys,
      families);

    // load and verify that there are rows in the table
    MasterProcedureTestingUtility.loadData(UTIL.getConnection(), tableName, 100, splitKeys,
      families);
    assertEquals(100, UTIL.countRows(tableName));

    // disable the table
    UTIL.getAdmin().disableTable(tableName);

    // truncate the table
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    long procId =
      ProcedureTestingUtility.submitAndWait(procExec, new TruncateTableProcedureOnHDFSFailure(
        procExec.getEnvironment(), tableName, preserveSplits));
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
  }

  @Test
  public void testTruncateWithPreserveAfterSplit() throws Exception {
    String[] families = new String[] { "f1", "f2" };
    byte[][] splitKeys =
      new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c") };
    TableName tableName = TableName.valueOf(testMethodName);
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(getMasterProcedureExecutor(),
      tableName, splitKeys, families);
    splitAndTruncate(tableName, regions, 1);
  }

  @Test
  public void testTruncatePreserveWithReplicaRegionAfterSplit() throws Exception {
    String[] families = new String[] { "f1", "f2" };
    byte[][] splitKeys =
      new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c") };
    TableName tableName = TableName.valueOf(testMethodName);

    // create a table with region replications
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(3)
      .setColumnFamilies(Arrays.stream(families)
        .map(fam -> ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(fam)).build())
        .collect(Collectors.toList()))
      .build();
    RegionInfo[] regions = ModifyRegionUtils.createRegionInfos(htd, splitKeys);
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new CreateTableProcedure(procExec.getEnvironment(), htd, regions));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));

    splitAndTruncate(tableName, regions, 3);
  }

  private void splitAndTruncate(TableName tableName, RegionInfo[] regions, int regionReplication)
    throws IOException, InterruptedException {
    // split a region
    UTIL.getAdmin().split(tableName, new byte[] { '0' });

    // wait until split really happens
    UTIL.waitFor(60000,
      () -> UTIL.getAdmin().getRegions(tableName).size() > regions.length * regionReplication);

    // disable the table
    UTIL.getAdmin().disableTable(tableName);

    // truncate the table
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new TruncateTableProcedure(procExec.getEnvironment(), tableName, true));
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    UTIL.waitUntilAllRegionsAssigned(tableName);
    // confirm that we have the correct number of regions
    assertEquals((regions.length + 1) * regionReplication,
      UTIL.getAdmin().getRegions(tableName).size());
  }

  @Test
  public void testTruncatePreserveSplitsWithDuplicateStartKeyRegions() throws Exception {
    final TableName tableName = TableName.valueOf(testMethodName);
    final String[] families = new String[] { "f1", "f2" };
    final byte[][] splitKeys =
      new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c") };

    // Create a table with splits
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(getMasterProcedureExecutor(),
      tableName, splitKeys, families);

    // Manually insert a duplicate startKey region into meta to simulate region overlap.
    // This creates a region with the same startKey as an existing region (startKey="a").
    RegionInfo overlapRegion = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(Bytes.toBytes("a")).setEndKey(Bytes.toBytes("b")).build();
    MetaTableAccessor.addRegionsToMeta(UTIL.getConnection(),
      Collections.singletonList(overlapRegion), 1);
    // Register the overlap region into RegionStates (CLOSED state) so that it shows up in
    // the region list seen by TruncateTableProcedure
    RegionStates regionStates = getMaster().getAssignmentManager().getRegionStates();
    RegionStateNode node = regionStates.getOrCreateRegionStateNode(overlapRegion);
    node.setState(RegionState.State.CLOSED);

    // Disable the table
    UTIL.getAdmin().disableTable(tableName);

    // Try to truncate with preserveSplits=true, should fail due to duplicate startKey
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new TruncateTableProcedure(procExec.getEnvironment(), tableName, true));

    Procedure<?> result = procExec.getResult(procId);
    assertTrue(result.isFailed(), "Truncate should fail when there are duplicate startKey regions");
    Throwable cause = ProcedureTestingUtility.getExceptionCause(result);
    assertTrue(cause instanceof HBaseIOException,
      "Expected HBaseIOException but got: " + cause.getClass().getName());
    assertTrue(cause.getMessage().contains("Found duplicate startKey region"),
      "Error message should mention duplicate startKey");
    LOG.info("Truncate correctly failed with: " + cause.getMessage());
  }

  @Test
  public void testTruncateNoPreserveSplitsWithDuplicateStartKeyRegions() throws Exception {
    final TableName tableName = TableName.valueOf(testMethodName);
    final String[] families = new String[] { "f1", "f2" };
    final byte[][] splitKeys =
      new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c") };

    // Create a table with splits
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(getMasterProcedureExecutor(),
      tableName, splitKeys, families);

    // Manually insert a duplicate startKey region into meta to simulate region overlap
    RegionInfo overlapRegion = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(Bytes.toBytes("a")).setEndKey(Bytes.toBytes("b")).build();
    MetaTableAccessor.addRegionsToMeta(UTIL.getConnection(),
      Collections.singletonList(overlapRegion), 1);
    // Register the overlap region into RegionStates (CLOSED state) so that it shows up in
    // the region list seen by TruncateTableProcedure
    RegionStates regionStates = getMaster().getAssignmentManager().getRegionStates();
    RegionStateNode node = regionStates.getOrCreateRegionStateNode(overlapRegion);
    node.setState(RegionState.State.CLOSED);

    // Disable the table
    UTIL.getAdmin().disableTable(tableName);

    // Truncate with preserveSplits=false should succeed even with duplicate startKey regions,
    // because it creates a single new region regardless of existing region state
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new TruncateTableProcedure(procExec.getEnvironment(), tableName, false));
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    UTIL.waitUntilAllRegionsAssigned(tableName);

    // Verify table has only 1 region (no splits preserved)
    RegionInfo[] newRegions = UTIL.getAdmin().getRegions(tableName).toArray(new RegionInfo[0]);
    assertEquals(1, newRegions.length,
      "Should have exactly 1 region after truncate without preserving splits");
    LOG.info("Truncate without preserveSplits succeeded with " + newRegions.length + " region(s)");
  }

  /**
   * Test scenario: the table has 2 regions (split key "m"), each region has an overlapping
   * duplicate region (same startKey/endKey but different regionId, hence different encodedName --
   * i.e. duplicate regions with the same key range caused by region overlap in production). With
   * preserveSplits=true, TruncateTableProcedure detects the duplicate startKey in the pre-check
   * phase via checkRegionsStartKeyNoDuplicate and FAILs the truncate operation instead of
   * proceeding. This case verifies that failure behavior.
   */
  @Test
  public void testTruncatePreserveSplitsWithOverlapRegions() throws Exception {
    final TableName tableName = TableName.valueOf(testMethodName);
    final String[] families = new String[] { "f1" };
    final byte[][] splitKeys = new byte[][] { Bytes.toBytes("m") };

    // Create a normal table with 2 regions ("" -> "m" and "m" -> "")
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(getMasterProcedureExecutor(),
      tableName, splitKeys, families);
    assertEquals(2, regions.length);

    // Build overlap regions (same key range but different regionId, different encodedName)
    long overlapRegionId1 = regions[0].getRegionId() + 1000;
    long overlapRegionId2 = regions[1].getRegionId() + 1000;

    RegionInfo overlapRegion1 =
      RegionInfoBuilder.newBuilder(tableName).setStartKey(regions[0].getStartKey())
        .setEndKey(regions[0].getEndKey()).setRegionId(overlapRegionId1).build();

    RegionInfo overlapRegion2 =
      RegionInfoBuilder.newBuilder(tableName).setStartKey(regions[1].getStartKey())
        .setEndKey(regions[1].getEndKey()).setRegionId(overlapRegionId2).build();

    // Write the overlap regions into meta
    List<RegionInfo> overlapRegions = new ArrayList<>();
    overlapRegions.add(overlapRegion1);
    overlapRegions.add(overlapRegion2);
    MetaTableAccessor.addRegionsToMeta(UTIL.getConnection(), overlapRegions, 1);

    // Register the overlap regions into RegionStates (CLOSED state, simulating post-disable)
    RegionStates regionStates = getMaster().getAssignmentManager().getRegionStates();
    RegionStateNode node1 = regionStates.getOrCreateRegionStateNode(overlapRegion1);
    node1.setState(RegionState.State.CLOSED);
    RegionStateNode node2 = regionStates.getOrCreateRegionStateNode(overlapRegion2);
    node2.setState(RegionState.State.CLOSED);

    // Verify RegionStates has 4 regions for this table (2 normal + 2 overlap)
    assertEquals(4, regionStates.getRegionsOfTable(tableName).size(),
      "Should have 4 regions (2 normal + 2 overlap)");

    // Disable the table
    UTIL.getAdmin().disableTable(tableName);

    // preserveSplits=true should fail due to duplicate startKey (instead of succeeding)
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new TruncateTableProcedure(procExec.getEnvironment(), tableName, true));

    Procedure<?> result = procExec.getResult(procId);
    assertTrue(result.isFailed(),
      "Truncate should fail with overlap regions when preserveSplits=true");
    Throwable cause = ProcedureTestingUtility.getExceptionCause(result);
    assertTrue(cause instanceof HBaseIOException,
      "Expected HBaseIOException but got: " + cause.getClass().getName());
    assertTrue(cause.getMessage().contains("Found duplicate startKey region"),
      "Error message should mention duplicate startKey");
    LOG.info("Truncate correctly failed with: " + cause.getMessage());
  }

  /**
   * Test scenario: the table has 2 regions, each has 3 overlapping duplicate regions (8 regions in
   * total), simulating more severe overlap with the same key range. Same as
   * {@link #testTruncatePreserveSplitsWithOverlapRegions()}, the duplicate startKey check across
   * multiple overlapping regions FAILs the whole truncate operation when preserveSplits=true.
   */
  @Test
  public void testTruncatePreserveSplitsWithMultipleOverlapRegions() throws Exception {
    final TableName tableName = TableName.valueOf(testMethodName);
    final String[] families = new String[] { "f1" };
    final byte[][] splitKeys = new byte[][] { Bytes.toBytes("m") };

    // Create a normal table with 2 regions
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(getMasterProcedureExecutor(),
      tableName, splitKeys, families);
    assertEquals(2, regions.length);

    // Build 3 overlap regions per region (6 extra regions in total, same key range)
    RegionStates regionStates = getMaster().getAssignmentManager().getRegionStates();
    List<RegionInfo> overlapRegions = new ArrayList<>();

    for (int i = 0; i < regions.length; i++) {
      for (int j = 1; j <= 3; j++) {
        long overlapRegionId = regions[i].getRegionId() + j * 1000;
        RegionInfo overlapRegion =
          RegionInfoBuilder.newBuilder(tableName).setStartKey(regions[i].getStartKey())
            .setEndKey(regions[i].getEndKey()).setRegionId(overlapRegionId).build();
        overlapRegions.add(overlapRegion);

        // Register into RegionStates (CLOSED state)
        RegionStateNode node = regionStates.getOrCreateRegionStateNode(overlapRegion);
        node.setState(RegionState.State.CLOSED);
      }
    }

    // Write into meta
    MetaTableAccessor.addRegionsToMeta(UTIL.getConnection(), overlapRegions, 1);

    // Verify there are 8 regions (2 normal + 6 overlap)
    assertEquals(8, regionStates.getRegionsOfTable(tableName).size(),
      "Should have 8 regions (2 normal + 6 overlap)");

    // Disable the table
    UTIL.getAdmin().disableTable(tableName);

    // preserveSplits=true should fail due to duplicate startKey
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new TruncateTableProcedure(procExec.getEnvironment(), tableName, true));

    Procedure<?> result = procExec.getResult(procId);
    assertTrue(result.isFailed(),
      "Truncate should fail with multiple overlap regions when preserveSplits=true");
    Throwable cause = ProcedureTestingUtility.getExceptionCause(result);
    assertTrue(cause instanceof HBaseIOException,
      "Expected HBaseIOException but got: " + cause.getClass().getName());
    assertTrue(cause.getMessage().contains("Found duplicate startKey region"),
      "Error message should mention duplicate startKey");
    LOG.info("Truncate correctly failed with: " + cause.getMessage());
  }
}
