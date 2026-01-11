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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestReopenTableRegionsProcedureSpecificRegions {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReopenTableRegionsProcedureSpecificRegions.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] CF = Bytes.toBytes("cf");

  private static MiniHBaseCluster singleProcessHBaseCluster;

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    singleProcessHBaseCluster = UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
    if (Objects.nonNull(singleProcessHBaseCluster)) {
      singleProcessHBaseCluster.close();
    }
  }

  private ProcedureExecutor<MasterProcedureEnv> getProcExec() {
    return UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
  }

  @Test
  public void testInvalidRegionNamesThrowsException() throws Exception {
    TableName tableName = TableName.valueOf("TestInvalidRegions");
    try (Table ignored = UTIL.createTable(tableName, CF)) {

      List<RegionInfo> regions = UTIL.getAdmin().getRegions(tableName);
      assertFalse("Table should have at least one region", regions.isEmpty());

      List<byte[]> invalidRegionNames =
        Collections.singletonList(Bytes.toBytes("non-existent-region-name"));

      ReopenTableRegionsProcedure proc =
        new ReopenTableRegionsProcedure(tableName, invalidRegionNames, 0L, Integer.MAX_VALUE);

      long procId = getProcExec().submitProcedure(proc);
      UTIL.waitFor(60000, proc::isFailed);

      Throwable cause = ProcedureTestingUtility.getExceptionCause(proc);
      assertTrue("Expected UnknownRegionException, got: " + cause.getClass().getName(),
        cause instanceof UnknownRegionException);
      assertTrue("Error message should contain region name",
        cause.getMessage().contains("non-existent-region-name"));
      assertTrue("Error message should contain table name",
        cause.getMessage().contains(tableName.getNameAsString()));
    }
  }

  @Test
  public void testMixedValidInvalidRegions() throws Exception {
    TableName tableName = TableName.valueOf("TestMixedRegions");
    try (Table ignored = UTIL.createTable(tableName, CF)) {

      List<RegionInfo> actualRegions = UTIL.getAdmin().getRegions(tableName);
      assertFalse("Table should have at least one region", actualRegions.isEmpty());

      List<byte[]> mixedRegionNames = new ArrayList<>();
      mixedRegionNames.add(actualRegions.get(0).getRegionName());
      mixedRegionNames.add(Bytes.toBytes("invalid-region-1"));
      mixedRegionNames.add(Bytes.toBytes("invalid-region-2"));

      ReopenTableRegionsProcedure proc =
        new ReopenTableRegionsProcedure(tableName, mixedRegionNames, 0L, Integer.MAX_VALUE);

      long procId = getProcExec().submitProcedure(proc);
      UTIL.waitFor(60000, proc::isFailed);

      Throwable cause = ProcedureTestingUtility.getExceptionCause(proc);
      assertTrue("Expected UnknownRegionException", cause instanceof UnknownRegionException);
      assertTrue("Error message should contain first invalid region",
        cause.getMessage().contains("invalid-region-1"));
      assertTrue("Error message should contain second invalid region",
        cause.getMessage().contains("invalid-region-2"));
    }
  }

  @Test
  public void testSpecificRegionsReopenWithThrottling() throws Exception {
    TableName tableName = TableName.valueOf("TestSpecificThrottled");

    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
      .setValue(ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY, "100")
      .setValue(ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_SIZE_MAX_KEY, "2").build();

    UTIL.getAdmin().createTable(td, Bytes.toBytes("a"), Bytes.toBytes("z"), 5);

    List<RegionInfo> allRegions = UTIL.getAdmin().getRegions(tableName);
    assertEquals(5, allRegions.size());

    List<byte[]> specificRegionNames =
      allRegions.subList(0, 3).stream().map(RegionInfo::getRegionName).collect(Collectors.toList());

    ReopenTableRegionsProcedure proc = ReopenTableRegionsProcedure.throttled(
      UTIL.getConfiguration(), UTIL.getAdmin().getDescriptor(tableName), specificRegionNames);

    long procId = getProcExec().submitProcedure(proc);
    ProcedureTestingUtility.waitProcedure(getProcExec(), procId);

    assertFalse("Procedure should succeed", proc.isFailed());
    assertEquals("Should reopen exactly 3 regions", 3, proc.getRegionsReopened());
    assertTrue("Should process multiple batches with batch size 2",
      proc.getBatchesProcessed() >= 2);
  }

  @Test
  public void testEmptyRegionListReopensAll() throws Exception {
    TableName tableName = TableName.valueOf("TestEmptyList");

    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build();

    UTIL.getAdmin().createTable(td, Bytes.toBytes("a"), Bytes.toBytes("z"), 5);

    List<RegionInfo> allRegions = UTIL.getAdmin().getRegions(tableName);
    assertEquals(5, allRegions.size());

    ReopenTableRegionsProcedure proc = ReopenTableRegionsProcedure
      .throttled(UTIL.getConfiguration(), UTIL.getAdmin().getDescriptor(tableName));

    long procId = getProcExec().submitProcedure(proc);
    ProcedureTestingUtility.waitProcedure(getProcExec(), procId);

    assertFalse("Procedure should succeed", proc.isFailed());
    assertEquals("Should reopen all 5 regions", 5, proc.getRegionsReopened());
  }

  @Test
  public void testDisabledTableSkipsReopen() throws Exception {
    TableName tableName = TableName.valueOf("TestDisabledTable");
    try (Table ignored = UTIL.createTable(tableName, CF)) {
      UTIL.getAdmin().disableTable(tableName);

      ReopenTableRegionsProcedure proc = ReopenTableRegionsProcedure
        .throttled(UTIL.getConfiguration(), UTIL.getAdmin().getDescriptor(tableName));

      long procId = getProcExec().submitProcedure(proc);
      ProcedureTestingUtility.waitProcedure(getProcExec(), procId);

      assertFalse("Procedure should succeed", proc.isFailed());
      assertEquals("Should not reopen any regions for disabled table", 0,
        proc.getRegionsReopened());
    }
  }

  @Test
  public void testReopenRegionsThrottledWithLargeTable() throws Exception {
    TableName tableName = TableName.valueOf("TestLargeTable");

    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
      .setValue(ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY, "50")
      .setValue(ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_SIZE_MAX_KEY, "3").build();

    UTIL.getAdmin().createTable(td, Bytes.toBytes("a"), Bytes.toBytes("z"), 10);

    List<RegionInfo> regions = UTIL.getAdmin().getRegions(tableName);
    assertEquals(10, regions.size());

    ReopenTableRegionsProcedure proc = ReopenTableRegionsProcedure
      .throttled(UTIL.getConfiguration(), UTIL.getAdmin().getDescriptor(tableName));

    long procId = getProcExec().submitProcedure(proc);
    ProcedureTestingUtility.waitProcedure(getProcExec(), procId);

    assertFalse("Procedure should succeed", proc.isFailed());
    assertEquals("Should reopen all 10 regions", 10, proc.getRegionsReopened());
    assertTrue("Should process multiple batches", proc.getBatchesProcessed() >= 4);
  }

  @Test
  public void testConfigurationPrecedence() throws Exception {
    TableName tableName = TableName.valueOf("TestConfigPrecedence");

    Configuration conf = UTIL.getConfiguration();
    conf.setLong(ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY, 1000);
    conf.setInt(ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_SIZE_MAX_KEY, 5);

    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
      .setValue(ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY, "2000")
      .setValue(ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_SIZE_MAX_KEY, "2").build();

    UTIL.getAdmin().createTable(td);

    ReopenTableRegionsProcedure proc =
      ReopenTableRegionsProcedure.throttled(conf, UTIL.getAdmin().getDescriptor(tableName));

    assertEquals("Table descriptor config should override global config", 2000,
      proc.getReopenBatchBackoffMillis());
  }

  @Test
  public void testThrottledVsUnthrottled() throws Exception {
    TableName tableName = TableName.valueOf("TestThrottledVsUnthrottled");

    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
      .setValue(ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY, "1000")
      .setValue(ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_SIZE_MAX_KEY, "2").build();

    UTIL.getAdmin().createTable(td, Bytes.toBytes("a"), Bytes.toBytes("z"), 5);

    List<RegionInfo> regions = UTIL.getAdmin().getRegions(tableName);
    List<byte[]> regionNames =
      regions.stream().map(RegionInfo::getRegionName).collect(Collectors.toList());

    ReopenTableRegionsProcedure unthrottledProc =
      new ReopenTableRegionsProcedure(tableName, regionNames);
    assertEquals("Unthrottled should use default (0ms)", 0,
      unthrottledProc.getReopenBatchBackoffMillis());

    ReopenTableRegionsProcedure throttledProc = ReopenTableRegionsProcedure
      .throttled(UTIL.getConfiguration(), UTIL.getAdmin().getDescriptor(tableName), regionNames);
    assertEquals("Throttled should use table config (1000ms)", 1000,
      throttledProc.getReopenBatchBackoffMillis());
  }

  @Test
  public void testExceptionInProcedureExecution() throws Exception {
    TableName tableName = TableName.valueOf("TestExceptionInExecution");
    try (Table ignored = UTIL.createTable(tableName, CF)) {

      List<byte[]> invalidRegionNames =
        Collections.singletonList(Bytes.toBytes("nonexistent-region"));

      ReopenTableRegionsProcedure proc =
        new ReopenTableRegionsProcedure(tableName, invalidRegionNames, 0L, Integer.MAX_VALUE);

      long procId = getProcExec().submitProcedure(proc);
      UTIL.waitFor(60000, () -> getProcExec().isFinished(procId));

      Procedure<?> result = getProcExec().getResult(procId);
      assertTrue("Procedure should have failed", result.isFailed());

      Throwable cause = ProcedureTestingUtility.getExceptionCause(result);
      assertTrue("Should be UnknownRegionException", cause instanceof UnknownRegionException);
    }
  }

  @Test
  public void testSerializationWithRegionNames() throws Exception {
    TableName tableName = TableName.valueOf("TestSerialization");
    try (Table ignored = UTIL.createTable(tableName, CF)) {

      List<RegionInfo> regions = UTIL.getAdmin().getRegions(tableName);
      List<byte[]> regionNames =
        regions.stream().map(RegionInfo::getRegionName).collect(Collectors.toList());

      ReopenTableRegionsProcedure proc =
        new ReopenTableRegionsProcedure(tableName, regionNames, 500L, 3);

      long procId = getProcExec().submitProcedure(proc);
      ProcedureTestingUtility.waitProcedure(getProcExec(), procId);

      assertEquals("TableName should be preserved", tableName, proc.getTableName());
      assertEquals("Backoff should be preserved", 500L, proc.getReopenBatchBackoffMillis());
    }
  }

  @Test
  public void testAllRegionsWithValidNames() throws Exception {
    TableName tableName = TableName.valueOf("TestAllValidRegions");
    try (Table ignored = UTIL.createTable(tableName, CF)) {

      List<RegionInfo> actualRegions = UTIL.getAdmin().getRegions(tableName);
      assertFalse("Table should have regions", actualRegions.isEmpty());

      List<byte[]> validRegionNames =
        actualRegions.stream().map(RegionInfo::getRegionName).collect(Collectors.toList());

      ReopenTableRegionsProcedure proc =
        new ReopenTableRegionsProcedure(tableName, validRegionNames, 0L, Integer.MAX_VALUE);

      long procId = getProcExec().submitProcedure(proc);
      ProcedureTestingUtility.waitProcedure(getProcExec(), procId);

      assertFalse("Procedure should succeed with all valid regions", proc.isFailed());
      assertEquals("Should reopen all specified regions", actualRegions.size(),
        proc.getRegionsReopened());
    }
  }

  @Test
  public void testSingleInvalidRegion() throws Exception {
    TableName tableName = TableName.valueOf("TestSingleInvalid");
    try (Table ignored = UTIL.createTable(tableName, CF)) {

      List<byte[]> invalidRegionNames =
        Collections.singletonList(Bytes.toBytes("totally-fake-region"));

      ReopenTableRegionsProcedure proc =
        new ReopenTableRegionsProcedure(tableName, invalidRegionNames, 0L, Integer.MAX_VALUE);

      long procId = getProcExec().submitProcedure(proc);
      UTIL.waitFor(60000, proc::isFailed);

      Throwable cause = ProcedureTestingUtility.getExceptionCause(proc);
      assertTrue("Expected UnknownRegionException", cause instanceof UnknownRegionException);
      assertTrue("Error message should list the invalid region",
        cause.getMessage().contains("totally-fake-region"));
    }
  }

  @Test
  public void testRecoveryAfterValidationFailure() throws Exception {
    TableName tableName = TableName.valueOf("TestRecoveryValidation");
    try (Table ignored = UTIL.createTable(tableName, CF)) {

      List<byte[]> invalidRegionNames =
        Collections.singletonList(Bytes.toBytes("invalid-for-recovery"));

      ReopenTableRegionsProcedure proc =
        new ReopenTableRegionsProcedure(tableName, invalidRegionNames, 0L, Integer.MAX_VALUE);

      ProcedureExecutor<MasterProcedureEnv> procExec = getProcExec();
      long procId = procExec.submitProcedure(proc);

      UTIL.waitFor(60000, () -> procExec.isFinished(procId));

      Procedure<?> result = procExec.getResult(procId);
      assertTrue("Procedure should fail validation", result.isFailed());

      Throwable cause = ProcedureTestingUtility.getExceptionCause(result);
      assertTrue("Should be UnknownRegionException", cause instanceof UnknownRegionException);
      assertTrue("Error should mention the invalid region",
        cause.getMessage().contains("invalid-for-recovery"));
    }
  }

  @Test
  public void testEmptyTableWithNoRegions() throws Exception {
    TableName tableName = TableName.valueOf("TestEmptyTable");

    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build();

    UTIL.getAdmin().createTable(td);

    List<RegionInfo> regions = UTIL.getAdmin().getRegions(tableName);
    int regionCount = regions.size();

    ReopenTableRegionsProcedure proc = ReopenTableRegionsProcedure
      .throttled(UTIL.getConfiguration(), UTIL.getAdmin().getDescriptor(tableName));

    long procId = getProcExec().submitProcedure(proc);
    ProcedureTestingUtility.waitProcedure(getProcExec(), procId);

    assertFalse("Procedure should complete successfully even with no regions", proc.isFailed());
    assertEquals("Should handle empty table gracefully", regionCount, proc.getRegionsReopened());
  }

  @Test
  public void testConfigChangeDoesNotAffectRunningProcedure() throws Exception {
    TableName tableName = TableName.valueOf("TestConfigChange");

    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
      .setValue(ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY, "1000")
      .setValue(ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_SIZE_MAX_KEY, "2").build();

    UTIL.getAdmin().createTable(td, Bytes.toBytes("a"), Bytes.toBytes("z"), 5);

    ReopenTableRegionsProcedure proc = ReopenTableRegionsProcedure
      .throttled(UTIL.getConfiguration(), UTIL.getAdmin().getDescriptor(tableName));

    assertEquals("Initial config should be 1000ms", 1000L, proc.getReopenBatchBackoffMillis());

    TableDescriptor modifiedTd = TableDescriptorBuilder.newBuilder(td)
      .setValue(ReopenTableRegionsProcedure.PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY, "5000").build();
    UTIL.getAdmin().modifyTable(modifiedTd);

    assertEquals("Running procedure should keep original config", 1000L,
      proc.getReopenBatchBackoffMillis());

    long procId = getProcExec().submitProcedure(proc);
    ProcedureTestingUtility.waitProcedure(getProcExec(), procId);

    assertFalse("Procedure should complete successfully", proc.isFailed());
  }
}
