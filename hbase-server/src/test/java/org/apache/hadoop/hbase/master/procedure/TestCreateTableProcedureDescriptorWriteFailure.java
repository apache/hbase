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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Test for HBASE-30539: {@link CreateTableProcedure} must not move on to {@code hbase:meta} and
 * region assignment when the table descriptor could not be written.
 * <p>
 * {@code FSTableDescriptors.writeTableDescriptor} retries 10 times. If every attempt fails it must
 * throw, so that the create procedure stays in {@code CREATE_TABLE_WRITE_FS_LAYOUT} and retries,
 * instead of publishing a table whose region servers can never load a descriptor ("Missing table
 * descriptor for ...") and whose assign procedure retries forever.
 */
@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestCreateTableProcedureDescriptorWriteFailure {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  /** Same as {@code maxAttempts} in {@code FSTableDescriptors.writeTableDescriptor}. */
  private static final int WRITE_ATTEMPTS = 10;

  /** Substring identifying the file creations to fail; null means do not inject. */
  private static volatile String failPathContains = null;
  /** Stop injecting once this many failures were injected; default is to fail forever. */
  private static volatile int maxInjectedFailures = Integer.MAX_VALUE;
  private static final AtomicInteger INJECTED_FAILURES = new AtomicInteger();

  /**
   * A {@link DistributedFileSystem} that refuses to create any file whose path contains
   * {@link #failPathContains}, simulating the NameNode answering "could only be written to 0 of the
   * 1 minReplication nodes".
   */
  public static class FailingTableInfoFileSystem extends DistributedFileSystem {
    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> cflags,
      int bufferSize, short replication, long blockSize, Progressable progress,
      Options.ChecksumOpt checksumOpt) throws IOException {
      String marker = failPathContains;
      if (
        marker != null && f.toString().contains(marker)
          && INJECTED_FAILURES.get() < maxInjectedFailures
      ) {
        INJECTED_FAILURES.incrementAndGet();
        throw new IOException("Injected HDFS failure creating " + f);
      }
      return super.create(f, permission, cflags, bufferSize, replication, blockSize, progress,
        checksumOpt);
    }
  }

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setClass("fs.hdfs.impl", FailingTableInfoFileSystem.class,
      FileSystem.class);
    UTIL.getConfiguration().setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    UTIL.startMiniCluster(1);
  }

  @AfterAll
  public static void tearDownCluster() throws Exception {
    failPathContains = null;
    UTIL.shutdownMiniCluster();
  }

  @AfterEach
  public void resetInjection() {
    failPathContains = null;
    maxInjectedFailures = Integer.MAX_VALUE;
    INJECTED_FAILURES.set(0);
  }

  /**
   * Every write of the table descriptor fails, so the {@value #WRITE_ATTEMPTS} in-method attempts
   * of {@code writeTableDescriptor} are exhausted, over and over. The create procedure must keep
   * retrying {@code CREATE_TABLE_WRITE_FS_LAYOUT}: it must not fail, finish, add the regions to
   * {@code hbase:meta} or start any {@link TransitRegionStateProcedure} for region assignment. Once
   * the injection is cleared, i.e. HDFS recovers, the same procedure completes, the descriptor is
   * on the file system and the region server can load it.
   */
  @Test
  public void testCreateTableDoesNotProceedWhenTableDescriptorWriteFails() throws Exception {
    final TableName tableName = TableName.valueOf("testDescriptorWriteFailure");
    final HMaster master = UTIL.getHBaseCluster().getMaster();
    final ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    final FileSystem fs = master.getMasterFileSystem().getFileSystem();
    final Path tableDir =
      CommonFSUtils.getTableDir(master.getMasterFileSystem().getRootDir(), tableName);

    TableDescriptor htd = MasterProcedureTestingUtility.createHTD(tableName, "f1");
    RegionInfo[] regions = ModifyRegionUtils.createRegionInfos(htd, null);

    // Make every write of <table>/.tabledesc/.tableinfo.* fail.
    failPathContains = "/" + tableName.getNameAsString() + "/.tabledesc/";
    long procId =
      procExec.submitProcedure(new CreateTableProcedure(procExec.getEnvironment(), htd, regions));

    // 1. The procedure keeps retrying the file system layout step: at least two rounds of
    // WRITE_ATTEMPTS refused writes.
    UTIL.waitFor(60_000, 100, () -> INJECTED_FAILURES.get() >= 2 * WRITE_ATTEMPTS);

    // 2. It must neither have failed, nor finished, nor gone on to hbase:meta / assignment.
    Procedure<MasterProcedureEnv> createProc = procExec.getProcedure(procId);
    assertNotNull(createProc, "create procedure should still be tracked");
    assertFalse(createProc.isFailed(), "retryable failure must not fail the procedure");
    assertFalse(createProc.isFinished(), "procedure cannot finish without a descriptor");
    assertTrue(MetaTableAccessor.getTableRegions(UTIL.getConnection(), tableName).isEmpty(),
      "regions must not be added to hbase:meta before the descriptor is written");
    assertNull(FSTableDescriptors.getTableDescriptorFromFs(fs, tableDir),
      "no table descriptor should exist on the file system");
    for (Procedure<MasterProcedureEnv> p : procExec.getProcedures()) {
      assertFalse(p instanceof TransitRegionStateProcedure && p.getParentProcId() == procId,
        "no region assignment may be started: " + p);
    }

    // 3. Once HDFS recovers, the same procedure completes and the table is usable.
    failPathContains = null;
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));
    assertNotNull(FSTableDescriptors.getTableDescriptorFromFs(fs, tableDir),
      "table descriptor must be on the file system after recovery");
    assertEquals(1, MetaTableAccessor.getTableRegions(UTIL.getConnection(), tableName).size());
    UTIL.waitTableAvailable(tableName);
    assertNotNull(UTIL.getHBaseCluster().getRegionServer(0).getTableDescriptors().get(tableName),
      "region server must be able to load the descriptor");
  }

  /**
   * A few transient failures, fewer than the {@value #WRITE_ATTEMPTS} in-method attempts, are
   * absorbed by the retry loop in {@code writeTableDescriptor}: the procedure never notices.
   */
  @Test
  public void testTransientWriteFailuresAbsorbedByWriteRetries() throws Exception {
    final TableName tableName = TableName.valueOf("testTransientDescriptorWriteFailure");
    final int failures = 3;
    assertTrue(failures < WRITE_ATTEMPTS);

    failPathContains = "/" + tableName.getNameAsString() + "/.tabledesc/";
    maxInjectedFailures = failures;
    long procId = submitCreateTable(tableName);

    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));

    assertEquals(failures, INJECTED_FAILURES.get(), "exactly the injected failures happened");
    assertTableCreatedWithDescriptor(tableName);
  }

  /**
   * More failures than the in-method attempts: the first round of {@value #WRITE_ATTEMPTS} writes
   * is exhausted and {@code CREATE_TABLE_WRITE_FS_LAYOUT} fails, then the procedure retries that
   * step, the write succeeds and the procedure completes without ever having added the table to
   * {@code hbase:meta} in between.
   */
  @Test
  public void testProcedureRetriesAfterWriteAttemptsExhaustedThenSucceeds() throws Exception {
    final TableName tableName = TableName.valueOf("testDescriptorWriteFailureThenSuccess");
    final int failures = WRITE_ATTEMPTS + 2;

    failPathContains = "/" + tableName.getNameAsString() + "/.tabledesc/";
    maxInjectedFailures = failures;
    long procId = submitCreateTable(tableName);

    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));

    assertEquals(failures, INJECTED_FAILURES.get(), "exactly the injected failures happened");
    assertTableCreatedWithDescriptor(tableName);
  }

  private long submitCreateTable(TableName tableName) throws IOException {
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    TableDescriptor htd = MasterProcedureTestingUtility.createHTD(tableName, "f1");
    RegionInfo[] regions = ModifyRegionUtils.createRegionInfos(htd, null);
    return procExec
      .submitProcedure(new CreateTableProcedure(procExec.getEnvironment(), htd, regions));
  }

  private void assertTableCreatedWithDescriptor(TableName tableName) throws Exception {
    HMaster master = UTIL.getHBaseCluster().getMaster();
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path tableDir = CommonFSUtils.getTableDir(master.getMasterFileSystem().getRootDir(), tableName);
    assertNotNull(FSTableDescriptors.getTableDescriptorFromFs(fs, tableDir),
      "table descriptor must be on the file system");
    assertEquals(1, MetaTableAccessor.getTableRegions(UTIL.getConnection(), tableName).size());
    UTIL.waitTableAvailable(tableName);
    assertNotNull(UTIL.getHBaseCluster().getRegionServer(0).getTableDescriptors().get(tableName),
      "region server must be able to load the descriptor");
  }
}
