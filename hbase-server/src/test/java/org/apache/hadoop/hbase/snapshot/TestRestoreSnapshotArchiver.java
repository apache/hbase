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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.MergeTableRegionsProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link RestoreSnapshotHelper} routes its archiving through the injected
 * {@link RestoreSnapshotArchiver}: the fresh-clone path archives nothing, while a re-restore that
 * removes regions drives the archiving through the supplied archiver rather than the default.
 */
@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
public class TestRestoreSnapshotArchiver {

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private Configuration conf;
  private FileSystem fs;
  private Path rootDir;

  /** Counting archiver that delegates to the default so the restore still behaves correctly. */
  private static final class RecordingArchiver implements RestoreSnapshotArchiver {
    final AtomicInteger regionCount = new AtomicInteger();
    final AtomicInteger familyCount = new AtomicInteger();

    @Override
    public void archiveRegion(Configuration conf, FileSystem fs, RegionInfo info, Path rootDir,
      Path tableDir) throws IOException {
      regionCount.incrementAndGet();
      RestoreSnapshotArchiver.DEFAULT.archiveRegion(conf, fs, info, rootDir, tableDir);
    }

    @Override
    public void archiveFamilyByFamilyDir(FileSystem fs, Configuration conf, RegionInfo parent,
      Path familyDir, byte[] family) throws IOException {
      familyCount.incrementAndGet();
      RestoreSnapshotArchiver.DEFAULT.archiveFamilyByFamilyDir(fs, conf, parent, familyDir, family);
    }
  }

  @BeforeAll
  public static void setupCluster() throws Exception {
    TEST_UTIL.getConfiguration().setInt(AssignmentManager.ASSIGN_MAX_ATTEMPTS, 3);
    TEST_UTIL.startMiniCluster();
  }

  @AfterAll
  public static void tearDownCluster() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeEach
  public void setup() throws Exception {
    rootDir = TEST_UTIL.getDefaultRootDirPath();
    fs = rootDir.getFileSystem(TEST_UTIL.getConfiguration());
    conf = TEST_UTIL.getConfiguration();
    CommonFSUtils.setRootDir(conf, rootDir);
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterEach
  public void tearDown() throws Exception {
    fs.delete(TEST_UTIL.getDataTestDir(), true);
  }

  @Test
  public void testDefaultArchiverNotNull() {
    assertNotNull(RestoreSnapshotArchiver.DEFAULT);
  }

  /** The default archiver must move a family's store files into the archive directory. */
  @Test
  public void testDefaultArchivesFamilyByFamilyDir() throws IOException {
    TableName tableName = TableName.valueOf("testDefaultArchiveFamily");
    RegionInfo region = RegionInfoBuilder.newBuilder(tableName).build();
    byte[] family = Bytes.toBytes("cf");
    Path familyDir = new Path(FSUtils.getRegionDirFromRootDir(rootDir, region), "cf");
    Path storeFile = new Path(familyDir, "f1");
    fs.mkdirs(familyDir);
    try (FSDataOutputStream out = fs.create(storeFile)) {
      out.write(Bytes.toBytes("data"));
    }
    assertTrue(fs.exists(storeFile));

    RestoreSnapshotArchiver.DEFAULT.archiveFamilyByFamilyDir(fs, conf, region, familyDir, family);

    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(conf, region, family);
    assertTrue(fs.exists(new Path(storeArchiveDir, "f1")), "store file should be moved to archive");
    assertFalse(fs.exists(storeFile), "source store file should be gone after archiving");
  }

  /**
   * A fresh (empty) restore directory always takes the clone path, which only creates HFileLinks
   * and must never invoke the archiver.
   */
  @Test
  public void testFreshCloneDoesNotInvokeArchiver() throws IOException {
    TableName tableName = TableName.valueOf("testFreshCloneNoArchive");
    String snapshotName = tableName.getNameAsString() + "-snapshot";
    byte[] column = Bytes.toBytes("A");
    Table table = TEST_UTIL.createTable(tableName, column, 2);
    TEST_UTIL.loadTable(table, column);
    TEST_UTIL.getAdmin().snapshot(snapshotName, tableName);

    Path restoreDir = new Path("/hbase/.tmp-archiver/fresh-" + UUID.randomUUID());
    RecordingArchiver archiver = new RecordingArchiver();
    RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName,
      archiver);

    assertEquals(0, archiver.regionCount.get(), "clone path must not archive any region");
    assertEquals(0, archiver.familyCount.get(), "clone path must not archive any family");
  }

  /**
   * Re-restoring a snapshot with fewer regions into an already-populated restore directory must
   * remove the now-absent regions through the <em>injected</em> archiver.
   */
  @Test
  public void testReRestoreInvokesInjectedArchiver() throws IOException, InterruptedException {
    TableName tableName = TableName.valueOf("testReRestoreInjectedArchiver");
    Path restoreDir = new Path("/hbase/.tmp-archiver/restore-dest");
    byte[] columnFamily = Bytes.toBytes("A");

    Table table = TEST_UTIL.createTable(tableName, new byte[][] { columnFamily },
      new byte[][] { new byte[] { 'b' }, new byte[] { 'd' } });
    Put put1 = new Put(Bytes.toBytes("a"));
    put1.addColumn(columnFamily, Bytes.toBytes("q"), Bytes.toBytes("val1"));
    table.put(put1);
    Put put2 = new Put(Bytes.toBytes("b"));
    put2.addColumn(columnFamily, Bytes.toBytes("q"), Bytes.toBytes("val2"));
    table.put(put2);
    Put put3 = new Put(Bytes.toBytes("d"));
    put3.addColumn(columnFamily, Bytes.toBytes("q"), Bytes.toBytes("val3"));
    table.put(put3);
    TEST_UTIL.getAdmin().flush(tableName);

    String snapshotOne = tableName.getNameAsString() + "-snapshot-one";
    createSnapshot(tableName, snapshotOne);
    // First restore populates the restore directory with the original (3-region) layout.
    RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotOne);

    flipCompactions(false);
    mergeRegions(tableName, 2);
    String snapshotTwo = tableName.getNameAsString() + "-snapshot-two";
    createSnapshot(tableName, snapshotTwo);

    // Second restore into the SAME directory: the merged-away regions become "regions to remove",
    // which RestoreSnapshotHelper archives through the injected archiver.
    RecordingArchiver archiver = new RecordingArchiver();
    RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotTwo,
      archiver);
    flipCompactions(true);

    assertTrue(archiver.regionCount.get() >= 1,
      "expected at least one region to be archived via the injected archiver, but got "
        + archiver.regionCount.get());
  }

  private void createSnapshot(TableName tableName, String snapshotName) throws IOException {
    org.apache.hadoop.hbase.client.SnapshotDescription desc =
      new org.apache.hadoop.hbase.client.SnapshotDescription(snapshotName, tableName,
        SnapshotType.FLUSH, null, EnvironmentEdgeManager.currentTime(), -1);
    TEST_UTIL.getAdmin().snapshot(desc);
    assertTrue(TEST_UTIL.getAdmin().listSnapshots().stream()
      .anyMatch(ele -> snapshotName.equals(ele.getName())));
  }

  private void flipCompactions(boolean isEnable) {
    int numLiveRegionServers = TEST_UTIL.getHBaseCluster().getNumLiveRegionServers();
    for (int serverNumber = 0; serverNumber < numLiveRegionServers; serverNumber++) {
      HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(serverNumber);
      regionServer.getCompactSplitThread().setCompactionsEnabled(isEnable);
    }
  }

  private void mergeRegions(TableName tableName, int mergeCount) throws IOException {
    List<RegionInfo> ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tableName);
    int originalRegionCount = ris.size();
    assertTrue(originalRegionCount > mergeCount);
    RegionInfo[] regionsToMerge = ris.subList(0, mergeCount).toArray(new RegionInfo[] {});
    final ProcedureExecutor<MasterProcedureEnv> procExec =
      TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    MergeTableRegionsProcedure proc =
      new MergeTableRegionsProcedure(procExec.getEnvironment(), regionsToMerge, true);
    long procId = procExec.submitProcedure(proc);
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    assertEquals(originalRegionCount - mergeCount + 1,
      MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tableName).size());
  }
}
