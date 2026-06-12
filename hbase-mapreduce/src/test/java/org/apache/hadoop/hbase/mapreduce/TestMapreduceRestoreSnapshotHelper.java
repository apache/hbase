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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTTLExpiredException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link MapreduceRestoreSnapshotHelper}, the thin MapReduce wrapper that guards against
 * accidental production data loss (HBASE-29435) and then delegates the actual restore/clone to
 * {@code RestoreSnapshotHelper} using the MapReduce-local archiver.
 */
@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
public class TestMapreduceRestoreSnapshotHelper {

  protected final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  protected Configuration conf;
  protected FileSystem fs;
  protected Path rootDir;

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
    rootDir = TEST_UTIL.getDataTestDir("testRestore");
    fs = TEST_UTIL.getTestFileSystem();
    conf = TEST_UTIL.getConfiguration();
    CommonFSUtils.setRootDir(conf, rootDir);
    // Turn off balancer so it doesn't cut in and mess up our placements.
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterEach
  public void tearDown() throws Exception {
    fs.delete(TEST_UTIL.getDataTestDir(), true);
  }

  /**
   * A fresh restore directory outside the HBase root must be accepted and must restore via the clone
   * path (HFileLinks land under restoreDir, never inside the production data directory).
   */
  @Test
  public void testNoHFileLinkInRootDir() throws IOException {
    rootDir = TEST_UTIL.getDefaultRootDirPath();
    CommonFSUtils.setRootDir(conf, rootDir);
    fs = rootDir.getFileSystem(conf);

    TableName tableName = TableName.valueOf("testNoHFileLinkInRootDir");
    String snapshotName = tableName.getNameAsString() + "-snapshot";
    createTableAndSnapshot(tableName, snapshotName);

    Path restoreDir = new Path("/hbase/.tmp-restore");
    MapreduceRestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir,
      snapshotName);
    checkNoHFileLinkInTableDir(tableName);
  }

  /** Restoring an expired snapshot must surface the TTL error from the delegated helper. */
  @Test
  public void testCopyExpiredSnapshotForScanner() throws IOException, InterruptedException {
    rootDir = TEST_UTIL.getDefaultRootDirPath();
    CommonFSUtils.setRootDir(conf, rootDir);
    TableName tableName = TableName.valueOf("testCopyExpiredSnapshotForScanner");
    String snapshotName = tableName.getNameAsString() + "-snapshot";
    Path restoreDir = new Path("/hbase/.tmp-expired-snapshot/copySnapshotDest");
    // create table and put some data into the table
    byte[] columnFamily = Bytes.toBytes("A");
    Table table = TEST_UTIL.createTable(tableName, columnFamily);
    TEST_UTIL.loadTable(table, columnFamily);
    // create snapshot with ttl = 10 sec
    Map<String, Object> properties = new HashMap<>();
    properties.put("TTL", 10);
    org.apache.hadoop.hbase.client.SnapshotDescription snapshotDesc =
      new org.apache.hadoop.hbase.client.SnapshotDescription(snapshotName, tableName,
        SnapshotType.FLUSH, null, EnvironmentEdgeManager.currentTime(), -1, properties);
    TEST_UTIL.getAdmin().snapshot(snapshotDesc);
    boolean isExist = TEST_UTIL.getAdmin().listSnapshots().stream()
      .anyMatch(ele -> snapshotName.equals(ele.getName()));
    assertTrue(isExist);
    int retry = 6;
    while (
      !SnapshotDescriptionUtils.isExpiredSnapshot(snapshotDesc.getTtl(),
        snapshotDesc.getCreationTime(), EnvironmentEdgeManager.currentTime()) && retry > 0
    ) {
      retry--;
      Thread.sleep(10 * 1000);
    }
    boolean isExpiredSnapshot = SnapshotDescriptionUtils.isExpiredSnapshot(snapshotDesc.getTtl(),
      snapshotDesc.getCreationTime(), EnvironmentEdgeManager.currentTime());
    assertTrue(isExpiredSnapshot);
    assertThrows(SnapshotTTLExpiredException.class, () -> MapreduceRestoreSnapshotHelper
      .copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName));
  }

  /**
   * Guard (HBASE-29435): a restore directory exactly equal to the HBase root directory must be
   * rejected before any filesystem work, to avoid archiving/deleting production data.
   */
  @Test
  public void testRejectRestoreDirEqualToRootDir() {
    Path root = new Path("/hbase");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
      MapreduceRestoreSnapshotHelper.copySnapshotForScanner(conf, fs, root, root, "snap"));
    assertTrue(e.getMessage().contains("BLOCKED: MapReduce restore directory cannot be the HBase root directory"), e.getMessage());
  }

  /**
   * Guard (HBASE-29435): a restore directory nested under the HBase root directory must be rejected.
   */
  @Test
  public void testRejectRestoreDirUnderRootDir() {
    Path root = new Path("/hbase");
    Path restoreDir = new Path(root, "data/.tmp-restore");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
      MapreduceRestoreSnapshotHelper.copySnapshotForScanner(conf, fs, root, restoreDir, "snap"));
    assertTrue(e.getMessage().contains("BLOCKED: MapReduce restore directory cannot be the HBase root directory"), e.getMessage());
  }

  protected void createTableAndSnapshot(TableName tableName, String snapshotName)
    throws IOException {
    byte[] column = Bytes.toBytes("A");
    Table table = TEST_UTIL.createTable(tableName, column, 2);
    TEST_UTIL.loadTable(table, column);
    TEST_UTIL.getAdmin().snapshot(snapshotName, tableName);
  }

  private void checkNoHFileLinkInTableDir(TableName tableName) throws IOException {
    Path[] tableDirs = new Path[] { CommonFSUtils.getTableDir(rootDir, tableName),
      CommonFSUtils.getTableDir(new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY), tableName),
      CommonFSUtils.getTableDir(MobUtils.getMobHome(rootDir), tableName) };
    for (Path tableDir : tableDirs) {
      assertFalse(hasHFileLink(tableDir));
    }
  }

  private boolean hasHFileLink(Path tableDir) throws IOException {
    if (fs.exists(tableDir)) {
      RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(tableDir, true);
      while (iterator.hasNext()) {
        LocatedFileStatus fileStatus = iterator.next();
        if (fileStatus.isFile() && HFileLink.isHFileLink(fileStatus.getPath())) {
          return true;
        }
      }
    }
    return false;
  }
}
