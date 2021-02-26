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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * Test that the {@link SnapshotDescription} helper is helping correctly.
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestSnapshotDescriptionUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotDescriptionUtils.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static FileSystem fs;
  private static Path root;

  @BeforeClass
  public static void setupFS() throws Exception {
    fs = UTIL.getTestFileSystem();
    root = new Path(UTIL.getDataTestDir(), "hbase");
  }

  @After
  public void cleanupFS() throws Exception {
    if (fs.exists(root)) {
      if (!fs.delete(root, true)) {
        throw new IOException("Failed to delete root test dir: " + root);
      }
      if (!fs.mkdirs(root)) {
        throw new IOException("Failed to create root test dir: " + root);
      }
    }
    EnvironmentEdgeManagerTestHelper.reset();
  }

  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotDescriptionUtils.class);

  @Test
  public void testValidateMissingTableName() throws IOException {
    Configuration conf = new Configuration(false);
    try {
      SnapshotDescriptionUtils.validate(SnapshotDescription.newBuilder().setName("fail").build(),
        conf);
      fail("Snapshot was considered valid without a table name");
    } catch (IllegalArgumentException e) {
      LOG.debug("Correctly failed when snapshot doesn't have a tablename");
    }
  }

  /**
   * Test that we throw an exception if there is no working snapshot directory when we attempt to
   * 'complete' the snapshot
   * @throws Exception on failure
   */
  @Test
  public void testCompleteSnapshotWithNoSnapshotDirectoryFailure() throws Exception {
    Path snapshotDir = new Path(root, HConstants.SNAPSHOT_DIR_NAME);
    Path tmpDir = new Path(snapshotDir, ".tmp");
    Path workingDir = new Path(tmpDir, "not_a_snapshot");
    Configuration conf = new Configuration();
    FileSystem workingFs = workingDir.getFileSystem(conf);
    assertFalse("Already have working snapshot dir: " + workingDir
        + " but shouldn't. Test file leak?", fs.exists(workingDir));
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName("snapshot").build();
    Path finishedDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, snapshotDir);

    try {
      SnapshotDescriptionUtils.completeSnapshot(finishedDir, workingDir, fs, workingFs, conf);
      fail("Shouldn't successfully complete move of a non-existent directory.");
    } catch (IOException e) {
      LOG.info("Correctly failed to move non-existant directory: " + e.getMessage());
    }
  }

  @Test
  public void testIsSubDirectoryWorks() {
    Path rootDir = new Path("hdfs://root/.hbase-snapshot/");

    assertFalse(SnapshotDescriptionUtils.isSubDirectoryOf(rootDir, rootDir));
    assertFalse(SnapshotDescriptionUtils.isSubDirectoryOf(
        new Path("hdfs://root/.hbase-snapshotdir"), rootDir));
    assertFalse(SnapshotDescriptionUtils.isSubDirectoryOf(
        new Path("hdfs://root/.hbase-snapshot"), rootDir));
    assertFalse(SnapshotDescriptionUtils.isSubDirectoryOf(
        new Path("hdfs://.hbase-snapshot"), rootDir));
    assertFalse(SnapshotDescriptionUtils.isSubDirectoryOf(
        new Path("hdfs://.hbase-snapshot/.tmp"), rootDir));
    assertFalse(SnapshotDescriptionUtils.isSubDirectoryOf(new Path("hdfs://root"), rootDir));
    assertTrue(SnapshotDescriptionUtils.isSubDirectoryOf(
        new Path("hdfs://root/.hbase-snapshot/.tmp"), rootDir));
    assertTrue(SnapshotDescriptionUtils.isSubDirectoryOf(
        new Path("hdfs://root/.hbase-snapshot/.tmp/snapshot"), rootDir));

    assertFalse(SnapshotDescriptionUtils.isSubDirectoryOf(
        new Path("s3://root/.hbase-snapshot/"), rootDir));
    assertFalse(SnapshotDescriptionUtils.isSubDirectoryOf(new Path("s3://root"), rootDir));
    assertFalse(SnapshotDescriptionUtils.isSubDirectoryOf(
        new Path("s3://root/.hbase-snapshot/.tmp/snapshot"), rootDir));
  }

  @Test
  public void testIsWithinWorkingDir() throws IOException {
    Configuration conf = new Configuration();
    conf.set(HConstants.HBASE_DIR, "hdfs://localhost/root/");

    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
        new Path("hdfs://localhost/root/"), conf));
    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
        new Path("hdfs://localhost/root/.hbase-snapshotdir"), conf));
    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
        new Path("hdfs://localhost/root/.hbase-snapshot"), conf));
    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
        new Path("hdfs://localhost/.hbase-snapshot"), conf));
    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
        new Path("hdfs://localhost/.hbase-snapshot/.tmp"), conf));
    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
      new Path("hdfs://localhost/root"), conf));
    assertTrue(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
        new Path("hdfs://localhost/root/.hbase-snapshot/.tmp"), conf));
    assertTrue(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
        new Path("hdfs://localhost/root/.hbase-snapshot/.tmp/snapshot"), conf));

    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
        new Path("s3://localhost/root/.hbase-snapshot/"), conf));
    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
      new Path("s3://localhost/root"), conf));
    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
        new Path("s3://localhost/root/.hbase-snapshot/.tmp/snapshot"), conf));

    // for local mode
    conf = HBaseConfiguration.create();
    String hbsaeDir = conf.get(HConstants.HBASE_DIR);

    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
      new Path("file:" + hbsaeDir + "/"), conf));
    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
      new Path("file:" + hbsaeDir + "/.hbase-snapshotdir"), conf));
    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
      new Path("file:" + hbsaeDir + "/.hbase-snapshot"), conf));
    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
      new Path("file:/.hbase-snapshot"), conf));
    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
      new Path("file:/.hbase-snapshot/.tmp"), conf));
    assertFalse(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
      new Path("file:" + hbsaeDir), conf));
    assertTrue(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
      new Path("file:" + hbsaeDir + "/.hbase-snapshot/.tmp"), conf));
    assertTrue(SnapshotDescriptionUtils.isWithinDefaultWorkingDir(
      new Path("file:" + hbsaeDir + "/.hbase-snapshot/.tmp/snapshot"), conf));
  }
}
