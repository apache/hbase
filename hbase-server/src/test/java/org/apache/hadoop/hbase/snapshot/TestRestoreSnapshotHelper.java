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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.fs.legacy.io.HFileLink;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils.SnapshotMock;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test the restore/clone operation from a file-system point of view.
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestRestoreSnapshotHelper {
  private static final Log LOG = LogFactory.getLog(TestRestoreSnapshotHelper.class);

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected final static String TEST_HFILE = "abc";

  protected Configuration conf;
  protected Path archiveDir;
  protected FileSystem fs;
  protected Path rootDir;

  protected void setupConf(Configuration conf) {
  }

  @Before
  public void setup() throws Exception {
    rootDir = TEST_UTIL.getDataTestDir("testRestore");
    archiveDir = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
    fs = TEST_UTIL.getTestFileSystem();
    conf = TEST_UTIL.getConfiguration();
    setupConf(conf);
    FSUtils.setRootDir(conf, rootDir);
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(TEST_UTIL.getDataTestDir(), true);
  }

  protected SnapshotMock createSnapshotMock() throws IOException {
    return new SnapshotMock(TEST_UTIL.getConfiguration(), fs, rootDir);
  }

  @Test
  public void testRestore() throws IOException {
    restoreAndVerify("snapshot", "testRestore");
  }

  @Test
  public void testRestoreWithNamespace() throws IOException {
    restoreAndVerify("snapshot", "namespace1:testRestoreWithNamespace");
  }

  private void restoreAndVerify(final String snapshotName, final String tableName) throws IOException {
    // Test Rolling-Upgrade like Snapshot.
    // half machines writing using v1 and the others using v2 format.
    SnapshotMock snapshotMock = createSnapshotMock();
    SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV2("snapshot", tableName);
    builder.addRegionV1();
    builder.addRegionV2();
    builder.addRegionV2();
    builder.addRegionV1();
    Path snapshotDir = builder.commit();
    HTableDescriptor htd = builder.getTableDescriptor();
    SnapshotDescription desc = builder.getSnapshotDescription();

    // Test clone a snapshot
    HTableDescriptor htdClone = snapshotMock.createHtd("testtb-clone");
    testRestore(snapshotDir, desc, htdClone);
    verifyRestore(rootDir, htd, htdClone);

    // Test clone a clone ("link to link")
    SnapshotDescription cloneDesc = SnapshotDescription.newBuilder()
        .setName("cloneSnapshot")
        .setTable("testtb-clone")
        .build();
    Path cloneDir = FSUtils.getTableDir(rootDir, htdClone.getTableName());
    HTableDescriptor htdClone2 = snapshotMock.createHtd("testtb-clone2");
    testRestore(cloneDir, cloneDesc, htdClone2);
    verifyRestore(rootDir, htd, htdClone2);
  }

  private void verifyRestore(final Path rootDir, final HTableDescriptor sourceHtd,
      final HTableDescriptor htdClone) throws IOException {
    List<String> files = SnapshotTestingUtils.listHFileNames(fs,
      FSUtils.getTableDir(rootDir, htdClone.getTableName()));
    assertEquals(12, files.size());
    for (int i = 0; i < files.size(); i += 2) {
      String linkFile = files.get(i);
      String refFile = files.get(i+1);
      assertTrue(linkFile + " should be a HFileLink", HFileLink.isHFileLink(linkFile));
      assertTrue(refFile + " should be a Referene", StoreFileInfo.isReference(refFile));
      assertEquals(sourceHtd.getTableName(), HFileLink.getReferencedTableName(linkFile));
      Path refPath = getReferredToFile(refFile);
      LOG.debug("get reference name for file " + refFile + " = " + refPath);
      assertTrue(refPath.getName() + " should be a HFileLink", HFileLink.isHFileLink(refPath.getName()));
      assertEquals(linkFile, refPath.getName());
    }
  }

  /**
   * Execute the restore operation
   * @param snapshotDir The snapshot directory to use as "restore source"
   * @param sd The snapshot descriptor
   * @param htdClone The HTableDescriptor of the table to restore/clone.
   */
  private void testRestore(final Path snapshotDir, final SnapshotDescription sd,
      final HTableDescriptor htdClone) throws IOException {
    LOG.debug("pre-restore table=" + htdClone.getTableName() + " snapshot=" + snapshotDir);
    FSUtils.logFileSystemState(fs, rootDir, LOG);

    new FSTableDescriptors(conf).createTableDescriptor(htdClone);
    RestoreSnapshotHelper helper = getRestoreHelper(rootDir, snapshotDir, sd, htdClone);
    helper.restoreStorageRegions();

    LOG.debug("post-restore table=" + htdClone.getTableName() + " snapshot=" + snapshotDir);
    FSUtils.logFileSystemState(fs, rootDir, LOG);
  }

  /**
   * Initialize the restore helper, based on the snapshot and table information provided.
   */
  private RestoreSnapshotHelper getRestoreHelper(final Path rootDir, final Path snapshotDir,
      final SnapshotDescription sd, final HTableDescriptor htdClone) throws IOException {
    ForeignExceptionDispatcher monitor = Mockito.mock(ForeignExceptionDispatcher.class);
    MonitoredTask status = Mockito.mock(MonitoredTask.class);

    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, sd);
    return new RestoreSnapshotHelper(conf, manifest,
      htdClone, monitor, status);
  }

  private Path getReferredToFile(final String referenceName) {
    Path fakeBasePath = new Path(new Path("table", "region"), "cf");
    return StoreFileInfo.getReferredToFile(new Path(fakeBasePath, referenceName));
  }
}
