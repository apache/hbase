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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hdfs.DFSClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, MediumTests.class})
public class TestHdfsSnapshotHRegion {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHdfsSnapshotHRegion.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String SNAPSHOT_NAME = "foo_snapshot";
  private Table table;
  public static final TableName TABLE_NAME = TableName.valueOf("foo");
  public static final byte[] FAMILY = Bytes.toBytes("f1");
  private DFSClient client;
  private String baseDir;


  @Before
  public void setUp() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    c.setBoolean("dfs.support.append", true);
    TEST_UTIL.startMiniCluster(1);
    table = TEST_UTIL.createMultiRegionTable(TABLE_NAME, FAMILY);
    TEST_UTIL.loadTable(table, FAMILY);

    // setup the hdfssnapshots
    client = new DFSClient(TEST_UTIL.getDFSCluster().getURI(), TEST_UTIL.getConfiguration());
    String fullUrIPath = TEST_UTIL.getDefaultRootDirPath().toString();
    String uriString = TEST_UTIL.getTestFileSystem().getUri().toString();
    baseDir = StringUtils.removeStart(fullUrIPath, uriString);
    client.allowSnapshot(baseDir);
  }

  @After
  public void tearDown() throws Exception {
    client.deleteSnapshot(baseDir, SNAPSHOT_NAME);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testOpeningReadOnlyRegionBasic() throws Exception {
    String snapshotDir = client.createSnapshot(baseDir, SNAPSHOT_NAME);
    RegionInfo firstRegion = TEST_UTIL.getConnection().getRegionLocator(
        table.getName()).getAllRegionLocations().stream().findFirst().get().getRegion();
    Path tableDir = CommonFSUtils.getTableDir(new Path(snapshotDir), TABLE_NAME);
    HRegion snapshottedRegion = openSnapshotRegion(firstRegion, tableDir);
    Assert.assertNotNull(snapshottedRegion);
    snapshottedRegion.close();
  }

  @Test
  public void testSnapshottingWithTmpSplitsAndMergeDirectoriesPresent() throws Exception {
    // lets get a region and create those directories and make sure we ignore them
    RegionInfo firstRegion = TEST_UTIL.getConnection().getRegionLocator(
        table.getName()).getAllRegionLocations().stream().findFirst().get().getRegion();
    String encodedName = firstRegion.getEncodedName();
    Path tableDir = CommonFSUtils.getTableDir(TEST_UTIL.getDefaultRootDirPath(), TABLE_NAME);
    Path regionDirectoryPath = new Path(tableDir, encodedName);
    TEST_UTIL.getTestFileSystem().create(
        new Path(regionDirectoryPath, HRegionFileSystem.REGION_TEMP_DIR));
    TEST_UTIL.getTestFileSystem().create(
        new Path(regionDirectoryPath, HRegionFileSystem.REGION_SPLITS_DIR));
    TEST_UTIL.getTestFileSystem().create(
        new Path(regionDirectoryPath, HRegionFileSystem.REGION_MERGES_DIR));
    // now snapshot
    String snapshotDir = client.createSnapshot(baseDir, "foo_snapshot");
    // everything should still open just fine
    HRegion snapshottedRegion = openSnapshotRegion(firstRegion,
      CommonFSUtils.getTableDir(new Path(snapshotDir), TABLE_NAME));
    Assert.assertNotNull(snapshottedRegion); // no errors and the region should open
    snapshottedRegion.close();
  }

  private HRegion openSnapshotRegion(RegionInfo firstRegion, Path tableDir) throws IOException {
    return HRegion.openReadOnlyFileSystemHRegion(
        TEST_UTIL.getConfiguration(),
        TEST_UTIL.getTestFileSystem(),
        tableDir,
        firstRegion,
        table.getDescriptor()
    );
  }
}
