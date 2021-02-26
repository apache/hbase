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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the master filesystem in a local cluster
 */
@Category({MasterTests.class, MediumTests.class})
public class TestMasterFileSystem {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterFileSystem.class);

  @Rule
  public TestName name = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(TestMasterFileSystem.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupTest() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownTest() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testFsUriSetProperly() throws Exception {
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    MasterFileSystem fs = master.getMasterFileSystem();
    Path masterRoot = CommonFSUtils.getRootDir(fs.getConfiguration());
    Path rootDir = CommonFSUtils.getRootDir(fs.getFileSystem().getConf());
    // make sure the fs and the found root dir have the same scheme
    LOG.debug("from fs uri:" + FileSystem.getDefaultUri(fs.getFileSystem().getConf()));
    LOG.debug("from configuration uri:" + FileSystem.getDefaultUri(fs.getConfiguration()));
    // make sure the set uri matches by forcing it.
    assertEquals(masterRoot, rootDir);
  }

  @Test
  public void testCheckTempDir() throws Exception {
    final MasterFileSystem masterFileSystem =
      UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem();

    final TableName tableName = TableName.valueOf(name.getMethodName());
    final byte[] FAM = Bytes.toBytes("fam");
    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("b"), Bytes.toBytes("c"), Bytes.toBytes("d")
    };

    UTIL.createTable(tableName, FAM, splitKeys);

    // get the current store files for the regions
    List<HRegion> regions = UTIL.getHBaseCluster().getRegions(tableName);
    // make sure we have 4 regions serving this table
    assertEquals(4, regions.size());

    // load the table
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      UTIL.loadTable(table, FAM);
    }

    // disable the table so that we can manipulate the files
    UTIL.getAdmin().disableTable(tableName);

    final Path tableDir = CommonFSUtils.getTableDir(masterFileSystem.getRootDir(), tableName);
    final Path tempDir = masterFileSystem.getTempDir();
    final Path tempTableDir = CommonFSUtils.getTableDir(tempDir, tableName);
    final FileSystem fs = masterFileSystem.getFileSystem();

    // move the table to the temporary directory
    if (!fs.rename(tableDir, tempTableDir)) {
      fail();
    }

    masterFileSystem.checkTempDir(tempDir, UTIL.getConfiguration(), fs);

    // check if the temporary directory exists and is empty
    assertTrue(fs.exists(tempDir));
    assertEquals(0, fs.listStatus(tempDir).length);

    // check for the existence of the archive directory
    for (HRegion region : regions) {
      Path archiveDir = HFileArchiveTestingUtil.getRegionArchiveDir(UTIL.getConfiguration(),
        region);
      assertTrue(fs.exists(archiveDir));
    }

    UTIL.deleteTable(tableName);
  }
}
