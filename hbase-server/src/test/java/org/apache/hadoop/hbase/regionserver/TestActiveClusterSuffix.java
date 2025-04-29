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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test Active Cluster Suffix file.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestActiveClusterSuffix {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestActiveClusterSuffix.class);

  private final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private JVMClusterUtil.RegionServerThread rst;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(ShutdownHook.RUN_SHUTDOWN_HOOK, false);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    if (rst != null && rst.getRegionServer() != null) {
      rst.getRegionServer().stop("end of test");
      rst.join();
    }
  }

  @Test
  public void testActiveClusterSuffixCreated() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniDFSCluster(1);
    TEST_UTIL.startMiniHBaseCluster();

    Path rootDir = CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration());
    FileSystem fs = rootDir.getFileSystem(TEST_UTIL.getConfiguration());
    Path filePath = new Path(rootDir, HConstants.ACTIVE_CLUSTER_SUFFIX_FILE_NAME);

    assertTrue(filePath + " should exists ", fs.exists(filePath));
    assertTrue(filePath + " should not be empty  ", fs.getFileStatus(filePath).getLen() > 0);

    MasterFileSystem mfs = TEST_UTIL.getHBaseCluster().getMaster().getMasterFileSystem();
    // Compute string using currently set suffix and the cluster id
    String cluster_suffix1 =
      new String(mfs.getSuffixFileDataToCompare(), StandardCharsets.US_ASCII);
    // Compute string member variable
    String cluster_suffix2 = mfs.getActiveClusterSuffix().toString();
    assertEquals(cluster_suffix1, cluster_suffix2);
  }

  @Test
  public void testSuffixFileOnRestart() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniDFSCluster(1);
    TEST_UTIL.createRootDir();
    TEST_UTIL.getConfiguration().set(HConstants.HBASE_META_TABLE_SUFFIX, "Test");

    String clusterId = HBaseCommonTestingUtil.getRandomUUID().toString();
    String cluster_suffix = clusterId + ":" + TEST_UTIL.getConfiguration()
      .get(HConstants.HBASE_META_TABLE_SUFFIX, HConstants.HBASE_META_TABLE_SUFFIX_DEFAULT_VALUE);

    writeIdFile(clusterId, HConstants.CLUSTER_ID_FILE_NAME);
    writeIdFile(cluster_suffix, HConstants.ACTIVE_CLUSTER_SUFFIX_FILE_NAME);

    try {
      TEST_UTIL.startMiniHBaseCluster();
    } catch (IOException ioe) {
      Assert.fail("Can't start mini hbase cluster.");
    }

    MasterFileSystem mfs = TEST_UTIL.getHBaseCluster().getMaster().getMasterFileSystem();
    // Compute using file contents
    String cluster_suffix1 =
      new String(mfs.getSuffixFileDataToCompare(), StandardCharsets.US_ASCII);
    // Compute using config
    String cluster_suffix2 = mfs.getSuffixFromConfig();

    assertEquals(cluster_suffix1, cluster_suffix2);
    assertEquals(cluster_suffix, cluster_suffix1);
  }

  @Test
  public void testVerifyErrorWhenSuffixNotMatched() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniDFSCluster(1);
    TEST_UTIL.createRootDir();
    TEST_UTIL.getConfiguration().setInt("hbase.master.start.timeout.localHBaseCluster", 10000);
    String cluster_suffix = String.valueOf("2df92f65-d801-46e6-b892-c2bae2df3c21:test");
    writeIdFile(cluster_suffix, HConstants.ACTIVE_CLUSTER_SUFFIX_FILE_NAME);
    // Exception: as config in the file and the one set by the user are not matching
    boolean threwIOE = false;
    try {
      TEST_UTIL.startMiniHBaseCluster();
    } catch (IOException ioe) {
      threwIOE = true;
    } finally {
      assertTrue("The master should have thrown an exception", threwIOE);
    }
  }

  private void writeIdFile(String id, String fileName) throws Exception {
    Path rootDir = CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration());
    FileSystem fs = rootDir.getFileSystem(TEST_UTIL.getConfiguration());
    Path filePath = new Path(rootDir, fileName);
    FSDataOutputStream s = null;
    try {
      s = fs.create(filePath);
      s.writeUTF(id);
    } finally {
      if (s != null) {
        s.close();
      }
    }
  }
}
