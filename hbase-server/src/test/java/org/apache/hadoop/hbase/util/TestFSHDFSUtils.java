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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test our recoverLease loop against mocked up filesystem.
 */
@Category({MiscTests.class, MediumTests.class})
public class TestFSHDFSUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFSHDFSUtils.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestFSHDFSUtils.class);
  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  static {
    Configuration conf = HTU.getConfiguration();
    conf.setInt("hbase.lease.recovery.first.pause", 10);
    conf.setInt("hbase.lease.recovery.pause", 10);
  }
  private FSHDFSUtils fsHDFSUtils = new FSHDFSUtils();
  private static Path FILE = new Path(HTU.getDataTestDir(), "file.txt");
  long startTime = -1;

  @Before
  public void setup() {
    this.startTime = EnvironmentEdgeManager.currentTime();
  }

  /**
   * Test recover lease eventually succeeding.
   */
  @Test
  public void testRecoverLease() throws IOException {
    HTU.getConfiguration().setInt("hbase.lease.recovery.dfs.timeout", 1000);
    CancelableProgressable reporter = Mockito.mock(CancelableProgressable.class);
    Mockito.when(reporter.progress()).thenReturn(true);
    DistributedFileSystem dfs = Mockito.mock(DistributedFileSystem.class);
    // Fail four times and pass on the fifth.
    Mockito.when(dfs.recoverLease(FILE)).
      thenReturn(false).thenReturn(false).thenReturn(false).thenReturn(false).thenReturn(true);
    assertTrue(this.fsHDFSUtils.recoverDFSFileLease(dfs, FILE, HTU.getConfiguration(), reporter));
    Mockito.verify(dfs, Mockito.times(5)).recoverLease(FILE);
    // Make sure we waited at least hbase.lease.recovery.dfs.timeout * 3 (the first two
    // invocations will happen pretty fast... the we fall into the longer wait loop).
    assertTrue((EnvironmentEdgeManager.currentTime() - this.startTime) >
      (3 * HTU.getConfiguration().getInt("hbase.lease.recovery.dfs.timeout", 61000)));
  }

  /**
   * Test that isFileClosed makes us recover lease faster.
   */
  @Test
  public void testIsFileClosed() throws IOException {
    // Make this time long so it is plain we broke out because of the isFileClosed invocation.
    HTU.getConfiguration().setInt("hbase.lease.recovery.dfs.timeout", 100000);
    CancelableProgressable reporter = Mockito.mock(CancelableProgressable.class);
    Mockito.when(reporter.progress()).thenReturn(true);
    IsFileClosedDistributedFileSystem dfs = Mockito.mock(IsFileClosedDistributedFileSystem.class);
    // Now make it so we fail the first two times -- the two fast invocations, then we fall into
    // the long loop during which we will call isFileClosed.... the next invocation should
    // therefore return true if we are to break the loop.
    Mockito.when(dfs.recoverLease(FILE)).
      thenReturn(false).thenReturn(false).thenReturn(true);
    Mockito.when(dfs.isFileClosed(FILE)).thenReturn(true);
    assertTrue(this.fsHDFSUtils.recoverDFSFileLease(dfs, FILE, HTU.getConfiguration(), reporter));
    Mockito.verify(dfs, Mockito.times(2)).recoverLease(FILE);
    Mockito.verify(dfs, Mockito.times(1)).isFileClosed(FILE);
  }

  void testIsSameHdfs(int nnport) throws IOException {
    try {
      Class dfsUtilClazz = Class.forName("org.apache.hadoop.hdfs.DFSUtil");
      dfsUtilClazz.getMethod("getNNServiceRpcAddresses", Configuration.class);
    } catch (Exception e) {
      LOG.info("Skip testIsSameHdfs test case because of the no-HA hadoop version.");
      return;
    }

    Configuration conf = HBaseConfiguration.create();
    Path srcPath = new Path("hdfs://localhost:" + nnport + "/");
    Path desPath = new Path("hdfs://127.0.0.1/");
    FileSystem srcFs = srcPath.getFileSystem(conf);
    FileSystem desFs = desPath.getFileSystem(conf);

    assertTrue(FSHDFSUtils.isSameHdfs(conf, srcFs, desFs));

    desPath = new Path("hdfs://127.0.0.1:8070/");
    desFs = desPath.getFileSystem(conf);
    assertTrue(!FSHDFSUtils.isSameHdfs(conf, srcFs, desFs));

    desPath = new Path("hdfs://127.0.1.1:" + nnport + "/");
    desFs = desPath.getFileSystem(conf);
    assertTrue(!FSHDFSUtils.isSameHdfs(conf, srcFs, desFs));

    conf.set("fs.defaultFS", "hdfs://haosong-hadoop");
    conf.set("dfs.nameservices", "haosong-hadoop");
    conf.set("dfs.ha.namenodes.haosong-hadoop", "nn1,nn2");
    conf.set("dfs.client.failover.proxy.provider.haosong-hadoop",
        "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

    conf.set("dfs.namenode.rpc-address.haosong-hadoop.nn1", "127.0.0.1:"+ nnport);
    conf.set("dfs.namenode.rpc-address.haosong-hadoop.nn2", "127.10.2.1:8000");
    desPath = new Path("/");
    desFs = desPath.getFileSystem(conf);
    assertTrue(FSHDFSUtils.isSameHdfs(conf, srcFs, desFs));

    conf.set("dfs.namenode.rpc-address.haosong-hadoop.nn1", "127.10.2.1:"+nnport);
    conf.set("dfs.namenode.rpc-address.haosong-hadoop.nn2", "127.0.0.1:8000");
    desPath = new Path("/");
    desFs = desPath.getFileSystem(conf);
    assertTrue(!FSHDFSUtils.isSameHdfs(conf, srcFs, desFs));
  }

  @Test
  public void testIsSameHdfs() throws IOException {
    String hadoopVersion = org.apache.hadoop.util.VersionInfo.getVersion();
    LOG.info("hadoop version is: "  + hadoopVersion);
    boolean isHadoop3_0_0 = hadoopVersion.startsWith("3.0.0");
    if (isHadoop3_0_0) {
      // Hadoop 3.0.0 alpha1+ ~ 3.0.0 GA changed default nn port to 9820.
      // See HDFS-9427
      testIsSameHdfs(9820);
    } else {
      // pre hadoop 3.0.0 defaults to port 8020
      // Hadoop 3.0.1 changed it back to port 8020. See HDFS-12990
      testIsSameHdfs(8020);
    }
  }

  /**
   * Version of DFS that has HDFS-4525 in it.
   */
  static class IsFileClosedDistributedFileSystem extends DistributedFileSystem {
    /**
     * Close status of a file. Copied over from HDFS-4525
     * @return true if file is already closed
     **/
    @Override
    public boolean isFileClosed(Path f) throws IOException{
      return false;
    }
  }
}
