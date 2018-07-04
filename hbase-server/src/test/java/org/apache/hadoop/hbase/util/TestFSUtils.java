/**
 *
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test {@link FSUtils}.
 */
@Category(MediumTests.class)
public class TestFSUtils {
  private static final Log LOG = LogFactory.getLog(TestFSUtils.class);

  /**
   * Test path compare and prefix checking.
   * @throws IOException
   */
  @Test
  public void testMatchingTail() throws IOException {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    final FileSystem fs = htu.getTestFileSystem();
    Path rootdir = htu.getDataTestDir();
    assertTrue(rootdir.depth() > 1);
    Path partPath = new Path("a", "b");
    Path fullPath = new Path(rootdir, partPath);
    Path fullyQualifiedPath = fs.makeQualified(fullPath);
    assertFalse(FSUtils.isMatchingTail(fullPath, partPath));
    assertFalse(FSUtils.isMatchingTail(fullPath, partPath.toString()));
    assertTrue(FSUtils.isStartingWithPath(rootdir, fullPath.toString()));
    assertTrue(FSUtils.isStartingWithPath(fullyQualifiedPath, fullPath.toString()));
    assertFalse(FSUtils.isStartingWithPath(rootdir, partPath.toString()));
    assertFalse(FSUtils.isMatchingTail(fullyQualifiedPath, partPath));
    assertTrue(FSUtils.isMatchingTail(fullyQualifiedPath, fullPath));
    assertTrue(FSUtils.isMatchingTail(fullyQualifiedPath, fullPath.toString()));
    assertTrue(FSUtils.isMatchingTail(fullyQualifiedPath, fs.makeQualified(fullPath)));
    assertTrue(FSUtils.isStartingWithPath(rootdir, fullyQualifiedPath.toString()));
    assertFalse(FSUtils.isMatchingTail(fullPath, new Path("x")));
    assertFalse(FSUtils.isMatchingTail(new Path("x"), fullPath));
  }

  @Test
  public void testVersion() throws DeserializationException, IOException {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    final FileSystem fs = htu.getTestFileSystem();
    final Path rootdir = htu.getDataTestDir();
    assertNull(FSUtils.getVersion(fs, rootdir));
    // Write out old format version file.  See if we can read it in and convert.
    Path versionFile = new Path(rootdir, HConstants.VERSION_FILE_NAME);
    FSDataOutputStream s = fs.create(versionFile);
    final String version = HConstants.FILE_SYSTEM_VERSION;
    s.writeUTF(version);
    s.close();
    assertTrue(fs.exists(versionFile));
    FileStatus [] status = fs.listStatus(versionFile);
    assertNotNull(status);
    assertTrue(status.length > 0);
    String newVersion = FSUtils.getVersion(fs, rootdir);
    assertEquals(version.length(), newVersion.length());
    assertEquals(version, newVersion);
    // File will have been converted. Exercise the pb format
    assertEquals(version, FSUtils.getVersion(fs, rootdir));
    FSUtils.checkVersion(fs, rootdir, true);
  }

  @Test public void testIsHDFS() throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    htu.getConfiguration().setBoolean("dfs.support.append", false);
    assertFalse(FSUtils.isHDFS(htu.getConfiguration()));
    htu.getConfiguration().setBoolean("dfs.support.append", true);
    MiniDFSCluster cluster = null;
    try {
      cluster = htu.startMiniDFSCluster(1);
      assertTrue(FSUtils.isHDFS(htu.getConfiguration()));
      assertTrue(FSUtils.isAppendSupported(htu.getConfiguration()));
    } finally {
      if (cluster != null) cluster.shutdown();
    }
  }

  private void WriteDataToHDFS(FileSystem fs, Path file, int dataSize)
    throws Exception {
    FSDataOutputStream out = fs.create(file);
    byte [] data = new byte[dataSize];
    out.write(data, 0, dataSize);
    out.close();
  }

  @Test public void testcomputeHDFSBlocksDistribution() throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    final int DEFAULT_BLOCK_SIZE = 1024;
    htu.getConfiguration().setLong("dfs.blocksize", DEFAULT_BLOCK_SIZE);
    MiniDFSCluster cluster = null;
    Path testFile = null;

    try {
      // set up a cluster with 3 nodes
      String hosts[] = new String[] { "host1", "host2", "host3" };
      cluster = htu.startMiniDFSCluster(hosts);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();

      // create a file with two blocks
      testFile = new Path("/test1.txt");
      WriteDataToHDFS(fs, testFile, 2*DEFAULT_BLOCK_SIZE);

      // given the default replication factor is 3, the same as the number of
      // datanodes; the locality index for each host should be 100%,
      // or getWeight for each host should be the same as getUniqueBlocksWeights
      final long maxTime = System.currentTimeMillis() + 2000;
      boolean ok;
      do {
        ok = true;
        FileStatus status = fs.getFileStatus(testFile);
        HDFSBlocksDistribution blocksDistribution =
          FSUtils.computeHDFSBlocksDistribution(fs, status, 0, status.getLen());
        long uniqueBlocksTotalWeight =
          blocksDistribution.getUniqueBlocksTotalWeight();
        for (String host : hosts) {
          long weight = blocksDistribution.getWeight(host);
          ok = (ok && uniqueBlocksTotalWeight == weight);
        }
      } while (!ok && System.currentTimeMillis() < maxTime);
      assertTrue(ok);
      } finally {
      htu.shutdownMiniDFSCluster();
    }


    try {
      // set up a cluster with 4 nodes
      String hosts[] = new String[] { "host1", "host2", "host3", "host4" };
      cluster = htu.startMiniDFSCluster(hosts);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();

      // create a file with three blocks
      testFile = new Path("/test2.txt");
      WriteDataToHDFS(fs, testFile, 3*DEFAULT_BLOCK_SIZE);

      // given the default replication factor is 3, we will have total of 9
      // replica of blocks; thus the host with the highest weight should have
      // weight == 3 * DEFAULT_BLOCK_SIZE
      final long maxTime = System.currentTimeMillis() + 2000;
      long weight;
      long uniqueBlocksTotalWeight;
      do {
        FileStatus status = fs.getFileStatus(testFile);
        HDFSBlocksDistribution blocksDistribution =
          FSUtils.computeHDFSBlocksDistribution(fs, status, 0, status.getLen());
        uniqueBlocksTotalWeight = blocksDistribution.getUniqueBlocksTotalWeight();

        String tophost = blocksDistribution.getTopHosts().get(0);
        weight = blocksDistribution.getWeight(tophost);

        // NameNode is informed asynchronously, so we may have a delay. See HBASE-6175
      } while (uniqueBlocksTotalWeight != weight && System.currentTimeMillis() < maxTime);
      assertTrue(uniqueBlocksTotalWeight == weight);

    } finally {
      htu.shutdownMiniDFSCluster();
    }


    try {
      // set up a cluster with 4 nodes
      String hosts[] = new String[] { "host1", "host2", "host3", "host4" };
      cluster = htu.startMiniDFSCluster(hosts);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();

      // create a file with one block
      testFile = new Path("/test3.txt");
      WriteDataToHDFS(fs, testFile, DEFAULT_BLOCK_SIZE);

      // given the default replication factor is 3, we will have total of 3
      // replica of blocks; thus there is one host without weight
      final long maxTime = System.currentTimeMillis() + 2000;
      HDFSBlocksDistribution blocksDistribution;
      do {
        FileStatus status = fs.getFileStatus(testFile);
        blocksDistribution = FSUtils.computeHDFSBlocksDistribution(fs, status, 0, status.getLen());
        // NameNode is informed asynchronously, so we may have a delay. See HBASE-6175
      }
      while (blocksDistribution.getTopHosts().size() != 3 && System.currentTimeMillis() < maxTime);
      assertEquals("Wrong number of hosts distributing blocks.", 3,
        blocksDistribution.getTopHosts().size());
    } finally {
      htu.shutdownMiniDFSCluster();
    }
  }

  @Test
  public void testPermMask() throws Exception {

    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(conf);

    // default fs permission
    FsPermission defaultFsPerm = FSUtils.getFilePermissions(fs, conf,
        HConstants.DATA_FILE_UMASK_KEY);
    // 'hbase.data.umask.enable' is false. We will get default fs permission.
    assertEquals(FsPermission.getFileDefault(), defaultFsPerm);

    conf.setBoolean(HConstants.ENABLE_DATA_FILE_UMASK, true);
    // first check that we don't crash if we don't have perms set
    FsPermission defaultStartPerm = FSUtils.getFilePermissions(fs, conf,
        HConstants.DATA_FILE_UMASK_KEY);
    // default 'hbase.data.umask'is 000, and this umask will be used when
    // 'hbase.data.umask.enable' is true.
    // Therefore we will not get the real fs default in this case.
    // Instead we will get the starting point FULL_RWX_PERMISSIONS
    assertEquals(new FsPermission(FSUtils.FULL_RWX_PERMISSIONS), defaultStartPerm);

    conf.setStrings(HConstants.DATA_FILE_UMASK_KEY, "077");
    // now check that we get the right perms
    FsPermission filePerm = FSUtils.getFilePermissions(fs, conf,
        HConstants.DATA_FILE_UMASK_KEY);
    assertEquals(new FsPermission("700"), filePerm);

    // then that the correct file is created
    Path p = new Path("target" + File.separator + UUID.randomUUID().toString());
    try {
      FSDataOutputStream out = FSUtils.create(conf, fs, p, filePerm, null);
      out.close();
      FileStatus stat = fs.getFileStatus(p);
      assertEquals(new FsPermission("700"), stat.getPermission());
      // and then cleanup
    } finally {
      fs.delete(p, true);
    }
  }

  @Test
  public void testDeleteAndExists() throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    Configuration conf = htu.getConfiguration();
    conf.setBoolean(HConstants.ENABLE_DATA_FILE_UMASK, true);
    FileSystem fs = FileSystem.get(conf);
    FsPermission perms = FSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
    // then that the correct file is created
    String file = UUID.randomUUID().toString();
    Path p = new Path(htu.getDataTestDir(), "temptarget" + File.separator + file);
    Path p1 = new Path(htu.getDataTestDir(), "temppath" + File.separator + file);
    try {
      FSDataOutputStream out = FSUtils.create(conf, fs, p, perms, null);
      out.close();
      assertTrue("The created file should be present", FSUtils.isExists(fs, p));
      // delete the file with recursion as false. Only the file will be deleted.
      FSUtils.delete(fs, p, false);
      // Create another file
      FSDataOutputStream out1 = FSUtils.create(conf, fs, p1, perms, null);
      out1.close();
      // delete the file with recursion as false. Still the file only will be deleted
      FSUtils.delete(fs, p1, true);
      assertFalse("The created file should be present", FSUtils.isExists(fs, p1));
      // and then cleanup
    } finally {
      FSUtils.delete(fs, p, true);
      FSUtils.delete(fs, p1, true);
    }
  }

  @Test
  public void testFilteredStatusDoesNotThrowOnNotFound() throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    MiniDFSCluster cluster = htu.startMiniDFSCluster(1);
    try {
      assertNull(FSUtils.listStatusWithStatusFilter(cluster.getFileSystem(), new Path("definitely/doesn't/exist"), null));
    } finally {
      cluster.shutdown();
    }

  }

  @Test
  public void testRenameAndSetModifyTime() throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    Configuration conf = htu.getConfiguration();

    MiniDFSCluster cluster = htu.startMiniDFSCluster(1);
    assertTrue(FSUtils.isHDFS(conf));

    FileSystem fs = FileSystem.get(conf);
    Path testDir = htu.getDataTestDirOnTestFS("testArchiveFile");

    String file = UUID.randomUUID().toString();
    Path p = new Path(testDir, file);

    FSDataOutputStream out = fs.create(p);
    out.close();
    assertTrue("The created file should be present", FSUtils.isExists(fs, p));

    long expect = System.currentTimeMillis() + 1000;
    assertNotEquals(expect, fs.getFileStatus(p).getModificationTime());

    ManualEnvironmentEdge mockEnv = new ManualEnvironmentEdge();
    mockEnv.setValue(expect);
    EnvironmentEdgeManager.injectEdge(mockEnv);
    try {
      String dstFile = UUID.randomUUID().toString();
      Path dst = new Path(testDir , dstFile);

      assertTrue(FSUtils.renameAndSetModifyTime(fs, p, dst));
      assertFalse("The moved file should not be present", FSUtils.isExists(fs, p));
      assertTrue("The dst file should be present", FSUtils.isExists(fs, dst));

      assertEquals(expect, fs.getFileStatus(dst).getModificationTime());
      cluster.shutdown();
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  private void verifyFileInDirWithStoragePolicy(final String policy) throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    Configuration conf = htu.getConfiguration();
    conf.set(HConstants.WAL_STORAGE_POLICY, policy);

    MiniDFSCluster cluster = htu.startMiniDFSCluster(1);
    try {
      assertTrue(FSUtils.isHDFS(conf));

      FileSystem fs = FileSystem.get(conf);
      Path testDir = htu.getDataTestDirOnTestFS("testArchiveFile");
      fs.mkdirs(testDir);

      String storagePolicy =
          conf.get(HConstants.WAL_STORAGE_POLICY, HConstants.DEFAULT_WAL_STORAGE_POLICY);
      FSUtils.setStoragePolicy(fs, testDir, storagePolicy);

      String file = UUID.randomUUID().toString();
      Path p = new Path(testDir, file);
      WriteDataToHDFS(fs, p, 4096);
      try (HFileSystem hfs = new HFileSystem(fs)) {
        String policySet = hfs.getStoragePolicyName(p);
        LOG.debug("The storage policy of path " + p + " is " + policySet);
        if (policy.equals(HConstants.DEFER_TO_HDFS_STORAGE_POLICY)
            || policy.equals(INVALID_STORAGE_POLICY)) {
          String hdfsDefaultPolicy = hfs.getStoragePolicyName(hfs.getHomeDirectory());
          LOG.debug("The default hdfs storage policy (indicated by home path: "
              + hfs.getHomeDirectory() + ") is " + hdfsDefaultPolicy);
          Assert.assertEquals(hdfsDefaultPolicy, policySet);
        } else {
          Assert.assertEquals(policy, policySet);
        }
        // will assert existance before deleting.
        cleanupFile(fs, testDir);
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testSetStoragePolicyDefault() throws Exception {
    verifyNoHDFSApiInvocationForDefaultPolicy();
    verifyFileInDirWithStoragePolicy(HConstants.DEFAULT_WAL_STORAGE_POLICY);
  }

  /**
   * Note: currently the default policy is set to defer to HDFS and this case is to verify the
   * logic, will need to remove the check if the default policy is changed
   */
  private void verifyNoHDFSApiInvocationForDefaultPolicy() {
    FileSystem testFs = new AlwaysFailSetStoragePolicyFileSystem();
    // There should be no exception thrown when setting to default storage policy, which indicates
    // the HDFS API hasn't been called
    try {
      FSUtils.setStoragePolicy(testFs, new Path("non-exist"), HConstants.DEFAULT_WAL_STORAGE_POLICY,
        true);
    } catch (IOException e) {
      Assert.fail("Should have bypassed the FS API when setting default storage policy");
    }
    // There should be exception thrown when given non-default storage policy, which indicates the
    // HDFS API has been called
    try {
      FSUtils.setStoragePolicy(testFs, new Path("non-exist"), "HOT", true);
      Assert.fail("Should have invoked the FS API but haven't");
    } catch (IOException e) {
      // expected given an invalid path
    }
  }

  class AlwaysFailSetStoragePolicyFileSystem extends DistributedFileSystem {
    @Override
    public void setStoragePolicy(final Path src, final String policyName) throws IOException {
      throw new IOException("The setStoragePolicy method is invoked");
    }
  }

  /* might log a warning, but still work. (always warning on Hadoop < 2.6.0) */
  @Test
  public void testSetStoragePolicyValidButMaybeNotPresent() throws Exception {
    verifyFileInDirWithStoragePolicy("ALL_SSD");
  }

  final String INVALID_STORAGE_POLICY = "1772";

  /* should log a warning, but still work. (different warning on Hadoop < 2.6.0) */
  @Test
  public void testSetStoragePolicyInvalid() throws Exception {
    verifyFileInDirWithStoragePolicy(INVALID_STORAGE_POLICY);
  }

  @Test
  public void testSetWALRootDir() throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    Configuration conf = htu.getConfiguration();
    Path p = new Path("file:///hbase/root");
    FSUtils.setWALRootDir(conf, p);
    assertEquals(p.toString(), conf.get(HFileSystem.HBASE_WAL_DIR));
  }

  @Test
  public void testGetWALRootDir() throws IOException {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    Configuration conf = htu.getConfiguration();
    Path root = new Path("file:///hbase/root");
    Path walRoot = new Path("file:///hbase/logroot");
    FSUtils.setRootDir(conf, root);
    assertEquals(FSUtils.getRootDir(conf), root);
    assertEquals(FSUtils.getWALRootDir(conf), root);
    FSUtils.setWALRootDir(conf, walRoot);
    assertEquals(FSUtils.getWALRootDir(conf), walRoot);
  }

  @Test(expected=IllegalStateException.class)
  public void testGetWALRootDirIllegalWALDir() throws IOException {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    Configuration conf = htu.getConfiguration();
    Path root = new Path("file:///hbase/root");
    Path invalidWALDir = new Path("file:///hbase/root/logroot");
    FSUtils.setRootDir(conf, root);
    FSUtils.setWALRootDir(conf, invalidWALDir);
    FSUtils.getWALRootDir(conf);
  }

  @Test
  public void testRemoveWALRootPath() throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    Configuration conf = htu.getConfiguration();
    FSUtils.setRootDir(conf, new Path("file:///user/hbase"));
    Path testFile = new Path(FSUtils.getRootDir(conf), "test/testfile");
    Path tmpFile = new Path("file:///test/testfile");
    assertEquals(FSUtils.removeWALRootPath(testFile, conf), "test/testfile");
    assertEquals(FSUtils.removeWALRootPath(tmpFile, conf), tmpFile.toString());
    FSUtils.setWALRootDir(conf, new Path("file:///user/hbaseLogDir"));
    assertEquals(FSUtils.removeWALRootPath(testFile, conf), testFile.toString());
    Path logFile = new Path(FSUtils.getWALRootDir(conf), "test/testlog");
    assertEquals(FSUtils.removeWALRootPath(logFile, conf), "test/testlog");
  }

  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    assertTrue(fileSys.delete(name, true));
    assertTrue(!fileSys.exists(name));
  }
}
