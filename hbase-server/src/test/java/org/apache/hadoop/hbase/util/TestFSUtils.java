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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSHedgedReadMetrics;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link FSUtils}.
 */
@Category({MiscTests.class, MediumTests.class})
public class TestFSUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFSUtils.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestFSUtils.class);

  private HBaseTestingUtility htu;
  private FileSystem fs;
  private Configuration conf;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseTestingUtility();
    fs = htu.getTestFileSystem();
    conf = htu.getConfiguration();
  }

  @Test
  public void testIsHDFS() throws Exception {
    assertFalse(CommonFSUtils.isHDFS(conf));
    MiniDFSCluster cluster = null;
    try {
      cluster = htu.startMiniDFSCluster(1);
      assertTrue(CommonFSUtils.isHDFS(conf));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
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
    final int DEFAULT_BLOCK_SIZE = 1024;
    conf.setLong("dfs.blocksize", DEFAULT_BLOCK_SIZE);
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

  private void writeVersionFile(Path versionFile, String version) throws IOException {
    if (CommonFSUtils.isExists(fs, versionFile)) {
      assertTrue(CommonFSUtils.delete(fs, versionFile, true));
    }
    try (FSDataOutputStream s = fs.create(versionFile)) {
      s.writeUTF(version);
    }
    assertTrue(fs.exists(versionFile));
  }

  @Test
  public void testVersion() throws DeserializationException, IOException {
    final Path rootdir = htu.getDataTestDir();
    final FileSystem fs = rootdir.getFileSystem(conf);
    assertNull(FSUtils.getVersion(fs, rootdir));
    // No meta dir so no complaint from checkVersion.
    // Presumes it a new install. Will create version file.
    FSUtils.checkVersion(fs, rootdir, true);
    // Now remove the version file and create a metadir so checkVersion fails.
    Path versionFile = new Path(rootdir, HConstants.VERSION_FILE_NAME);
    assertTrue(CommonFSUtils.isExists(fs, versionFile));
    assertTrue(CommonFSUtils.delete(fs, versionFile, true));
    Path metaRegionDir =
        FSUtils.getRegionDirFromRootDir(rootdir, RegionInfoBuilder.FIRST_META_REGIONINFO);
    FsPermission defaultPerms = CommonFSUtils.getFilePermissions(fs, this.conf,
        HConstants.DATA_FILE_UMASK_KEY);
    CommonFSUtils.create(fs, metaRegionDir, defaultPerms, false);
    boolean thrown = false;
    try {
      FSUtils.checkVersion(fs, rootdir, true);
    } catch (FileSystemVersionException e) {
      thrown = true;
    }
    assertTrue("Expected FileSystemVersionException", thrown);
    // Write out a good version file.  See if we can read it in and convert.
    String version = HConstants.FILE_SYSTEM_VERSION;
    writeVersionFile(versionFile, version);
    FileStatus [] status = fs.listStatus(versionFile);
    assertNotNull(status);
    assertTrue(status.length > 0);
    String newVersion = FSUtils.getVersion(fs, rootdir);
    assertEquals(version.length(), newVersion.length());
    assertEquals(version, newVersion);
    // File will have been converted. Exercise the pb format
    assertEquals(version, FSUtils.getVersion(fs, rootdir));
    FSUtils.checkVersion(fs, rootdir, true);
    // Write an old version file.
    String oldVersion = "1";
    writeVersionFile(versionFile, oldVersion);
    newVersion = FSUtils.getVersion(fs, rootdir);
    assertNotEquals(version, newVersion);
    thrown = false;
    try {
      FSUtils.checkVersion(fs, rootdir, true);
    } catch (FileSystemVersionException e) {
      thrown = true;
    }
    assertTrue("Expected FileSystemVersionException", thrown);
  }

  @Test
  public void testPermMask() throws Exception {
    final Path rootdir = htu.getDataTestDir();
    final FileSystem fs = rootdir.getFileSystem(conf);
    // default fs permission
    FsPermission defaultFsPerm = CommonFSUtils.getFilePermissions(fs, conf,
        HConstants.DATA_FILE_UMASK_KEY);
    // 'hbase.data.umask.enable' is false. We will get default fs permission.
    assertEquals(FsPermission.getFileDefault(), defaultFsPerm);

    conf.setBoolean(HConstants.ENABLE_DATA_FILE_UMASK, true);
    // first check that we don't crash if we don't have perms set
    FsPermission defaultStartPerm = CommonFSUtils.getFilePermissions(fs, conf,
        HConstants.DATA_FILE_UMASK_KEY);
    // default 'hbase.data.umask'is 000, and this umask will be used when
    // 'hbase.data.umask.enable' is true.
    // Therefore we will not get the real fs default in this case.
    // Instead we will get the starting point FULL_RWX_PERMISSIONS
    assertEquals(new FsPermission(CommonFSUtils.FULL_RWX_PERMISSIONS), defaultStartPerm);

    conf.setStrings(HConstants.DATA_FILE_UMASK_KEY, "077");
    // now check that we get the right perms
    FsPermission filePerm = CommonFSUtils.getFilePermissions(fs, conf,
        HConstants.DATA_FILE_UMASK_KEY);
    assertEquals(new FsPermission("700"), filePerm);

    // then that the correct file is created
    Path p = new Path("target" + File.separator + HBaseTestingUtility.getRandomUUID().toString());
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
    final Path rootdir = htu.getDataTestDir();
    final FileSystem fs = rootdir.getFileSystem(conf);
    conf.setBoolean(HConstants.ENABLE_DATA_FILE_UMASK, true);
    FsPermission perms = CommonFSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
    // then that the correct file is created
    String file = HBaseTestingUtility.getRandomUUID().toString();
    Path p = new Path(htu.getDataTestDir(), "temptarget" + File.separator + file);
    Path p1 = new Path(htu.getDataTestDir(), "temppath" + File.separator + file);
    try {
      FSDataOutputStream out = FSUtils.create(conf, fs, p, perms, null);
      out.close();
      assertTrue("The created file should be present", CommonFSUtils.isExists(fs, p));
      // delete the file with recursion as false. Only the file will be deleted.
      CommonFSUtils.delete(fs, p, false);
      // Create another file
      FSDataOutputStream out1 = FSUtils.create(conf, fs, p1, perms, null);
      out1.close();
      // delete the file with recursion as false. Still the file only will be deleted
      CommonFSUtils.delete(fs, p1, true);
      assertFalse("The created file should be present", CommonFSUtils.isExists(fs, p1));
      // and then cleanup
    } finally {
      CommonFSUtils.delete(fs, p, true);
      CommonFSUtils.delete(fs, p1, true);
    }
  }

  @Test
  public void testFilteredStatusDoesNotThrowOnNotFound() throws Exception {
    MiniDFSCluster cluster = htu.startMiniDFSCluster(1);
    try {
      assertNull(FSUtils.listStatusWithStatusFilter(cluster.getFileSystem(), new Path("definitely/doesn't/exist"), null));
    } finally {
      cluster.shutdown();
    }

  }

  @Test
  public void testRenameAndSetModifyTime() throws Exception {
    MiniDFSCluster cluster = htu.startMiniDFSCluster(1);
    assertTrue(CommonFSUtils.isHDFS(conf));

    FileSystem fs = FileSystem.get(conf);
    Path testDir = htu.getDataTestDirOnTestFS("testArchiveFile");

    String file = HBaseTestingUtility.getRandomUUID().toString();
    Path p = new Path(testDir, file);

    FSDataOutputStream out = fs.create(p);
    out.close();
    assertTrue("The created file should be present", CommonFSUtils.isExists(fs, p));

    long expect = System.currentTimeMillis() + 1000;
    assertNotEquals(expect, fs.getFileStatus(p).getModificationTime());

    ManualEnvironmentEdge mockEnv = new ManualEnvironmentEdge();
    mockEnv.setValue(expect);
    EnvironmentEdgeManager.injectEdge(mockEnv);
    try {
      String dstFile = HBaseTestingUtility.getRandomUUID().toString();
      Path dst = new Path(testDir , dstFile);

      assertTrue(CommonFSUtils.renameAndSetModifyTime(fs, p, dst));
      assertFalse("The moved file should not be present", CommonFSUtils.isExists(fs, p));
      assertTrue("The dst file should be present", CommonFSUtils.isExists(fs, dst));

      assertEquals(expect, fs.getFileStatus(dst).getModificationTime());
      cluster.shutdown();
    } finally {
      EnvironmentEdgeManager.reset();
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
      CommonFSUtils.setStoragePolicy(testFs, new Path("non-exist"),
        HConstants.DEFAULT_WAL_STORAGE_POLICY, true);
    } catch (IOException e) {
      Assert.fail("Should have bypassed the FS API when setting default storage policy");
    }
    // There should be exception thrown when given non-default storage policy, which indicates the
    // HDFS API has been called
    try {
      CommonFSUtils.setStoragePolicy(testFs, new Path("non-exist"), "HOT", true);
      Assert.fail("Should have invoked the FS API but haven't");
    } catch (IOException e) {
      // expected given an invalid path
    }
  }

  class AlwaysFailSetStoragePolicyFileSystem extends DistributedFileSystem {
    @Override
    public void setStoragePolicy(final Path src, final String policyName)
            throws IOException {
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

  // Here instead of TestCommonFSUtils because we need a minicluster
  private void verifyFileInDirWithStoragePolicy(final String policy) throws Exception {
    conf.set(HConstants.WAL_STORAGE_POLICY, policy);

    MiniDFSCluster cluster = htu.startMiniDFSCluster(1);
    try {
      assertTrue(CommonFSUtils.isHDFS(conf));

      FileSystem fs = FileSystem.get(conf);
      Path testDir = htu.getDataTestDirOnTestFS("testArchiveFile");
      fs.mkdirs(testDir);

      String storagePolicy =
          conf.get(HConstants.WAL_STORAGE_POLICY, HConstants.DEFAULT_WAL_STORAGE_POLICY);
      CommonFSUtils.setStoragePolicy(fs, testDir, storagePolicy);

      String file = HBaseTestingUtility.getRandomUUID().toString();
      Path p = new Path(testDir, file);
      WriteDataToHDFS(fs, p, 4096);
      HFileSystem hfs = new HFileSystem(fs);
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
      // will assert existence before deleting.
      cleanupFile(fs, testDir);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Ugly test that ensures we can get at the hedged read counters in dfsclient.
   * Does a bit of preading with hedged reads enabled using code taken from hdfs TestPread.
   * @throws Exception
   */
  @Test public void testDFSHedgedReadMetrics() throws Exception {
    // Enable hedged reads and set it so the threshold is really low.
    // Most of this test is taken from HDFS, from TestPread.
    conf.setInt(DFSConfigKeys.DFS_DFSCLIENT_HEDGED_READ_THREADPOOL_SIZE, 5);
    conf.setLong(DFSConfigKeys.DFS_DFSCLIENT_HEDGED_READ_THRESHOLD_MILLIS, 0);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 4096);
    conf.setLong(DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY, 4096);
    // Set short retry timeouts so this test runs faster
    conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE, 0);
    conf.setBoolean("dfs.datanode.transferTo.allowed", false);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    // Get the metrics.  Should be empty.
    DFSHedgedReadMetrics metrics = FSUtils.getDFSHedgedReadMetrics(conf);
    assertEquals(0, metrics.getHedgedReadOps());
    FileSystem fileSys = cluster.getFileSystem();
    try {
      Path p = new Path("preadtest.dat");
      // We need > 1 blocks to test out the hedged reads.
      DFSTestUtil.createFile(fileSys, p, 12 * blockSize, 12 * blockSize,
        blockSize, (short) 3, seed);
      pReadFile(fileSys, p);
      cleanupFile(fileSys, p);
      assertTrue(metrics.getHedgedReadOps() > 0);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }


  @Test
  public void testCopyFilesParallel() throws Exception {
    MiniDFSCluster cluster = htu.startMiniDFSCluster(1);
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    Path src = new Path("/src");
    fs.mkdirs(src);
    for (int i = 0; i < 50; i++) {
      WriteDataToHDFS(fs, new Path(src, String.valueOf(i)), 1024);
    }
    Path sub = new Path(src, "sub");
    fs.mkdirs(sub);
    for (int i = 0; i < 50; i++) {
      WriteDataToHDFS(fs, new Path(sub, String.valueOf(i)), 1024);
    }
    Path dst = new Path("/dst");
    List<Path> allFiles = FSUtils.copyFilesParallel(fs, src, fs, dst, conf, 4);

    assertEquals(102, allFiles.size());
    FileStatus[] list = fs.listStatus(dst);
    assertEquals(51, list.length);
    FileStatus[] sublist = fs.listStatus(new Path(dst, "sub"));
    assertEquals(50, sublist.length);
  }

  // Below is taken from TestPread over in HDFS.
  static final int blockSize = 4096;
  static final long seed = 0xDEADBEEFL;
  private Random rand = new Random(); // This test depends on Random#setSeed

  private void pReadFile(FileSystem fileSys, Path name) throws IOException {
    FSDataInputStream stm = fileSys.open(name);
    byte[] expected = new byte[12 * blockSize];
    rand.setSeed(seed);
    rand.nextBytes(expected);
    // do a sanity check. Read first 4K bytes
    byte[] actual = new byte[4096];
    stm.readFully(actual);
    checkAndEraseData(actual, 0, expected, "Read Sanity Test");
    // now do a pread for the first 8K bytes
    actual = new byte[8192];
    doPread(stm, 0L, actual, 0, 8192);
    checkAndEraseData(actual, 0, expected, "Pread Test 1");
    // Now check to see if the normal read returns 4K-8K byte range
    actual = new byte[4096];
    stm.readFully(actual);
    checkAndEraseData(actual, 4096, expected, "Pread Test 2");
    // Now see if we can cross a single block boundary successfully
    // read 4K bytes from blockSize - 2K offset
    stm.readFully(blockSize - 2048, actual, 0, 4096);
    checkAndEraseData(actual, (blockSize - 2048), expected, "Pread Test 3");
    // now see if we can cross two block boundaries successfully
    // read blockSize + 4K bytes from blockSize - 2K offset
    actual = new byte[blockSize + 4096];
    stm.readFully(blockSize - 2048, actual);
    checkAndEraseData(actual, (blockSize - 2048), expected, "Pread Test 4");
    // now see if we can cross two block boundaries that are not cached
    // read blockSize + 4K bytes from 10*blockSize - 2K offset
    actual = new byte[blockSize + 4096];
    stm.readFully(10 * blockSize - 2048, actual);
    checkAndEraseData(actual, (10 * blockSize - 2048), expected, "Pread Test 5");
    // now check that even after all these preads, we can still read
    // bytes 8K-12K
    actual = new byte[4096];
    stm.readFully(actual);
    checkAndEraseData(actual, 8192, expected, "Pread Test 6");
    // done
    stm.close();
    // check block location caching
    stm = fileSys.open(name);
    stm.readFully(1, actual, 0, 4096);
    stm.readFully(4*blockSize, actual, 0, 4096);
    stm.readFully(7*blockSize, actual, 0, 4096);
    actual = new byte[3*4096];
    stm.readFully(0*blockSize, actual, 0, 3*4096);
    checkAndEraseData(actual, 0, expected, "Pread Test 7");
    actual = new byte[8*4096];
    stm.readFully(3*blockSize, actual, 0, 8*4096);
    checkAndEraseData(actual, 3*blockSize, expected, "Pread Test 8");
    // read the tail
    stm.readFully(11*blockSize+blockSize/2, actual, 0, blockSize/2);
    IOException res = null;
    try { // read beyond the end of the file
      stm.readFully(11*blockSize+blockSize/2, actual, 0, blockSize);
    } catch (IOException e) {
      // should throw an exception
      res = e;
    }
    assertTrue("Error reading beyond file boundary.", res != null);

    stm.close();
  }

  private void checkAndEraseData(byte[] actual, int from, byte[] expected, String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                        expected[from+idx]+" actual "+actual[idx],
                        actual[idx], expected[from+idx]);
      actual[idx] = 0;
    }
  }

  private void doPread(FSDataInputStream stm, long position, byte[] buffer,
      int offset, int length) throws IOException {
    int nread = 0;
    // long totalRead = 0;
    // DFSInputStream dfstm = null;

    /* Disable. This counts do not add up. Some issue in original hdfs tests?
    if (stm.getWrappedStream() instanceof DFSInputStream) {
      dfstm = (DFSInputStream) (stm.getWrappedStream());
      totalRead = dfstm.getReadStatistics().getTotalBytesRead();
    } */

    while (nread < length) {
      int nbytes =
          stm.read(position + nread, buffer, offset + nread, length - nread);
      assertTrue("Error in pread", nbytes > 0);
      nread += nbytes;
    }

    /* Disable. This counts do not add up. Some issue in original hdfs tests?
    if (dfstm != null) {
      if (isHedgedRead) {
        assertTrue("Expected read statistic to be incremented",
          length <= dfstm.getReadStatistics().getTotalBytesRead() - totalRead);
      } else {
        assertEquals("Expected read statistic to be incremented", length, dfstm
            .getReadStatistics().getTotalBytesRead() - totalRead);
      }
    }*/
  }

  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    assertTrue(fileSys.delete(name, true));
    assertTrue(!fileSys.exists(name));
  }


  static {
    try {
      Class.forName("org.apache.hadoop.fs.StreamCapabilities");
      LOG.debug("Test thought StreamCapabilities class was present.");
    } catch (ClassNotFoundException exception) {
      LOG.debug("Test didn't think StreamCapabilities class was present.");
    }
  }

  // Here instead of TestCommonFSUtils because we need a minicluster
  @Test
  public void checkStreamCapabilitiesOnHdfsDataOutputStream() throws Exception {
    MiniDFSCluster cluster = htu.startMiniDFSCluster(1);
    try (FileSystem filesystem = cluster.getFileSystem()) {
      FSDataOutputStream stream = filesystem.create(new Path("/tmp/foobar"));
      assertTrue(stream.hasCapability(StreamCapabilities.HSYNC));
      assertTrue(stream.hasCapability(StreamCapabilities.HFLUSH));
      assertFalse(stream.hasCapability("a capability that hopefully HDFS doesn't add."));
    } finally {
      cluster.shutdown();
    }
  }

  private void testIsSameHdfs(int nnport) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    Path srcPath = new Path("hdfs://localhost:" + nnport + "/");
    Path desPath = new Path("hdfs://127.0.0.1/");
    FileSystem srcFs = srcPath.getFileSystem(conf);
    FileSystem desFs = desPath.getFileSystem(conf);

    assertTrue(FSUtils.isSameHdfs(conf, srcFs, desFs));

    desPath = new Path("hdfs://127.0.0.1:8070/");
    desFs = desPath.getFileSystem(conf);
    assertTrue(!FSUtils.isSameHdfs(conf, srcFs, desFs));

    desPath = new Path("hdfs://127.0.1.1:" + nnport + "/");
    desFs = desPath.getFileSystem(conf);
    assertTrue(!FSUtils.isSameHdfs(conf, srcFs, desFs));

    conf.set("fs.defaultFS", "hdfs://haosong-hadoop");
    conf.set("dfs.nameservices", "haosong-hadoop");
    conf.set("dfs.ha.namenodes.haosong-hadoop", "nn1,nn2");
    conf.set("dfs.client.failover.proxy.provider.haosong-hadoop",
      "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

    conf.set("dfs.namenode.rpc-address.haosong-hadoop.nn1", "127.0.0.1:" + nnport);
    conf.set("dfs.namenode.rpc-address.haosong-hadoop.nn2", "127.10.2.1:8000");
    desPath = new Path("/");
    desFs = desPath.getFileSystem(conf);
    assertTrue(FSUtils.isSameHdfs(conf, srcFs, desFs));

    conf.set("dfs.namenode.rpc-address.haosong-hadoop.nn1", "127.10.2.1:" + nnport);
    conf.set("dfs.namenode.rpc-address.haosong-hadoop.nn2", "127.0.0.1:8000");
    desPath = new Path("/");
    desFs = desPath.getFileSystem(conf);
    assertTrue(!FSUtils.isSameHdfs(conf, srcFs, desFs));
  }

  @Test
  public void testIsSameHdfs() throws IOException {
    String hadoopVersion = org.apache.hadoop.util.VersionInfo.getVersion();
    LOG.info("hadoop version is: " + hadoopVersion);
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
}
