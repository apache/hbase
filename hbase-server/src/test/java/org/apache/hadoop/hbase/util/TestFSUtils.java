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
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSHedgedReadMetrics;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test {@link FSUtils}.
 */
@Category({MiscTests.class, MediumTests.class})
public class TestFSUtils {
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

      FSUtils.setStoragePolicy(fs, conf, testDir, HConstants.WAL_STORAGE_POLICY,
          HConstants.DEFAULT_WAL_STORAGE_POLICY);

      String file = UUID.randomUUID().toString();
      Path p = new Path(testDir, file);
      WriteDataToHDFS(fs, p, 4096);
      // will assert existance before deleting.
      cleanupFile(fs, testDir);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testSetStoragePolicyDefault() throws Exception {
    verifyFileInDirWithStoragePolicy(HConstants.DEFAULT_WAL_STORAGE_POLICY);
  }

  /* might log a warning, but still work. (always warning on Hadoop < 2.6.0) */
  @Test
  public void testSetStoragePolicyValidButMaybeNotPresent() throws Exception {
    verifyFileInDirWithStoragePolicy("ALL_SSD");
  }

  /* should log a warning, but still work. (different warning on Hadoop < 2.6.0) */
  @Test
  public void testSetStoragePolicyInvalid() throws Exception {
    verifyFileInDirWithStoragePolicy("1772");
  }

  /**
   * Ugly test that ensures we can get at the hedged read counters in dfsclient.
   * Does a bit of preading with hedged reads enabled using code taken from hdfs TestPread.
   * @throws Exception
   */
  @Test public void testDFSHedgedReadMetrics() throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    // Enable hedged reads and set it so the threshold is really low.
    // Most of this test is taken from HDFS, from TestPread.
    Configuration conf = htu.getConfiguration();
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

  // Below is taken from TestPread over in HDFS.
  static final int blockSize = 4096;
  static final long seed = 0xDEADBEEFL;

  private void pReadFile(FileSystem fileSys, Path name) throws IOException {
    FSDataInputStream stm = fileSys.open(name);
    byte[] expected = new byte[12 * blockSize];
    Random rand = new Random(seed);
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
}
