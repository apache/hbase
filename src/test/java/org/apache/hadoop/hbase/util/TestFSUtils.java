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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.UUID;

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
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test {@link FSUtils}.
 */
@Category(MediumTests.class)
public class TestFSUtils {
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
    htu.getConfiguration().setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
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
    conf.setBoolean(HConstants.ENABLE_DATA_FILE_UMASK, true);
    FileSystem fs = FileSystem.get(conf);
    // first check that we don't crash if we don't have perms set
    FsPermission defaultPerms = FSUtils.getFilePermissions(fs, conf,
        HConstants.DATA_FILE_UMASK_KEY);
    assertEquals(FsPermission.getDefault(), defaultPerms);

    conf.setStrings(HConstants.DATA_FILE_UMASK_KEY, "077");
    // now check that we get the right perms
    FsPermission filePerm = FSUtils.getFilePermissions(fs, conf,
        HConstants.DATA_FILE_UMASK_KEY);
    assertEquals(new FsPermission("700"), filePerm);

    // then that the correct file is created
    Path p = new Path("target" + File.separator + UUID.randomUUID().toString());
    try {
      FSDataOutputStream out = FSUtils.create(fs, p, filePerm);
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
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.ENABLE_DATA_FILE_UMASK, true);
    FileSystem fs = FileSystem.get(conf);
    FsPermission perms = FSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
    // then that the correct file is created
    String file = UUID.randomUUID().toString();
    Path p = new Path("temptarget" + File.separator + file);
    Path p1 = new Path("temppath" + File.separator + file);
    try {
      FSDataOutputStream out = FSUtils.create(fs, p, perms);
      out.close();
      assertTrue("The created file should be present", FSUtils.isExists(fs, p));
      // delete the file with recursion as false. Only the file will be deleted.
      FSUtils.delete(fs, p, false);
      // Create another file
      FSDataOutputStream out1 = FSUtils.create(fs, p1, perms);
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

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

