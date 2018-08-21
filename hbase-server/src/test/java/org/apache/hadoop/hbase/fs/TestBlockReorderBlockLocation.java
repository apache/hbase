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
package org.apache.hadoop.hbase.fs;

import java.lang.reflect.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Tests for the hdfs fix from HBASE-6435.
 *
 * Please don't add new subtest which involves starting / stopping MiniDFSCluster in this class.
 * When stopping MiniDFSCluster, shutdown hooks would be cleared in hadoop's ShutdownHookManager
 *   in hadoop 3.
 * This leads to 'Failed suppression of fs shutdown hook' error in region server.
 */
@Category({MiscTests.class, LargeTests.class})
public class TestBlockReorderBlockLocation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBlockReorderBlockLocation.class);

  private Configuration conf;
  private MiniDFSCluster cluster;
  private HBaseTestingUtility htu;
  private DistributedFileSystem dfs;
  private static final String host1 = "host1";
  private static final String host2 = "host2";
  private static final String host3 = "host3";

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    htu = new HBaseTestingUtility();
    htu.getConfiguration().setInt("dfs.blocksize", 1024);// For the test with multiple blocks
    htu.getConfiguration().setInt("dfs.replication", 3);
    htu.startMiniDFSCluster(3,
        new String[]{"/r1", "/r2", "/r3"}, new String[]{host1, host2, host3});

    conf = htu.getConfiguration();
    cluster = htu.getDFSCluster();
    dfs = (DistributedFileSystem) FileSystem.get(conf);
  }

  @After
  public void tearDownAfterClass() throws Exception {
    htu.shutdownMiniCluster();
  }


  private static ClientProtocol getNamenode(DFSClient dfsc) throws Exception {
    Field nf = DFSClient.class.getDeclaredField("namenode");
    nf.setAccessible(true);
    return (ClientProtocol) nf.get(dfsc);
  }

  /**
   * Test that the reorder algo works as we expect.
   */
  @Test
  public void testBlockLocation() throws Exception {
    // We need to start HBase to get  HConstants.HBASE_DIR set in conf
    htu.startMiniZKCluster();
    MiniHBaseCluster hbm = htu.startMiniHBaseCluster();
    conf = hbm.getConfiguration();


    // The "/" is mandatory, without it we've got a null pointer exception on the namenode
    final String fileName = "/helloWorld";
    Path p = new Path(fileName);

    final int repCount = 3;
    Assert.assertTrue((short) cluster.getDataNodes().size() >= repCount);

    // Let's write the file
    FSDataOutputStream fop = dfs.create(p, (short) repCount);
    final double toWrite = 875.5613;
    fop.writeDouble(toWrite);
    fop.close();

    for (int i=0; i<10; i++){
      // The interceptor is not set in this test, so we get the raw list at this point
      LocatedBlocks l;
      final long max = System.currentTimeMillis() + 10000;
      do {
        l = getNamenode(dfs.getClient()).getBlockLocations(fileName, 0, 1);
        Assert.assertNotNull(l.getLocatedBlocks());
        Assert.assertEquals(1, l.getLocatedBlocks().size());
        Assert.assertTrue("Expecting " + repCount + " , got " + l.get(0).getLocations().length,
            System.currentTimeMillis() < max);
      } while (l.get(0).getLocations().length != repCount);

      // Should be filtered, the name is different => The order won't change
      Object originalList [] = l.getLocatedBlocks().toArray();
      HFileSystem.ReorderWALBlocks lrb = new HFileSystem.ReorderWALBlocks();
      lrb.reorderBlocks(conf, l, fileName);
      Assert.assertArrayEquals(originalList, l.getLocatedBlocks().toArray());

      // Should be reordered, as we pretend to be a file name with a compliant stuff
      Assert.assertNotNull(conf.get(HConstants.HBASE_DIR));
      Assert.assertFalse(conf.get(HConstants.HBASE_DIR).isEmpty());
      String pseudoLogFile = conf.get(HConstants.HBASE_DIR) + "/" +
          HConstants.HREGION_LOGDIR_NAME + "/" + host1 + ",6977,6576" + "/mylogfile";

      // Check that it will be possible to extract a ServerName from our construction
      Assert.assertNotNull("log= " + pseudoLogFile,
        AbstractFSWALProvider.getServerNameFromWALDirectoryName(dfs.getConf(), pseudoLogFile));

      // And check we're doing the right reorder.
      lrb.reorderBlocks(conf, l, pseudoLogFile);
      Assert.assertEquals(host1, l.get(0).getLocations()[2].getHostName());

      // Check again, it should remain the same.
      lrb.reorderBlocks(conf, l, pseudoLogFile);
      Assert.assertEquals(host1, l.get(0).getLocations()[2].getHostName());
    }
  }

}
