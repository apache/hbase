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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the hdfs fix from HBASE-6435.
 *
 * Please don't add new subtest which involves starting / stopping MiniDFSCluster in this class.
 * When stopping MiniDFSCluster, shutdown hooks would be cleared in hadoop's ShutdownHookManager
 *   in hadoop 3.
 * This leads to 'Failed suppression of fs shutdown hook' error in region server.
 */
@Category({MiscTests.class, LargeTests.class})
public class TestBlockReorderMultiBlocks {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBlockReorderMultiBlocks.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBlockReorderMultiBlocks.class);

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

  /**
   * Test that the hook works within HBase, including when there are multiple blocks.
   */
  @Test()
  public void testHBaseCluster() throws Exception {
    byte[] sb = Bytes.toBytes("sb");
    htu.startMiniZKCluster();

    MiniHBaseCluster hbm = htu.startMiniHBaseCluster();
    hbm.waitForActiveAndReadyMaster();
    HRegionServer targetRs = LoadBalancer.isTablesOnMaster(hbm.getConf())? hbm.getMaster():
      hbm.getRegionServer(0);

    // We want to have a datanode with the same name as the region server, so
    //  we're going to get the regionservername, and start a new datanode with this name.
    String host4 = targetRs.getServerName().getHostname();
    LOG.info("Starting a new datanode with the name=" + host4);
    cluster.startDataNodes(conf, 1, true, null, new String[]{"/r4"}, new String[]{host4}, null);
    cluster.waitClusterUp();

    final int repCount = 3;

    // We use the regionserver file system & conf as we expect it to have the hook.
    conf = targetRs.getConfiguration();
    HFileSystem rfs = (HFileSystem) targetRs.getFileSystem();
    Table h = htu.createTable(TableName.valueOf(name.getMethodName()), sb);

    // Now, we have 4 datanodes and a replication count of 3. So we don't know if the datanode
    // with the same node will be used. We can't really stop an existing datanode, this would
    // make us fall in nasty hdfs bugs/issues. So we're going to try multiple times.

    // Now we need to find the log file, its locations, and look at it

    String rootDir = new Path(CommonFSUtils.getWALRootDir(conf) + "/" +
      HConstants.HREGION_LOGDIR_NAME + "/" + targetRs.getServerName().toString()).toUri().getPath();

    DistributedFileSystem mdfs = (DistributedFileSystem)
        hbm.getMaster().getMasterFileSystem().getFileSystem();


    int nbTest = 0;
    while (nbTest < 10) {
      final List<HRegion> regions = targetRs.getRegions(h.getName());
      final CountDownLatch latch = new CountDownLatch(regions.size());
      // listen for successful log rolls
      final WALActionsListener listener = new WALActionsListener() {
            @Override
            public void postLogRoll(final Path oldPath, final Path newPath) throws IOException {
              latch.countDown();
            }
          };
      for (HRegion region : regions) {
        region.getWAL().registerWALActionsListener(listener);
      }

      htu.getAdmin().rollWALWriter(targetRs.getServerName());

      // wait
      try {
        latch.await();
      } catch (InterruptedException exception) {
        LOG.warn("Interrupted while waiting for the wal of '" + targetRs + "' to roll. If later " +
            "tests fail, it's probably because we should still be waiting.");
        Thread.currentThread().interrupt();
      }
      for (Region region : regions) {
        ((HRegion)region).getWAL().unregisterWALActionsListener(listener);
      }

      // We need a sleep as the namenode is informed asynchronously
      Thread.sleep(100);

      // insert one put to ensure a minimal size
      Put p = new Put(sb);
      p.addColumn(sb, sb, sb);
      h.put(p);

      DirectoryListing dl = dfs.getClient().listPaths(rootDir, HdfsFileStatus.EMPTY_NAME);
      HdfsFileStatus[] hfs = dl.getPartialListing();

      // As we wrote a put, we should have at least one log file.
      Assert.assertTrue(hfs.length >= 1);
      for (HdfsFileStatus hf : hfs) {
        // Because this is a live cluster, log files might get archived while we're processing
        try {
          LOG.info("Log file found: " + hf.getLocalName() + " in " + rootDir);
          String logFile = rootDir + "/" + hf.getLocalName();
          FileStatus fsLog = rfs.getFileStatus(new Path(logFile));

          LOG.info("Checking log file: " + logFile);
          // Now checking that the hook is up and running
          // We can't call directly getBlockLocations, it's not available in HFileSystem
          // We're trying multiple times to be sure, as the order is random

          BlockLocation[] bls = rfs.getFileBlockLocations(fsLog, 0, 1);
          if (bls.length > 0) {
            BlockLocation bl = bls[0];

            LOG.info(bl.getHosts().length + " replicas for block 0 in " + logFile + " ");
            for (int i = 0; i < bl.getHosts().length - 1; i++) {
              LOG.info(bl.getHosts()[i] + "    " + logFile);
              Assert.assertNotSame(bl.getHosts()[i], host4);
            }
            String last = bl.getHosts()[bl.getHosts().length - 1];
            LOG.info(last + "    " + logFile);
            if (host4.equals(last)) {
              nbTest++;
              LOG.info(logFile + " is on the new datanode and is ok");
              if (bl.getHosts().length == 3) {
                // We can test this case from the file system as well
                // Checking the underlying file system. Multiple times as the order is random
                testFromDFS(dfs, logFile, repCount, host4);

                // now from the master
                testFromDFS(mdfs, logFile, repCount, host4);
              }
            }
          }
        } catch (FileNotFoundException exception) {
          LOG.debug("Failed to find log file '" + hf.getLocalName() + "'; it probably was " +
              "archived out from under us so we'll ignore and retry. If this test hangs " +
              "indefinitely you should treat this failure as a symptom.", exception);
        } catch (RemoteException exception) {
          if (exception.unwrapRemoteException() instanceof FileNotFoundException) {
            LOG.debug("Failed to find log file '" + hf.getLocalName() + "'; it probably was " +
                "archived out from under us so we'll ignore and retry. If this test hangs " +
                "indefinitely you should treat this failure as a symptom.", exception);
          } else {
            throw exception;
          }
        }
      }
    }
  }

  private void testFromDFS(DistributedFileSystem dfs, String src, int repCount, String localhost)
      throws Exception {
    // Multiple times as the order is random
    for (int i = 0; i < 10; i++) {
      LocatedBlocks l;
      // The NN gets the block list asynchronously, so we may need multiple tries to get the list
      final long max = System.currentTimeMillis() + 10000;
      boolean done;
      do {
        Assert.assertTrue("Can't get enouth replica.", System.currentTimeMillis() < max);
        l = getNamenode(dfs.getClient()).getBlockLocations(src, 0, 1);
        Assert.assertNotNull("Can't get block locations for " + src, l);
        Assert.assertNotNull(l.getLocatedBlocks());
        Assert.assertTrue(l.getLocatedBlocks().size() > 0);

        done = true;
        for (int y = 0; y < l.getLocatedBlocks().size() && done; y++) {
          done = (l.get(y).getLocations().length == repCount);
        }
      } while (!done);

      for (int y = 0; y < l.getLocatedBlocks().size() && done; y++) {
        Assert.assertEquals(localhost, l.get(y).getLocations()[repCount - 1].getHostName());
      }
    }
  }

  private static ClientProtocol getNamenode(DFSClient dfsc) throws Exception {
    Field nf = DFSClient.class.getDeclaredField("namenode");
    nf.setAccessible(true);
    return (ClientProtocol) nf.get(dfsc);
  }

}
