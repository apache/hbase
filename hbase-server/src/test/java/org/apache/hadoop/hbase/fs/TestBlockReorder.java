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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for the hdfs fix from HBASE-6435.
 */
@Category({MiscTests.class, LargeTests.class})
public class TestBlockReorder {
  private static final Log LOG = LogFactory.getLog(TestBlockReorder.class);

  static {
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) HFileSystem.LOG).getLogger().setLevel(Level.ALL);
  }

  private Configuration conf;
  private MiniDFSCluster cluster;
  private HBaseTestingUtility htu;
  private DistributedFileSystem dfs;
  private static final String host1 = "host1";
  private static final String host2 = "host2";
  private static final String host3 = "host3";

  @Before
  public void setUp() throws Exception {
    htu = new HBaseTestingUtility();
    htu.getConfiguration().setInt("dfs.blocksize", 1024);// For the test with multiple blocks
    htu.getConfiguration().setBoolean("dfs.support.append", true);
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
   * Test that we're can add a hook, and that this hook works when we try to read the file in HDFS.
   */
  @Test
  public void testBlockLocationReorder() throws Exception {
    Path p = new Path("hello");

    Assert.assertTrue((short) cluster.getDataNodes().size() > 1);
    final int repCount = 2;

    // Let's write the file
    FSDataOutputStream fop = dfs.create(p, (short) repCount);
    final double toWrite = 875.5613;
    fop.writeDouble(toWrite);
    fop.close();

    // Let's check we can read it when everybody's there
    long start = System.currentTimeMillis();
    FSDataInputStream fin = dfs.open(p);
    Assert.assertTrue(toWrite == fin.readDouble());
    long end = System.currentTimeMillis();
    LOG.info("readtime= " + (end - start));
    fin.close();
    Assert.assertTrue((end - start) < 30 * 1000);

    // Let's kill the first location. But actually the fist location returned will change
    // The first thing to do is to get the location, then the port
    FileStatus f = dfs.getFileStatus(p);
    BlockLocation[] lbs;
    do {
      lbs = dfs.getFileBlockLocations(f, 0, 1);
    } while (lbs.length != 1 && lbs[0].getLength() != repCount);
    final String name = lbs[0].getNames()[0];
    Assert.assertTrue(name.indexOf(':') > 0);
    String portS = name.substring(name.indexOf(':') + 1);
    final int port = Integer.parseInt(portS);
    LOG.info("port= " + port);
    int ipcPort = -1;

    // Let's find the DN to kill. cluster.getDataNodes(int) is not on the same port, so we need
    // to iterate ourselves.
    boolean ok = false;
    final String lookup = lbs[0].getHosts()[0];
    StringBuilder sb = new StringBuilder();
    for (DataNode dn : cluster.getDataNodes()) {
      final String dnName = getHostName(dn);
      sb.append(dnName).append(' ');
      if (lookup.equals(dnName)) {
        ok = true;
        LOG.info("killing datanode " + name + " / " + lookup);
        ipcPort = dn.ipcServer.getListenerAddress().getPort();
        dn.shutdown();
        LOG.info("killed datanode " + name + " / " + lookup);
        break;
      }
    }
    Assert.assertTrue(
        "didn't find the server to kill, was looking for " + lookup + " found " + sb, ok);
    LOG.info("ipc port= " + ipcPort);

    // Add the hook, with an implementation checking that we don't use the port we've just killed.
    Assert.assertTrue(HFileSystem.addLocationsOrderInterceptor(conf,
        new HFileSystem.ReorderBlocks() {
          @Override
          public void reorderBlocks(Configuration c, LocatedBlocks lbs, String src) {
            for (LocatedBlock lb : lbs.getLocatedBlocks()) {
              if (lb.getLocations().length > 1) {
                DatanodeInfo[] infos = lb.getLocations();
                if (infos[0].getHostName().equals(lookup)) {
                  LOG.info("HFileSystem bad host, inverting");
                  DatanodeInfo tmp = infos[0];
                  infos[0] = infos[1];
                  infos[1] = tmp;
                }
              }
            }
          }
        }));


    final int retries = 10;
    ServerSocket ss = null;
    ServerSocket ssI;
    try {
      ss = new ServerSocket(port);// We're taking the port to have a timeout issue later.
      ssI = new ServerSocket(ipcPort);
    } catch (BindException be) {
      LOG.warn("Got bind exception trying to set up socket on " + port + " or " + ipcPort +
          ", this means that the datanode has not closed the socket or" +
          " someone else took it. It may happen, skipping this test for this time.", be);
      if (ss != null) {
        ss.close();
      }
      return;
    }

    // Now it will fail with a timeout, unfortunately it does not always connect to the same box,
    // so we try retries times;  with the reorder it will never last more than a few milli seconds
    for (int i = 0; i < retries; i++) {
      start = System.currentTimeMillis();

      fin = dfs.open(p);
      Assert.assertTrue(toWrite == fin.readDouble());
      fin.close();
      end = System.currentTimeMillis();
      LOG.info("HFileSystem readtime= " + (end - start));
      Assert.assertFalse("We took too much time to read", (end - start) > 60000);
    }

    ss.close();
    ssI.close();
  }

  /**
   * Allow to get the hostname, using getHostName (hadoop 1) or getDisplayName (hadoop 2)
   */
  private String getHostName(DataNode dn) throws InvocationTargetException, IllegalAccessException {
    Method m;
    try {
      m = DataNode.class.getMethod("getDisplayName");
    } catch (NoSuchMethodException e) {
      try {
        m = DataNode.class.getMethod("getHostName");
      } catch (NoSuchMethodException e1) {
        throw new RuntimeException(e1);
      }
    }

    String res = (String) m.invoke(dn);
    if (res.contains(":")) {
      return res.split(":")[0];
    } else {
      return res;
    }
  }

  /**
   * Test that the hook works within HBase, including when there are multiple blocks.
   */
  @Test()
  public void testHBaseCluster() throws Exception {
    byte[] sb = "sb".getBytes();
    htu.startMiniZKCluster();

    MiniHBaseCluster hbm = htu.startMiniHBaseCluster(1, 1);
    hbm.waitForActiveAndReadyMaster();
    HRegionServer targetRs = hbm.getMaster();

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
    Table h = htu.createTable(TableName.valueOf("table"), sb);

    // Now, we have 4 datanodes and a replication count of 3. So we don't know if the datanode
    // with the same node will be used. We can't really stop an existing datanode, this would
    // make us fall in nasty hdfs bugs/issues. So we're going to try multiple times.

    // Now we need to find the log file, its locations, and look at it

    String rootDir = new Path(FSUtils.getRootDir(conf) + "/" + HConstants.HREGION_LOGDIR_NAME +
            "/" + targetRs.getServerName().toString()).toUri().getPath();

    DistributedFileSystem mdfs = (DistributedFileSystem)
        hbm.getMaster().getMasterFileSystem().getFileSystem();


    int nbTest = 0;
    while (nbTest < 10) {
      final List<Region> regions = targetRs.getOnlineRegions(h.getName());
      final CountDownLatch latch = new CountDownLatch(regions.size());
      // listen for successful log rolls
      final WALActionsListener listener = new WALActionsListener.Base() {
            @Override
            public void postLogRoll(final Path oldPath, final Path newPath) throws IOException {
              latch.countDown();
            }
          };
      for (Region region : regions) {
        ((HRegion)region).getWAL().registerWALActionsListener(listener);
      }

      htu.getHBaseAdmin().rollWALWriter(targetRs.getServerName());

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
      p.add(sb, sb, sb);
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

  /**
   * Test that the reorder algo works as we expect.
   */
  @Test
  public void testBlockLocation() throws Exception {
    // We need to start HBase to get  HConstants.HBASE_DIR set in conf
    htu.startMiniZKCluster();
    MiniHBaseCluster hbm = htu.startMiniHBaseCluster(1, 1);
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
        Assert.assertEquals(l.getLocatedBlocks().size(), 1);
        Assert.assertTrue("Expecting " + repCount + " , got " + l.get(0).getLocations().length,
            System.currentTimeMillis() < max);
      } while (l.get(0).getLocations().length != repCount);

      // Should be filtered, the name is different => The order won't change
      Object originalList[] = l.getLocatedBlocks().toArray();
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
          DefaultWALProvider.getServerNameFromWALDirectoryName(dfs.getConf(), pseudoLogFile));

      // And check we're doing the right reorder.
      lrb.reorderBlocks(conf, l, pseudoLogFile);
      Assert.assertEquals(host1, l.get(0).getLocations()[2].getHostName());

      // Check again, it should remain the same.
      lrb.reorderBlocks(conf, l, pseudoLogFile);
      Assert.assertEquals(host1, l.get(0).getLocations()[2].getHostName());
    }
  }

}
