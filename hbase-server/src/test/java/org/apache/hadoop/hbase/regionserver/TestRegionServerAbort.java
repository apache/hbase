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

package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests around regionserver shutdown and abort
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestRegionServerAbort {
  private static final byte[] FAMILY_BYTES = Bytes.toBytes("f");

  private static final Log LOG = LogFactory.getLog(TestRegionServerAbort.class);

  private HBaseTestingUtility testUtil;
  private Configuration conf;
  private MiniDFSCluster dfsCluster;
  private MiniHBaseCluster cluster;

  @Before
  public void setup() throws Exception {
    testUtil = new HBaseTestingUtility();
    conf = testUtil.getConfiguration();
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
        StopBlockingRegionObserver.class.getName());
    // make sure we have multiple blocks so that the client does not prefetch all block locations
    conf.set("dfs.blocksize", Long.toString(100 * 1024));
    // prefetch the first block
    conf.set(DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY, Long.toString(100 * 1024));
    conf.set(HConstants.REGION_IMPL, ErrorThrowingHRegion.class.getName());

    testUtil.startMiniZKCluster();
    dfsCluster = testUtil.startMiniDFSCluster(2);
    cluster = testUtil.startMiniHBaseCluster(1, 2);
  }

  @After
  public void tearDown() throws Exception {
    for (JVMClusterUtil.RegionServerThread t : cluster.getRegionServerThreads()) {
      HRegionServer rs = t.getRegionServer();
      RegionServerCoprocessorHost cpHost = rs.getRegionServerCoprocessorHost();
      StopBlockingRegionObserver cp = (StopBlockingRegionObserver)
          cpHost.findCoprocessor(StopBlockingRegionObserver.class.getName());
      cp.setStopAllowed(true);
    }
    testUtil.shutdownMiniCluster();
  }

  /**
   * Test that a regionserver is able to abort properly, even when a coprocessor
   * throws an exception in preStopRegionServer().
   */
  @Test
  public void testAbortFromRPC() throws Exception {
    TableName tableName = TableName.valueOf("testAbortFromRPC");
    // create a test table
    HTable table = testUtil.createTable(tableName, FAMILY_BYTES);

    // write some edits
    testUtil.loadTable(table, FAMILY_BYTES);
    LOG.info("Wrote data");
    // force a flush
    cluster.flushcache(tableName);
    LOG.info("Flushed table");

    // delete a store file from the table region
    HRegion firstRegion = cluster.findRegionsForTable(tableName).get(0);

    // aborting from region
    HRegionFileSystem regionFS = firstRegion.getRegionFileSystem();
    Collection<StoreFileInfo> storeFileInfos = regionFS.getStoreFiles(FAMILY_BYTES);
    assertFalse(storeFileInfos.isEmpty());
    StoreFileInfo firstStoreFile = storeFileInfos.iterator().next();

    // move the store file away
    // we will still be able to read the first block, since the location was pre-fetched on open
    // but attempts to read subsequent blocks will fail
    LOG.info("Moving store file " + firstStoreFile.getPath());
    FileSystem fs = regionFS.getFileSystem();
    Path tmpdir = new Path("/tmp");
    fs.mkdirs(tmpdir);
    assertTrue(fs.rename(firstStoreFile.getPath(),
        new Path(tmpdir, firstStoreFile.getPath().getName())));

    // start a scan, this should trigger a regionserver abort
    ResultScanner scanner = table.getScanner(new Scan());
    int count = 0;
    for (Result f : scanner) {
      count++;
    }
    LOG.info("Finished scan with " + count + " results");
    // should have triggered an abort due to FileNotFoundException

    // verify that the regionserver is stopped
    assertTrue(firstRegion.getRegionServerServices().isAborted());
    assertTrue(firstRegion.getRegionServerServices().isStopped());
  }

  /**
   * Test that a coprocessor is able to override a normal regionserver stop request.
   */
  @Test
  public void testStopOverrideFromCoprocessor() throws Exception {
    Admin admin = testUtil.getHBaseAdmin();
    HRegionServer regionserver = cluster.getRegionServer(0);
    admin.stopRegionServer(regionserver.getServerName().getHostAndPort());

    // regionserver should have failed to stop due to coprocessor
    assertFalse(cluster.getRegionServer(0).isAborted());
    assertFalse(cluster.getRegionServer(0).isStopped());
  }

  public static class StopBlockingRegionObserver extends BaseRegionServerObserver {
    private boolean stopAllowed;

    @Override
    public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env)
        throws IOException {
      if (!stopAllowed) {
        throw new IOException("Stop not allowed");
      }
    }

    public void setStopAllowed(boolean allowed) {
      this.stopAllowed = allowed;
    }

    public boolean isStopAllowed() {
      return stopAllowed;
    }
  }

  /**
   * Throws an exception during store file refresh in order to trigger a regionserver abort.
   */
  public static class ErrorThrowingHRegion extends HRegion {
    public ErrorThrowingHRegion(Path tableDir, WAL wal, FileSystem fs, Configuration confParam,
                                HRegionInfo regionInfo, HTableDescriptor htd,
                                RegionServerServices rsServices) {
      super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
    }

    public ErrorThrowingHRegion(HRegionFileSystem fs, WAL wal, Configuration confParam,
                                HTableDescriptor htd, RegionServerServices rsServices) {
      super(fs, wal, confParam, htd, rsServices);
    }

    @Override
    protected boolean refreshStoreFiles(boolean force) throws IOException {
      // forced when called through RegionScannerImpl.handleFileNotFound()
      if (force) {
        throw new IOException("Failing file refresh for testing");
      }
      return super.refreshStoreFiles(force);
    }
  }
}
