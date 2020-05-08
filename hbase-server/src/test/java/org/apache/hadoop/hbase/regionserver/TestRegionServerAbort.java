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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasRegionServerServices;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests around regionserver shutdown and abort
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestRegionServerAbort {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionServerAbort.class);

  private static final byte[] FAMILY_BYTES = Bytes.toBytes("f");

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionServerAbort.class);

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
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        StopBlockingRegionObserver.class.getName());
    // make sure we have multiple blocks so that the client does not prefetch all block locations
    conf.set("dfs.blocksize", Long.toString(100 * 1024));
    // prefetch the first block
    conf.set(DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY, Long.toString(100 * 1024));
    conf.set(HConstants.REGION_IMPL, ErrorThrowingHRegion.class.getName());

    testUtil.startMiniZKCluster();
    dfsCluster = testUtil.startMiniDFSCluster(2);
    StartMiniClusterOption option = StartMiniClusterOption.builder().numRegionServers(2).build();
    cluster = testUtil.startMiniHBaseCluster(option);
  }

  @After
  public void tearDown() throws Exception {
    String className = StopBlockingRegionObserver.class.getName();
    for (JVMClusterUtil.RegionServerThread t : cluster.getRegionServerThreads()) {
      HRegionServer rs = t.getRegionServer();
      RegionServerCoprocessorHost cpHost = rs.getRegionServerCoprocessorHost();
      StopBlockingRegionObserver cp = (StopBlockingRegionObserver)cpHost.findCoprocessor(className);
      cp.setStopAllowed(true);
    }
    HMaster master = cluster.getMaster();
    RegionServerCoprocessorHost host = master.getRegionServerCoprocessorHost();
    if (host != null) {
      StopBlockingRegionObserver obs = (StopBlockingRegionObserver) host.findCoprocessor(className);
      if (obs != null) obs.setStopAllowed(true);
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
    Table table = testUtil.createTable(tableName, FAMILY_BYTES);

    // write some edits
    testUtil.loadTable(table, FAMILY_BYTES);
    LOG.info("Wrote data");
    // force a flush
    cluster.flushcache(tableName);
    LOG.info("Flushed table");

    // Send a poisoned put to trigger the abort
    Put put = new Put(new byte[]{0, 0, 0, 0});
    put.addColumn(FAMILY_BYTES, Bytes.toBytes("c"), new byte[]{});
    put.setAttribute(StopBlockingRegionObserver.DO_ABORT, new byte[]{1});

    List<HRegion> regions = cluster.findRegionsForTable(tableName);
    HRegion firstRegion = cluster.findRegionsForTable(tableName).get(0);
    table.put(put);
    // Verify that the regionserver is stopped
    assertNotNull(firstRegion);
    assertNotNull(firstRegion.getRegionServerServices());
    LOG.info("isAborted = " + firstRegion.getRegionServerServices().isAborted());
    assertTrue(firstRegion.getRegionServerServices().isAborted());
    LOG.info("isStopped = " + firstRegion.getRegionServerServices().isStopped());
    assertTrue(firstRegion.getRegionServerServices().isStopped());
  }

  /**
   * Test that a coprocessor is able to override a normal regionserver stop request.
   */
  @Test
  public void testStopOverrideFromCoprocessor() throws Exception {
    Admin admin = testUtil.getHBaseAdmin();
    HRegionServer regionserver = cluster.getRegionServer(0);
    admin.stopRegionServer(regionserver.getServerName().getAddress().toString());

    // regionserver should have failed to stop due to coprocessor
    assertFalse(cluster.getRegionServer(0).isAborted());
    assertFalse(cluster.getRegionServer(0).isStopped());
  }

  /**
   * Tests that only a single abort is processed when multiple aborts are requested.
   */
  @Test
  public void testMultiAbort() {
    assertTrue(cluster.getRegionServerThreads().size() > 0);
    JVMClusterUtil.RegionServerThread t = cluster.getRegionServerThreads().get(0);
    assertTrue(t.isAlive());
    HRegionServer rs = t.getRegionServer();
    assertFalse(rs.isAborted());
    RegionServerCoprocessorHost cpHost = rs.getRegionServerCoprocessorHost();
    StopBlockingRegionObserver cp = (StopBlockingRegionObserver)cpHost.findCoprocessor(
        StopBlockingRegionObserver.class.getName());
    // Enable clean abort.
    cp.setStopAllowed(true);
    // Issue two aborts in quick succession.
    // We need a thread pool here, otherwise the abort() runs into SecurityException when running
    // from the fork join pool when setting the context classloader.
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      CompletableFuture.runAsync(() -> rs.abort("Abort 1"), executor);
      CompletableFuture.runAsync(() -> rs.abort("Abort 2"), executor);
      long testTimeoutMs = 10 * 1000;
      Waiter.waitFor(cluster.getConf(), testTimeoutMs, (Waiter.Predicate<Exception>) rs::isStopped);
      // Make sure only one abort is received.
      assertEquals(1, cp.getNumAbortsRequested());
    } finally {
      executor.shutdownNow();
    }
  }

  @CoreCoprocessor
  public static class StopBlockingRegionObserver
      implements RegionServerCoprocessor, RegionCoprocessor, RegionServerObserver, RegionObserver {
    public static final String DO_ABORT = "DO_ABORT";
    private boolean stopAllowed;
    private AtomicInteger abortCount = new AtomicInteger();

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public Optional<RegionServerObserver> getRegionServerObserver() {
      return Optional.of(this);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
                       Durability durability) throws IOException {
      if (put.getAttribute(DO_ABORT) != null) {
        // TODO: Change this so it throws a CP Abort Exception instead.
        RegionServerServices rss =
            ((HasRegionServerServices)c.getEnvironment()).getRegionServerServices();
        String str = "Aborting for test";
        LOG.info(str  + " " + rss.getServerName());
        rss.abort(str, new Throwable(str));
      }
    }

    @Override
    public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env)
        throws IOException {
      abortCount.incrementAndGet();
      if (!stopAllowed) {
        throw new IOException("Stop not allowed");
      }
    }

    public int getNumAbortsRequested() {
      return abortCount.get();
    }

    public void setStopAllowed(boolean allowed) {
      this.stopAllowed = allowed;
    }
  }

  /**
   * Throws an exception during store file refresh in order to trigger a regionserver abort.
   */
  public static class ErrorThrowingHRegion extends HRegion {
    public ErrorThrowingHRegion(Path tableDir, WAL wal, FileSystem fs, Configuration confParam,
                                RegionInfo regionInfo, TableDescriptor htd,
                                RegionServerServices rsServices) {
      super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
    }

    public ErrorThrowingHRegion(HRegionFileSystem fs, WAL wal, Configuration confParam,
                                TableDescriptor htd, RegionServerServices rsServices) {
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
