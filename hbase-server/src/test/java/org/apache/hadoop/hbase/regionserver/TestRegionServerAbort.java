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
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
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
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        StopBlockingRegionObserver.class.getName());
    // make sure we have multiple blocks so that the client does not prefetch all block locations
    conf.set("dfs.blocksize", Long.toString(100 * 1024));
    // prefetch the first block
    conf.set(DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY, Long.toString(100 * 1024));

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

    table.put(put);
    // should have triggered an abort due to FileNotFoundException

    // verify that the regionserver is stopped
    HRegion firstRegion = cluster.findRegionsForTable(tableName).get(0);
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
    admin.stopRegionServer(regionserver.getServerName().getHostAndPort());

    // regionserver should have failed to stop due to coprocessor
    assertFalse(cluster.getRegionServer(0).isAborted());
    assertFalse(cluster.getRegionServer(0).isStopped());
  }

  public static class StopBlockingRegionObserver extends BaseRegionObserver
      implements RegionServerObserver {
    public static final String DO_ABORT = "DO_ABORT";
    private boolean stopAllowed;

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
                       Durability durability) throws IOException {
      if (put.getAttribute(DO_ABORT) != null) {
        HRegionServer rs = (HRegionServer) c.getEnvironment().getRegionServerServices();
        LOG.info("Triggering abort for regionserver " + rs.getServerName());
        rs.abort("Aborting for test");
      }
    }

    @Override
    public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> env)
        throws IOException {
      if (!stopAllowed) {
        throw new IOException("Stop not allowed");
      }
    }

    @Override
    public void preMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                         Region regionA, Region regionB) throws IOException {
      // no-op
    }

    @Override
    public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c,
                          Region regionA, Region regionB, Region mergedRegion) throws IOException {
      // no-op
    }

    @Override
    public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                               Region regionA, Region regionB, List<Mutation> metaEntries)
        throws IOException {
      // no-op
    }

    @Override
    public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                                Region regionA, Region regionB, Region mergedRegion)
        throws IOException {
      // no-op
    }

    @Override
    public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                                 Region regionA, Region regionB) throws IOException {
      // no-op
    }

    @Override
    public void postRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                                  Region regionA, Region regionB) throws IOException {
      // no-op
    }

    @Override
    public void preRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
        throws IOException {
      // no-op
    }

    @Override
    public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
        throws IOException {
      // no-op
    }

    @Override
    public ReplicationEndpoint postCreateReplicationEndPoint(
        ObserverContext<RegionServerCoprocessorEnvironment> ctx, ReplicationEndpoint endpoint) {
      return null;
    }

    @Override
    public void preReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                                       List<AdminProtos.WALEntry> entries, CellScanner cells)
        throws IOException {
      // no-op
    }

    @Override
    public void postReplicateLogEntries(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
                                        List<AdminProtos.WALEntry> entries, CellScanner cells)
        throws IOException {
      // no-op
    }

    public void setStopAllowed(boolean allowed) {
      this.stopAllowed = allowed;
    }

    public boolean isStopAllowed() {
      return stopAllowed;
    }
  }
}
