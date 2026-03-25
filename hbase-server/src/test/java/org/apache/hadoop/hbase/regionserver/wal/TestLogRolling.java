/*
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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowRegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.RecoverLeaseFSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.FSHLogProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALStreamReader;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ VerySlowRegionServerTests.class, LargeTests.class })
public class TestLogRolling extends AbstractTestLogRolling {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLogRolling.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestLogRolling.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // TODO: testLogRollOnDatanodeDeath fails if short circuit reads are on under the hadoop2
    // profile. See HBASE-9337 for related issues.
    System.setProperty("hbase.tests.use.shortcircuit.reads", "false");

    /**** configuration for testLogRollOnDatanodeDeath ****/
    // lower the namenode & datanode heartbeat so the namenode
    // quickly detects datanode failures
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    conf.setInt("dfs.heartbeat.interval", 1);
    // the namenode might still try to choose the recently-dead datanode
    // for a pipeline, so try to a new pipeline multiple times
    conf.setInt("dfs.client.block.write.retries", 30);
    conf.setInt("hbase.regionserver.hlog.tolerable.lowreplication", 2);
    conf.setInt("hbase.regionserver.hlog.lowreplication.rolllimit", 3);
    conf.set(WALFactory.WAL_PROVIDER, "filesystem");
    AbstractTestLogRolling.setUpBeforeClass();
  }

  public static class SlowSyncLogWriter extends ProtobufLogWriter {
    @Override
    public void sync(boolean forceSync) throws IOException {
      try {
        Thread.sleep(syncLatencyMillis);
      } catch (InterruptedException e) {
        InterruptedIOException ex = new InterruptedIOException();
        ex.initCause(e);
        throw ex;
      }
      super.sync(forceSync);
    }
  }

  @Override
  protected void setSlowLogWriter(Configuration conf) {
    conf.set(FSHLogProvider.WRITER_IMPL, SlowSyncLogWriter.class.getName());
  }

  @Override
  protected void setDefaultLogWriter(Configuration conf) {
    conf.set(FSHLogProvider.WRITER_IMPL, ProtobufLogWriter.class.getName());
  }

  void batchWriteAndWait(Table table, final FSHLog log, int start, boolean expect, int timeout)
    throws IOException {
    for (int i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("row" + String.format("%1$04d", (start + i))));
      put.addColumn(HConstants.CATALOG_FAMILY, null, value);
      table.put(put);
    }
    Put tmpPut = new Put(Bytes.toBytes("tmprow"));
    tmpPut.addColumn(HConstants.CATALOG_FAMILY, null, value);
    long startTime = EnvironmentEdgeManager.currentTime();
    long remaining = timeout;
    while (remaining > 0) {
      if (log.isLowReplicationRollEnabled() == expect) {
        break;
      } else {
        // Trigger calling FSHlog#checkLowReplication()
        table.put(tmpPut);
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          // continue
        }
        remaining = timeout - (EnvironmentEdgeManager.currentTime() - startTime);
      }
    }
  }

  @Test
  public void testSlowSyncLogRolling() throws Exception {
    // Create the test table
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(getName()))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();
    admin.createTable(desc);
    try (Table table = TEST_UTIL.getConnection().getTable(desc.getTableName())) {
      server = TEST_UTIL.getRSForFirstRegionInTable(desc.getTableName());
      RegionInfo region = server.getRegions(desc.getTableName()).get(0).getRegionInfo();
      final AbstractFSWAL<?> log = getWALAndRegisterSlowSyncHook(region);

      // Set default log writer, no additional latency to any sync on the hlog.
      checkSlowSync(log, table, -1, 10, false);

      // Adds 200 ms of latency to any sync on the hlog. This should be more than sufficient to
      // trigger slow sync warnings.
      // Write some data.
      // We need to write at least 5 times, but double it. We should only request
      // a SLOW_SYNC roll once in the current interval.
      checkSlowSync(log, table, 200, 10, true);

      // Adds 5000 ms of latency to any sync on the hlog. This will trip the other threshold.
      // Write some data. Should only take one sync.
      checkSlowSync(log, table, 5000, 1, true);

      // Set default log writer, no additional latency to any sync on the hlog.
      checkSlowSync(log, table, -1, 10, false);
    }
  }

  /**
   * Tests that logs are rolled upon detecting datanode death Requires an HDFS jar with HDFS-826 &
   * syncFs() support (HDFS-200)
   */
  @Test
  public void testLogRollOnDatanodeDeath() throws Exception {

    Long oldValue = TEST_UTIL.getConfiguration()
      .getLong("hbase.regionserver.hlog.check.lowreplication.interval", -1);

    try {
      /**
       * When we reuse the code of AsyncFSWAL to FSHLog, the low replication is only checked by
       * {@link LogRoller#checkLowReplication},so in order to make this test spend less time,we
       * should minimize following config which is maximized by
       * {@link AbstractTestLogRolling#setUpBeforeClass}
       */
      TEST_UTIL.getConfiguration().setLong("hbase.regionserver.hlog.check.lowreplication.interval",
        1000);
      this.tearDown();
      this.setUp();

      TEST_UTIL.ensureSomeRegionServersAvailable(2);
      assertTrue("This test requires WAL file replication set to 2.",
        fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()) == 2);
      LOG.info("Replication=" + fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()));

      this.server = cluster.getRegionServer(0);

      // Create the test table and open it
      TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(getName()))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();

      admin.createTable(desc);
      Table table = TEST_UTIL.getConnection().getTable(desc.getTableName());

      server = TEST_UTIL.getRSForFirstRegionInTable(desc.getTableName());
      RegionInfo region = server.getRegions(desc.getTableName()).get(0).getRegionInfo();
      final FSHLog log = (FSHLog) server.getWAL(region);
      final AtomicBoolean lowReplicationHookCalled = new AtomicBoolean(false);

      log.registerWALActionsListener(new WALActionsListener() {
        @Override
        public void logRollRequested(WALActionsListener.RollRequestReason reason) {
          switch (reason) {
            case LOW_REPLICATION:
              lowReplicationHookCalled.lazySet(true);
              break;
            default:
              break;
          }
        }
      });

      // add up the datanode count, to ensure proper replication when we kill 1
      // This function is synchronous; when it returns, the dfs cluster is active
      // We start 3 servers and then stop 2 to avoid a directory naming conflict
      // when we stop/start a namenode later, as mentioned in HBASE-5163
      List<DataNode> existingNodes = dfsCluster.getDataNodes();
      int numDataNodes = 3;
      TEST_UTIL.getConfiguration().setLong("hbase.regionserver.hlog.check.lowreplication.interval",
        1000);
      dfsCluster.startDataNodes(TEST_UTIL.getConfiguration(), numDataNodes, true, null, null);
      List<DataNode> allNodes = dfsCluster.getDataNodes();
      for (int i = allNodes.size() - 1; i >= 0; i--) {
        if (existingNodes.contains(allNodes.get(i))) {
          dfsCluster.stopDataNode(i);
        }
      }

      assertTrue(
        "DataNodes " + dfsCluster.getDataNodes().size() + " default replication "
          + fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()),
        dfsCluster.getDataNodes().size()
            >= fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()) + 1);

      writeData(table, 2);

      long curTime = EnvironmentEdgeManager.currentTime();
      LOG.info("log.getCurrentFileName(): " + log.getCurrentFileName());
      long oldFilenum = AbstractFSWALProvider.extractFileNumFromWAL(log);
      assertTrue("Log should have a timestamp older than now",
        curTime > oldFilenum && oldFilenum != -1);

      assertTrue("The log shouldn't have rolled yet",
        oldFilenum == AbstractFSWALProvider.extractFileNumFromWAL(log));
      final DatanodeInfo[] pipeline = log.getPipeline();
      assertTrue(pipeline.length == fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()));

      // kill a datanode in the pipeline to force a log roll on the next sync()
      // This function is synchronous, when it returns the node is killed.
      assertTrue(dfsCluster.stopDataNode(pipeline[0].getName()) != null);

      // this write should succeed, but trigger a log roll
      writeData(table, 2);

      TEST_UTIL.waitFor(10000, 100, () -> {
        long newFilenum = AbstractFSWALProvider.extractFileNumFromWAL(log);
        return newFilenum > oldFilenum && newFilenum > curTime && lowReplicationHookCalled.get();
      });

      long newFilenum = AbstractFSWALProvider.extractFileNumFromWAL(log);

      // write some more log data (this should use a new hdfs_out)
      writeData(table, 3);
      assertTrue("The log should not roll again.",
        AbstractFSWALProvider.extractFileNumFromWAL(log) == newFilenum);
      // kill another datanode in the pipeline, so the replicas will be lower than
      // the configured value 2.
      assertTrue(dfsCluster.stopDataNode(pipeline[1].getName()) != null);

      batchWriteAndWait(table, log, 3, false, 14000);
      int replication = log.getLogReplication();
      assertTrue(
        "LowReplication Roller should've been disabled, current replication=" + replication,
        !log.isLowReplicationRollEnabled());

      dfsCluster.startDataNodes(TEST_UTIL.getConfiguration(), 1, true, null, null);

      // Force roll writer. The new log file will have the default replications,
      // and the LowReplication Roller will be enabled.
      log.rollWriter(true);
      batchWriteAndWait(table, log, 13, true, 10000);
      replication = log.getLogReplication();
      assertTrue("New log file should have the default replication instead of " + replication,
        replication == fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()));
      assertTrue("LowReplication Roller should've been enabled", log.isLowReplicationRollEnabled());
    } finally {
      TEST_UTIL.getConfiguration().setLong("hbase.regionserver.hlog.check.lowreplication.interval",
        oldValue);
    }
  }

  /**
   * Test that WAL is rolled when all data nodes in the pipeline have been restarted.
   */
  @Test
  public void testLogRollOnPipelineRestart() throws Exception {
    LOG.info("Starting testLogRollOnPipelineRestart");
    assertTrue("This test requires WAL file replication.",
      fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()) > 1);
    LOG.info("Replication=" + fs.getDefaultReplication(TEST_UTIL.getDataTestDirOnTestFS()));
    // When the hbase:meta table can be opened, the region servers are running
    Table t = TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME);
    try {
      this.server = cluster.getRegionServer(0);

      // Create the test table and open it
      TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(getName()))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();

      admin.createTable(desc);
      Table table = TEST_UTIL.getConnection().getTable(desc.getTableName());

      server = TEST_UTIL.getRSForFirstRegionInTable(desc.getTableName());
      RegionInfo region = server.getRegions(desc.getTableName()).get(0).getRegionInfo();
      final WAL log = server.getWAL(region);
      final List<Path> paths = new ArrayList<>(1);
      final List<Integer> preLogRolledCalled = new ArrayList<>();

      paths.add(AbstractFSWALProvider.getCurrentFileName(log));
      log.registerWALActionsListener(new WALActionsListener() {

        @Override
        public void preLogRoll(Path oldFile, Path newFile) {
          LOG.debug("preLogRoll: oldFile=" + oldFile + " newFile=" + newFile);
          preLogRolledCalled.add(1);
        }

        @Override
        public void postLogRoll(Path oldFile, Path newFile) {
          paths.add(newFile);
        }
      });

      writeData(table, 1002);

      long curTime = EnvironmentEdgeManager.currentTime();
      LOG.info("log.getCurrentFileName()): " + AbstractFSWALProvider.getCurrentFileName(log));
      long oldFilenum = AbstractFSWALProvider.extractFileNumFromWAL(log);
      assertTrue("Log should have a timestamp older than now",
        curTime > oldFilenum && oldFilenum != -1);

      assertTrue("The log shouldn't have rolled yet",
        oldFilenum == AbstractFSWALProvider.extractFileNumFromWAL(log));

      // roll all datanodes in the pipeline
      dfsCluster.restartDataNodes();
      Thread.sleep(1000);
      dfsCluster.waitActive();
      LOG.info("Data Nodes restarted");
      validateData(table, 1002);

      // this write should succeed, but trigger a log roll
      writeData(table, 1003);
      long newFilenum = AbstractFSWALProvider.extractFileNumFromWAL(log);

      assertTrue("Missing datanode should've triggered a log roll",
        newFilenum > oldFilenum && newFilenum > curTime);
      validateData(table, 1003);

      writeData(table, 1004);

      // roll all datanode again
      dfsCluster.restartDataNodes();
      Thread.sleep(1000);
      dfsCluster.waitActive();
      LOG.info("Data Nodes restarted");
      validateData(table, 1004);

      // this write should succeed, but trigger a log roll
      writeData(table, 1005);

      // force a log roll to read back and verify previously written logs
      log.rollWriter(true);
      assertTrue("preLogRolledCalled has size of " + preLogRolledCalled.size(),
        preLogRolledCalled.size() >= 1);

      // read back the data written
      Set<String> loggedRows = new HashSet<>();
      for (Path p : paths) {
        LOG.debug("recovering lease for " + p);
        RecoverLeaseFSUtils.recoverFileLease(((HFileSystem) fs).getBackingFs(), p,
          TEST_UTIL.getConfiguration(), null);

        LOG.debug("Reading WAL " + CommonFSUtils.getPath(p));
        try (WALStreamReader reader =
          WALFactory.createStreamReader(fs, p, TEST_UTIL.getConfiguration())) {
          WAL.Entry entry;
          while ((entry = reader.next()) != null) {
            LOG.debug("#" + entry.getKey().getSequenceId() + ": " + entry.getEdit().getCells());
            for (Cell cell : entry.getEdit().getCells()) {
              loggedRows.add(
                Bytes.toStringBinary(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
            }
          }
        } catch (EOFException e) {
          LOG.debug("EOF reading file " + CommonFSUtils.getPath(p));
        }
      }

      // verify the written rows are there
      assertTrue(loggedRows.contains("row1002"));
      assertTrue(loggedRows.contains("row1003"));
      assertTrue(loggedRows.contains("row1004"));
      assertTrue(loggedRows.contains("row1005"));

      // flush all regions
      for (HRegion r : server.getOnlineRegionsLocalContext()) {
        try {
          r.flush(true);
        } catch (Exception e) {
          // This try/catch was added by HBASE-14317. It is needed
          // because this issue tightened up the semantic such that
          // a failed append could not be followed by a successful
          // sync. What is coming out here is a failed sync, a sync
          // that used to 'pass'.
          LOG.info(e.toString(), e);
        }
      }

      ResultScanner scanner = table.getScanner(new Scan());
      try {
        for (int i = 2; i <= 5; i++) {
          Result r = scanner.next();
          assertNotNull(r);
          assertFalse(r.isEmpty());
          assertEquals("row100" + i, Bytes.toString(r.getRow()));
        }
      } finally {
        scanner.close();
      }

      // verify that no region servers aborted
      for (JVMClusterUtil.RegionServerThread rsThread : TEST_UTIL.getHBaseCluster()
        .getRegionServerThreads()) {
        assertFalse(rsThread.getRegionServer().isAborted());
      }
    } finally {
      if (t != null) t.close();
    }
  }

}
