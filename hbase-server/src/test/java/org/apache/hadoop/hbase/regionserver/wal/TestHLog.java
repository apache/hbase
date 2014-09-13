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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** JUnit test case for HLog */
@Category({RegionServerTests.class, LargeTests.class})
@SuppressWarnings("deprecation")
public class TestHLog  {
  private static final Log LOG = LogFactory.getLog(TestHLog.class);
  {
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LogFactory.getLog("org.apache.hadoop.hdfs.server.namenode.FSNamesystem"))
      .getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)HLog.LOG).getLogger().setLevel(Level.ALL);
  }

  private static Configuration conf;
  private static FileSystem fs;
  private static Path dir;
  private static MiniDFSCluster cluster;
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Path hbaseDir;
  private static Path oldLogDir;

  @Before
  public void setUp() throws Exception {
    FileStatus[] entries = fs.listStatus(new Path("/"));
    for (FileStatus dir : entries) {
      fs.delete(dir.getPath(), true);
    }
  }

  @After
  public void tearDown() throws Exception {
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Make block sizes small.
    TEST_UTIL.getConfiguration().setInt("dfs.blocksize", 1024 * 1024);
    // needed for testAppendClose()
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.broken.append", true);
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    // quicker heartbeat interval for faster DN death notification
    TEST_UTIL.getConfiguration().setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.client.socket-timeout", 5000);

    // faster failover with cluster.shutdown();fs.close() idiom
    TEST_UTIL.getConfiguration()
        .setInt("hbase.ipc.client.connect.max.retries", 1);
    TEST_UTIL.getConfiguration().setInt(
        "dfs.client.block.recovery.retries", 1);
    TEST_UTIL.getConfiguration().setInt(
      "hbase.ipc.client.connection.maxidletime", 500);
    TEST_UTIL.getConfiguration().set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
        SampleRegionWALObserver.class.getName());
    TEST_UTIL.startMiniDFSCluster(3);

    conf = TEST_UTIL.getConfiguration();
    cluster = TEST_UTIL.getDFSCluster();
    fs = cluster.getFileSystem();

    hbaseDir = TEST_UTIL.createRootDir();
    oldLogDir = new Path(hbaseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    dir = new Path(hbaseDir, getName());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private static String getName() {
    // TODO Auto-generated method stub
    return "TestHLog";
  }

  /**
   * Test flush for sure has a sequence id that is beyond the last edit appended.  We do this
   * by slowing appends in the background ring buffer thread while in foreground we call
   * flush.  The addition of the sync over HRegion in flush should fix an issue where flush was
   * returning before all of its appends had made it out to the WAL (HBASE-11109).
   * @throws IOException 
   * @see HBASE-11109
   */
  @Test
  public void testFlushSequenceIdIsGreaterThanAllEditsInHFile() throws IOException {
    String testName = "testFlushSequenceIdIsGreaterThanAllEditsInHFile";
    final TableName tableName = TableName.valueOf(testName);
    final HRegionInfo hri = new HRegionInfo(tableName);
    final byte[] rowName = tableName.getName();
    final HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("f"));
    HRegion r = HRegion.createHRegion(hri, TEST_UTIL.getDefaultRootDirPath(),
      TEST_UTIL.getConfiguration(), htd);
    HRegion.closeHRegion(r);
    final int countPerFamily = 10;
    final MutableBoolean goslow = new MutableBoolean(false);
    // Bypass factory so I can subclass and doctor a method.
    FSHLog wal = new FSHLog(FileSystem.get(conf), TEST_UTIL.getDefaultRootDirPath(),
        testName, conf) {
      @Override
      void atHeadOfRingBufferEventHandlerAppend() {
        if (goslow.isTrue()) {
          Threads.sleep(100);
          LOG.debug("Sleeping before appending 100ms");
        }
        super.atHeadOfRingBufferEventHandlerAppend();
      }
    };
    HRegion region = HRegion.openHRegion(TEST_UTIL.getConfiguration(),
      TEST_UTIL.getTestFileSystem(), TEST_UTIL.getDefaultRootDirPath(), hri, htd, wal);
    EnvironmentEdge ee = EnvironmentEdgeManager.getDelegate();
    try {
      List<Put> puts = null;
      for (HColumnDescriptor hcd: htd.getFamilies()) {
        puts =
          TestWALReplay.addRegionEdits(rowName, hcd.getName(), countPerFamily, ee, region, "x");
      }

      // Now assert edits made it in.
      final Get g = new Get(rowName);
      Result result = region.get(g);
      assertEquals(countPerFamily * htd.getFamilies().size(), result.size());

      // Construct a WALEdit and add it a few times to the WAL.
      WALEdit edits = new WALEdit();
      for (Put p: puts) {
        CellScanner cs = p.cellScanner();
        while (cs.advance()) {
          edits.add(cs.current());
        }
      }
      // Add any old cluster id.
      List<UUID> clusterIds = new ArrayList<UUID>();
      clusterIds.add(UUID.randomUUID());
      // Now make appends run slow.
      goslow.setValue(true);
      for (int i = 0; i < countPerFamily; i++) {
        wal.appendNoSync(region.getRegionInfo(), tableName, edits,
          clusterIds, System.currentTimeMillis(), htd, region.getSequenceId(), true, -1, -1);
      }
      region.flushcache();
      // FlushResult.flushSequenceId is not visible here so go get the current sequence id.
      long currentSequenceId = region.getSequenceId().get();
      // Now release the appends
      goslow.setValue(false);
      synchronized (goslow) {
        goslow.notifyAll();
      }
      assertTrue(currentSequenceId >= region.getSequenceId().get());
    } finally {
      region.close(true);
      wal.close();
    }
  }

  /**
   * Write to a log file with three concurrent threads and verifying all data is written.
   * @throws Exception
   */
  @Test
  public void testConcurrentWrites() throws Exception {
    // Run the HPE tool with three threads writing 3000 edits each concurrently.
    // When done, verify that all edits were written.
    int errCode = HLogPerformanceEvaluation.
      innerMain(new Configuration(TEST_UTIL.getConfiguration()),
        new String [] {"-threads", "3", "-verify", "-noclosefs", "-iterations", "3000"});
    assertEquals(0, errCode);
  }

  /**
   * Just write multiple logs then split.  Before fix for HADOOP-2283, this
   * would fail.
   * @throws IOException
   */
  @Test
  public void testSplit() throws IOException {
    final TableName tableName =
        TableName.valueOf(getName());
    final byte [] rowName = tableName.getName();
    Path logdir = new Path(hbaseDir, HConstants.HREGION_LOGDIR_NAME);
    HLog log = HLogFactory.createHLog(fs, hbaseDir, HConstants.HREGION_LOGDIR_NAME, conf);
    final int howmany = 3;
    HRegionInfo[] infos = new HRegionInfo[3];
    Path tabledir = FSUtils.getTableDir(hbaseDir, tableName);
    fs.mkdirs(tabledir);
    for(int i = 0; i < howmany; i++) {
      infos[i] = new HRegionInfo(tableName,
                Bytes.toBytes("" + i), Bytes.toBytes("" + (i+1)), false);
      fs.mkdirs(new Path(tabledir, infos[i].getEncodedName()));
      LOG.info("allo " + new Path(tabledir, infos[i].getEncodedName()).toString());
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("column"));

    // Add edits for three regions.
    final AtomicLong sequenceId = new AtomicLong(1);
    try {
      for (int ii = 0; ii < howmany; ii++) {
        for (int i = 0; i < howmany; i++) {

          for (int j = 0; j < howmany; j++) {
            WALEdit edit = new WALEdit();
            byte [] family = Bytes.toBytes("column");
            byte [] qualifier = Bytes.toBytes(Integer.toString(j));
            byte [] column = Bytes.toBytes("column:" + Integer.toString(j));
            edit.add(new KeyValue(rowName, family, qualifier,
                System.currentTimeMillis(), column));
            LOG.info("Region " + i + ": " + edit);
            log.append(infos[i], tableName, edit,
              System.currentTimeMillis(), htd, sequenceId);
          }
        }
        log.rollWriter();
      }
      log.close();
      List<Path> splits = HLogSplitter.split(hbaseDir, logdir, oldLogDir, fs, conf);
      verifySplits(splits, howmany);
      log = null;
    } finally {
      if (log != null) {
        log.closeAndDelete();
      }
    }
  }

  /**
   * Test new HDFS-265 sync.
   * @throws Exception
   */
  @Test
  public void Broken_testSync() throws Exception {
    TableName tableName =
        TableName.valueOf(getName());
    // First verify that using streams all works.
    Path p = new Path(dir, getName() + ".fsdos");
    FSDataOutputStream out = fs.create(p);
    out.write(tableName.getName());
    Method syncMethod = null;
    try {
      syncMethod = out.getClass().getMethod("hflush", new Class<?> []{});
    } catch (NoSuchMethodException e) {
      try {
        syncMethod = out.getClass().getMethod("sync", new Class<?> []{});
      } catch (NoSuchMethodException ex) {
        fail("This version of Hadoop supports neither Syncable.sync() " +
            "nor Syncable.hflush().");
      }
    }
    syncMethod.invoke(out, new Object[]{});
    FSDataInputStream in = fs.open(p);
    assertTrue(in.available() > 0);
    byte [] buffer = new byte [1024];
    int read = in.read(buffer);
    assertEquals(tableName.getName().length, read);
    out.close();
    in.close();

    HLog wal = HLogFactory.createHLog(fs, dir, "hlogdir", conf);
    final AtomicLong sequenceId = new AtomicLong(1);
    final int total = 20;
    HLog.Reader reader = null;

    try {
      HRegionInfo info = new HRegionInfo(tableName,
                  null,null, false);
      HTableDescriptor htd = new HTableDescriptor();
      htd.addFamily(new HColumnDescriptor(tableName.getName()));

      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), tableName.getName()));
        wal.append(info, tableName, kvs, System.currentTimeMillis(), htd, sequenceId);
      }
      // Now call sync and try reading.  Opening a Reader before you sync just
      // gives you EOFE.
      wal.sync();
      // Open a Reader.
      Path walPath = ((FSHLog) wal).computeFilename();
      reader = HLogFactory.createReader(fs, walPath, conf);
      int count = 0;
      HLog.Entry entry = new HLog.Entry();
      while ((entry = reader.next(entry)) != null) count++;
      assertEquals(total, count);
      reader.close();
      // Add test that checks to see that an open of a Reader works on a file
      // that has had a sync done on it.
      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), tableName.getName()));
        wal.append(info, tableName, kvs, System.currentTimeMillis(), htd, sequenceId);
      }
      reader = HLogFactory.createReader(fs, walPath, conf);
      count = 0;
      while((entry = reader.next(entry)) != null) count++;
      assertTrue(count >= total);
      reader.close();
      // If I sync, should see double the edits.
      wal.sync();
      reader = HLogFactory.createReader(fs, walPath, conf);
      count = 0;
      while((entry = reader.next(entry)) != null) count++;
      assertEquals(total * 2, count);
      // Now do a test that ensures stuff works when we go over block boundary,
      // especially that we return good length on file.
      final byte [] value = new byte[1025 * 1024];  // Make a 1M value.
      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), value));
        wal.append(info, tableName, kvs, System.currentTimeMillis(), htd, sequenceId);
      }
      // Now I should have written out lots of blocks.  Sync then read.
      wal.sync();
      reader = HLogFactory.createReader(fs, walPath, conf);
      count = 0;
      while((entry = reader.next(entry)) != null) count++;
      assertEquals(total * 3, count);
      reader.close();
      // Close it and ensure that closed, Reader gets right length also.
      wal.close();
      reader = HLogFactory.createReader(fs, walPath, conf);
      count = 0;
      while((entry = reader.next(entry)) != null) count++;
      assertEquals(total * 3, count);
      reader.close();
    } finally {
      if (wal != null) wal.closeAndDelete();
      if (reader != null) reader.close();
    }
  }

  private void verifySplits(List<Path> splits, final int howmany)
  throws IOException {
    assertEquals(howmany * howmany, splits.size());
    for (int i = 0; i < splits.size(); i++) {
      LOG.info("Verifying=" + splits.get(i));
      HLog.Reader reader = HLogFactory.createReader(fs, splits.get(i), conf);
      try {
        int count = 0;
        String previousRegion = null;
        long seqno = -1;
        HLog.Entry entry = new HLog.Entry();
        while((entry = reader.next(entry)) != null) {
          HLogKey key = entry.getKey();
          String region = Bytes.toString(key.getEncodedRegionName());
          // Assert that all edits are for same region.
          if (previousRegion != null) {
            assertEquals(previousRegion, region);
          }
          LOG.info("oldseqno=" + seqno + ", newseqno=" + key.getLogSeqNum());
          assertTrue(seqno < key.getLogSeqNum());
          seqno = key.getLogSeqNum();
          previousRegion = region;
          count++;
        }
        assertEquals(howmany, count);
      } finally {
        reader.close();
      }
    }
  }

  /*
   * We pass different values to recoverFileLease() so that different code paths are covered
   *
   * For this test to pass, requires:
   * 1. HDFS-200 (append support)
   * 2. HDFS-988 (SafeMode should freeze file operations
   *              [FSNamesystem.nextGenerationStampForBlock])
   * 3. HDFS-142 (on restart, maintain pendingCreates)
   */
  @Test (timeout=300000)
  public void testAppendClose() throws Exception {
    TableName tableName =
        TableName.valueOf(getName());
    HRegionInfo regioninfo = new HRegionInfo(tableName,
             HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false);

    HLog wal = HLogFactory.createHLog(fs, dir, "hlogdir",
        "hlogdir_archive", conf);
    final AtomicLong sequenceId = new AtomicLong(1);
    final int total = 20;

    HTableDescriptor htd = new HTableDescriptor();
    htd.addFamily(new HColumnDescriptor(tableName.getName()));

    for (int i = 0; i < total; i++) {
      WALEdit kvs = new WALEdit();
      kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), tableName.getName()));
      wal.append(regioninfo, tableName, kvs, System.currentTimeMillis(), htd, sequenceId);
    }
    // Now call sync to send the data to HDFS datanodes
    wal.sync();
     int namenodePort = cluster.getNameNodePort();
    final Path walPath = ((FSHLog) wal).computeFilename();


    // Stop the cluster.  (ensure restart since we're sharing MiniDFSCluster)
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();
      dfs.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_ENTER);
      TEST_UTIL.shutdownMiniDFSCluster();
      try {
        // wal.writer.close() will throw an exception,
        // but still call this since it closes the LogSyncer thread first
        wal.close();
      } catch (IOException e) {
        LOG.info(e);
      }
      fs.close(); // closing FS last so DFSOutputStream can't call close
      LOG.info("STOPPED first instance of the cluster");
    } finally {
      // Restart the cluster
      while (cluster.isClusterUp()){
        LOG.error("Waiting for cluster to go down");
        Thread.sleep(1000);
      }
      assertFalse(cluster.isClusterUp());
      cluster = null;
      for (int i = 0; i < 100; i++) {
        try {
          cluster = TEST_UTIL.startMiniDFSClusterForTestHLog(namenodePort);
          break;
        } catch (BindException e) {
          LOG.info("Sleeping.  BindException bringing up new cluster");
          Threads.sleep(1000);
        }
      }
      cluster.waitActive();
      fs = cluster.getFileSystem();
      LOG.info("STARTED second instance.");
    }

    // set the lease period to be 1 second so that the
    // namenode triggers lease recovery upon append request
    Method setLeasePeriod = cluster.getClass()
      .getDeclaredMethod("setLeasePeriod", new Class[]{Long.TYPE, Long.TYPE});
    setLeasePeriod.setAccessible(true);
    setLeasePeriod.invoke(cluster, 1000L, 1000L);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      LOG.info(e);
    }

    // Now try recovering the log, like the HMaster would do
    final FileSystem recoveredFs = fs;
    final Configuration rlConf = conf;

    class RecoverLogThread extends Thread {
      public Exception exception = null;
      public void run() {
          try {
            FSUtils.getInstance(fs, rlConf)
              .recoverFileLease(recoveredFs, walPath, rlConf, null);
          } catch (IOException e) {
            exception = e;
          }
      }
    }

    RecoverLogThread t = new RecoverLogThread();
    t.start();
    // Timeout after 60 sec. Without correct patches, would be an infinite loop
    t.join(60 * 1000);
    if(t.isAlive()) {
      t.interrupt();
      throw new Exception("Timed out waiting for HLog.recoverLog()");
    }

    if (t.exception != null)
      throw t.exception;

    // Make sure you can read all the content
    HLog.Reader reader = HLogFactory.createReader(fs, walPath, conf);
    int count = 0;
    HLog.Entry entry = new HLog.Entry();
    while (reader.next(entry) != null) {
      count++;
      assertTrue("Should be one KeyValue per WALEdit",
                  entry.getEdit().getCells().size() == 1);
    }
    assertEquals(total, count);
    reader.close();

    // Reset the lease period
    setLeasePeriod.invoke(cluster, new Object[]{new Long(60000), new Long(3600000)});
  }

  /**
   * Tests that we can write out an edit, close, and then read it back in again.
   * @throws IOException
   */
  @Test
  public void testEditAdd() throws IOException {
    final int COL_COUNT = 10;
    final TableName tableName =
        TableName.valueOf("tablename");
    final byte [] row = Bytes.toBytes("row");
    HLog.Reader reader = null;
    HLog log = null;
    try {
      log = HLogFactory.createHLog(fs, hbaseDir, getName(), conf);
      final AtomicLong sequenceId = new AtomicLong(1);

      // Write columns named 1, 2, 3, etc. and then values of single byte
      // 1, 2, 3...
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      for (int i = 0; i < COL_COUNT; i++) {
        cols.add(new KeyValue(row, Bytes.toBytes("column"),
            Bytes.toBytes(Integer.toString(i)),
          timestamp, new byte[] { (byte)(i + '0') }));
      }
      HRegionInfo info = new HRegionInfo(tableName,
        row,Bytes.toBytes(Bytes.toString(row) + "1"), false);
      HTableDescriptor htd = new HTableDescriptor();
      htd.addFamily(new HColumnDescriptor("column"));

      log.append(info, tableName, cols, System.currentTimeMillis(), htd, sequenceId);
      log.startCacheFlush(info.getEncodedNameAsBytes());
      log.completeCacheFlush(info.getEncodedNameAsBytes());
      log.close();
      Path filename = ((FSHLog) log).computeFilename();
      log = null;
      // Now open a reader on the log and assert append worked.
      reader = HLogFactory.createReader(fs, filename, conf);
      // Above we added all columns on a single row so we only read one
      // entry in the below... thats why we have '1'.
      for (int i = 0; i < 1; i++) {
        HLog.Entry entry = reader.next(null);
        if (entry == null) break;
        HLogKey key = entry.getKey();
        WALEdit val = entry.getEdit();
        assertTrue(Bytes.equals(info.getEncodedNameAsBytes(), key.getEncodedRegionName()));
        assertTrue(tableName.equals(key.getTablename()));
        Cell cell = val.getCells().get(0);
        assertTrue(Bytes.equals(row, cell.getRow()));
        assertEquals((byte)(i + '0'), cell.getValue()[0]);
        System.out.println(key + " " + val);
      }
    } finally {
      if (log != null) {
        log.closeAndDelete();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * @throws IOException
   */
  @Test
  public void testAppend() throws IOException {
    final int COL_COUNT = 10;
    final TableName tableName =
        TableName.valueOf("tablename");
    final byte [] row = Bytes.toBytes("row");
    Reader reader = null;
    HLog log = HLogFactory.createHLog(fs, hbaseDir, getName(), conf);
    final AtomicLong sequenceId = new AtomicLong(1);
    try {
      // Write columns named 1, 2, 3, etc. and then values of single byte
      // 1, 2, 3...
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      for (int i = 0; i < COL_COUNT; i++) {
        cols.add(new KeyValue(row, Bytes.toBytes("column"),
          Bytes.toBytes(Integer.toString(i)),
          timestamp, new byte[] { (byte)(i + '0') }));
      }
      HRegionInfo hri = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      HTableDescriptor htd = new HTableDescriptor();
      htd.addFamily(new HColumnDescriptor("column"));
      log.append(hri, tableName, cols, System.currentTimeMillis(), htd, sequenceId);
      log.startCacheFlush(hri.getEncodedNameAsBytes());
      log.completeCacheFlush(hri.getEncodedNameAsBytes());
      log.close();
      Path filename = ((FSHLog) log).computeFilename();
      log = null;
      // Now open a reader on the log and assert append worked.
      reader = HLogFactory.createReader(fs, filename, conf);
      HLog.Entry entry = reader.next();
      assertEquals(COL_COUNT, entry.getEdit().size());
      int idx = 0;
      for (Cell val : entry.getEdit().getCells()) {
        assertTrue(Bytes.equals(hri.getEncodedNameAsBytes(),
          entry.getKey().getEncodedRegionName()));
        assertTrue(tableName.equals(entry.getKey().getTablename()));
        assertTrue(Bytes.equals(row, val.getRow()));
        assertEquals((byte)(idx + '0'), val.getValue()[0]);
        System.out.println(entry.getKey() + " " + val);
        idx++;
      }
    } finally {
      if (log != null) {
        log.closeAndDelete();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Test that we can visit entries before they are appended
   * @throws Exception
   */
  @Test
  public void testVisitors() throws Exception {
    final int COL_COUNT = 10;
    final TableName tableName =
        TableName.valueOf("tablename");
    final byte [] row = Bytes.toBytes("row");
    HLog log = HLogFactory.createHLog(fs, hbaseDir, getName(), conf);
    final AtomicLong sequenceId = new AtomicLong(1);
    try {
      DumbWALActionsListener visitor = new DumbWALActionsListener();
      log.registerWALActionsListener(visitor);
      long timestamp = System.currentTimeMillis();
      HTableDescriptor htd = new HTableDescriptor();
      htd.addFamily(new HColumnDescriptor("column"));

      HRegionInfo hri = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      for (int i = 0; i < COL_COUNT; i++) {
        WALEdit cols = new WALEdit();
        cols.add(new KeyValue(row, Bytes.toBytes("column"),
            Bytes.toBytes(Integer.toString(i)),
            timestamp, new byte[]{(byte) (i + '0')}));
        log.append(hri, tableName, cols, System.currentTimeMillis(), htd, sequenceId);
      }
      assertEquals(COL_COUNT, visitor.increments);
      log.unregisterWALActionsListener(visitor);
      WALEdit cols = new WALEdit();
      cols.add(new KeyValue(row, Bytes.toBytes("column"),
          Bytes.toBytes(Integer.toString(11)),
          timestamp, new byte[]{(byte) (11 + '0')}));
      log.append(hri, tableName, cols, System.currentTimeMillis(), htd, sequenceId);
      assertEquals(COL_COUNT, visitor.increments);
    } finally {
      if (log != null) log.closeAndDelete();
    }
  }

  @Test
  public void testLogCleaning() throws Exception {
    LOG.info("testLogCleaning");
    final TableName tableName =
        TableName.valueOf("testLogCleaning");
    final TableName tableName2 =
        TableName.valueOf("testLogCleaning2");

    HLog log = HLogFactory.createHLog(fs, hbaseDir,
        getName(), conf);
    final AtomicLong sequenceId = new AtomicLong(1);
    try {
      HRegionInfo hri = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      HRegionInfo hri2 = new HRegionInfo(tableName2,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);

      // Add a single edit and make sure that rolling won't remove the file
      // Before HBASE-3198 it used to delete it
      addEdits(log, hri, tableName, 1, sequenceId);
      log.rollWriter();
      assertEquals(1, ((FSHLog) log).getNumRolledLogFiles());

      // See if there's anything wrong with more than 1 edit
      addEdits(log, hri, tableName, 2, sequenceId);
      log.rollWriter();
      assertEquals(2, ((FSHLog) log).getNumRolledLogFiles());

      // Now mix edits from 2 regions, still no flushing
      addEdits(log, hri, tableName, 1, sequenceId);
      addEdits(log, hri2, tableName2, 1, sequenceId);
      addEdits(log, hri, tableName, 1, sequenceId);
      addEdits(log, hri2, tableName2, 1, sequenceId);
      log.rollWriter();
      assertEquals(3, ((FSHLog) log).getNumRolledLogFiles());

      // Flush the first region, we expect to see the first two files getting
      // archived. We need to append something or writer won't be rolled.
      addEdits(log, hri2, tableName2, 1, sequenceId);
      log.startCacheFlush(hri.getEncodedNameAsBytes());
      log.completeCacheFlush(hri.getEncodedNameAsBytes());
      log.rollWriter();
      assertEquals(2, ((FSHLog) log).getNumRolledLogFiles());

      // Flush the second region, which removes all the remaining output files
      // since the oldest was completely flushed and the two others only contain
      // flush information
      addEdits(log, hri2, tableName2, 1, sequenceId);
      log.startCacheFlush(hri2.getEncodedNameAsBytes());
      log.completeCacheFlush(hri2.getEncodedNameAsBytes());
      log.rollWriter();
      assertEquals(0, ((FSHLog) log).getNumRolledLogFiles());
    } finally {
      if (log != null) log.closeAndDelete();
    }
  }

  @Test(expected=IOException.class)
  public void testFailedToCreateHLogIfParentRenamed() throws IOException {
    FSHLog log = (FSHLog)HLogFactory.createHLog(
      fs, hbaseDir, "testFailedToCreateHLogIfParentRenamed", conf);
    long filenum = System.currentTimeMillis();
    Path path = log.computeFilename(filenum);
    HLogFactory.createWALWriter(fs, path, conf);
    Path parent = path.getParent();
    path = log.computeFilename(filenum + 1);
    Path newPath = new Path(parent.getParent(), parent.getName() + "-splitting");
    fs.rename(parent, newPath);
    HLogFactory.createWALWriter(fs, path, conf);
    fail("It should fail to create the new WAL");
  }

  @Test
  public void testGetServerNameFromHLogDirectoryName() throws IOException {
    ServerName sn = ServerName.valueOf("hn", 450, 1398);
    String hl = FSUtils.getRootDir(conf) + "/" + HLogUtil.getHLogDirectoryName(sn.toString());

    // Must not throw exception
    Assert.assertNull(HLogUtil.getServerNameFromHLogDirectoryName(conf, null));
    Assert.assertNull(HLogUtil.getServerNameFromHLogDirectoryName(conf,
        FSUtils.getRootDir(conf).toUri().toString()));
    Assert.assertNull(HLogUtil.getServerNameFromHLogDirectoryName(conf, ""));
    Assert.assertNull(HLogUtil.getServerNameFromHLogDirectoryName(conf, "                  "));
    Assert.assertNull(HLogUtil.getServerNameFromHLogDirectoryName(conf, hl));
    Assert.assertNull(HLogUtil.getServerNameFromHLogDirectoryName(conf, hl + "qdf"));
    Assert.assertNull(HLogUtil.getServerNameFromHLogDirectoryName(conf, "sfqf" + hl + "qdf"));

    final String wals = "/WALs/";
    ServerName parsed = HLogUtil.getServerNameFromHLogDirectoryName(conf,
      FSUtils.getRootDir(conf).toUri().toString() + wals + sn +
      "/localhost%2C32984%2C1343316388997.1343316390417");
    Assert.assertEquals("standard",  sn, parsed);

    parsed = HLogUtil.getServerNameFromHLogDirectoryName(conf, hl + "/qdf");
    Assert.assertEquals("subdir", sn, parsed);

    parsed = HLogUtil.getServerNameFromHLogDirectoryName(conf,
      FSUtils.getRootDir(conf).toUri().toString() + wals + sn +
      "-splitting/localhost%3A57020.1340474893931");
    Assert.assertEquals("split", sn, parsed);
  }

  /**
   * A loaded WAL coprocessor won't break existing HLog test cases.
   */
  @Test
  public void testWALCoprocessorLoaded() throws Exception {
    // test to see whether the coprocessor is loaded or not.
    HLog log = HLogFactory.createHLog(fs, hbaseDir,
        getName(), conf);
    try {
      WALCoprocessorHost host = log.getCoprocessorHost();
      Coprocessor c = host.findCoprocessor(SampleRegionWALObserver.class.getName());
      assertNotNull(c);
    } finally {
      if (log != null) log.closeAndDelete();
    }
  }

  private void addEdits(HLog log, HRegionInfo hri, TableName tableName,
                        int times, AtomicLong sequenceId) throws IOException {
    HTableDescriptor htd = new HTableDescriptor();
    htd.addFamily(new HColumnDescriptor("row"));

    final byte [] row = Bytes.toBytes("row");
    for (int i = 0; i < times; i++) {
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      cols.add(new KeyValue(row, row, row, timestamp, row));
      log.append(hri, tableName, cols, timestamp, htd, sequenceId);
    }
  }


  /**
   * @throws IOException
   */
  @Test
  public void testReadLegacyLog() throws IOException {
    final int columnCount = 5;
    final int recordCount = 5;
    final TableName tableName =
        TableName.valueOf("tablename");
    final byte[] row = Bytes.toBytes("row");
    long timestamp = System.currentTimeMillis();
    Path path = new Path(dir, "temphlog");
    SequenceFileLogWriter sflw = null;
    HLog.Reader reader = null;
    try {
      HRegionInfo hri = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      HTableDescriptor htd = new HTableDescriptor(tableName);
      fs.mkdirs(dir);
      // Write log in pre-PB format.
      sflw = new SequenceFileLogWriter();
      sflw.init(fs, path, conf, false);
      for (int i = 0; i < recordCount; ++i) {
        HLogKey key = new HLogKey(
            hri.getEncodedNameAsBytes(), tableName, i, timestamp, HConstants.DEFAULT_CLUSTER_ID);
        WALEdit edit = new WALEdit();
        for (int j = 0; j < columnCount; ++j) {
          if (i == 0) {
            htd.addFamily(new HColumnDescriptor("column" + j));
          }
          String value = i + "" + j;
          edit.add(new KeyValue(row, row, row, timestamp, Bytes.toBytes(value)));
        }
        sflw.append(new HLog.Entry(key, edit));
      }
      sflw.sync();
      sflw.close();

      // Now read the log using standard means.
      reader = HLogFactory.createReader(fs, path, conf);
      assertTrue(reader instanceof SequenceFileLogReader);
      for (int i = 0; i < recordCount; ++i) {
        HLog.Entry entry = reader.next();
        assertNotNull(entry);
        assertEquals(columnCount, entry.getEdit().size());
        assertArrayEquals(hri.getEncodedNameAsBytes(), entry.getKey().getEncodedRegionName());
        assertEquals(tableName, entry.getKey().getTablename());
        int idx = 0;
        for (Cell val : entry.getEdit().getCells()) {
          assertTrue(Bytes.equals(row, val.getRow()));
          String value = i + "" + idx;
          assertArrayEquals(Bytes.toBytes(value), val.getValue());
          idx++;
        }
      }
      HLog.Entry entry = reader.next();
      assertNull(entry);
    } finally {
      if (sflw != null) {
        sflw.close();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Reads the WAL with and without WALTrailer.
   * @throws IOException
   */
  @Test
  public void testWALTrailer() throws IOException {
    // read With trailer.
    doRead(true);
    // read without trailer
    doRead(false);
  }

  /**
   * Appends entries in the WAL and reads it.
   * @param withTrailer If 'withTrailer' is true, it calls a close on the WALwriter before reading
   *          so that a trailer is appended to the WAL. Otherwise, it starts reading after the sync
   *          call. This means that reader is not aware of the trailer. In this scenario, if the
   *          reader tries to read the trailer in its next() call, it returns false from
   *          ProtoBufLogReader.
   * @throws IOException
   */
  private void doRead(boolean withTrailer) throws IOException {
    final int columnCount = 5;
    final int recordCount = 5;
    final TableName tableName =
        TableName.valueOf("tablename");
    final byte[] row = Bytes.toBytes("row");
    long timestamp = System.currentTimeMillis();
    Path path = new Path(dir, "temphlog");
    // delete the log if already exists, for test only
    fs.delete(path, true);
    HLog.Writer writer = null;
    HLog.Reader reader = null;
    try {
      HRegionInfo hri = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      HTableDescriptor htd = new HTableDescriptor(tableName);
      fs.mkdirs(dir);
      // Write log in pb format.
      writer = HLogFactory.createWALWriter(fs, path, conf);
      for (int i = 0; i < recordCount; ++i) {
        HLogKey key = new HLogKey(
            hri.getEncodedNameAsBytes(), tableName, i, timestamp, HConstants.DEFAULT_CLUSTER_ID);
        WALEdit edit = new WALEdit();
        for (int j = 0; j < columnCount; ++j) {
          if (i == 0) {
            htd.addFamily(new HColumnDescriptor("column" + j));
          }
          String value = i + "" + j;
          edit.add(new KeyValue(row, row, row, timestamp, Bytes.toBytes(value)));
        }
        writer.append(new HLog.Entry(key, edit));
      }
      writer.sync();
      if (withTrailer) writer.close();

      // Now read the log using standard means.
      reader = HLogFactory.createReader(fs, path, conf);
      assertTrue(reader instanceof ProtobufLogReader);
      if (withTrailer) {
        assertNotNull(reader.getWALTrailer());
      } else {
        assertNull(reader.getWALTrailer());
      }
      for (int i = 0; i < recordCount; ++i) {
        HLog.Entry entry = reader.next();
        assertNotNull(entry);
        assertEquals(columnCount, entry.getEdit().size());
        assertArrayEquals(hri.getEncodedNameAsBytes(), entry.getKey().getEncodedRegionName());
        assertEquals(tableName, entry.getKey().getTablename());
        int idx = 0;
        for (Cell val : entry.getEdit().getCells()) {
          assertTrue(Bytes.equals(row, val.getRow()));
          String value = i + "" + idx;
          assertArrayEquals(Bytes.toBytes(value), val.getValue());
          idx++;
        }
      }
      HLog.Entry entry = reader.next();
      assertNull(entry);
    } finally {
      if (writer != null) {
        writer.close();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * tests the log comparator. Ensure that we are not mixing meta logs with non-meta logs (throws
   * exception if we do). Comparison is based on the timestamp present in the wal name.
   * @throws Exception
   */
  @Test
  public void testHLogComparator() throws Exception {
    HLog hlog1 = null;
    HLog hlogMeta = null;
    try {
      hlog1 = HLogFactory.createHLog(fs, FSUtils.getRootDir(conf), dir.toString(), conf);
      LOG.debug("Log obtained is: " + hlog1);
      Comparator<Path> comp = ((FSHLog) hlog1).LOG_NAME_COMPARATOR;
      Path p1 = ((FSHLog) hlog1).computeFilename(11);
      Path p2 = ((FSHLog) hlog1).computeFilename(12);
      // comparing with itself returns 0
      assertTrue(comp.compare(p1, p1) == 0);
      // comparing with different filenum.
      assertTrue(comp.compare(p1, p2) < 0);
      hlogMeta = HLogFactory.createMetaHLog(fs, FSUtils.getRootDir(conf), dir.toString(), conf,
        null, null);
      Comparator<Path> compMeta = ((FSHLog) hlogMeta).LOG_NAME_COMPARATOR;

      Path p1WithMeta = ((FSHLog) hlogMeta).computeFilename(11);
      Path p2WithMeta = ((FSHLog) hlogMeta).computeFilename(12);
      assertTrue(compMeta.compare(p1WithMeta, p1WithMeta) == 0);
      assertTrue(compMeta.compare(p1WithMeta, p2WithMeta) < 0);
      // mixing meta and non-meta logs gives error
      boolean ex = false;
      try {
        comp.compare(p1WithMeta, p2);
      } catch (Exception e) {
        ex = true;
      }
      assertTrue("Comparator doesn't complain while checking meta log files", ex);
      boolean exMeta = false;
      try {
        compMeta.compare(p1WithMeta, p2);
      } catch (Exception e) {
        exMeta = true;
      }
      assertTrue("Meta comparator doesn't complain while checking log files", exMeta);
    } finally {
      if (hlog1 != null) hlog1.close();
      if (hlogMeta != null) hlogMeta.close();
    }
  }

  /**
   * Tests wal archiving by adding data, doing flushing/rolling and checking we archive old logs
   * and also don't archive "live logs" (that is, a log with un-flushed entries).
   * <p>
   * This is what it does:
   * It creates two regions, and does a series of inserts along with log rolling.
   * Whenever a WAL is rolled, FSHLog checks previous wals for archiving. A wal is eligible for
   * archiving if for all the regions which have entries in that wal file, have flushed - past
   * their maximum sequence id in that wal file.
   * <p>
   * @throws IOException
   */
  @Test
  public void testWALArchiving() throws IOException {
    LOG.debug("testWALArchiving");
    TableName table1 = TableName.valueOf("t1");
    TableName table2 = TableName.valueOf("t2");
    HLog hlog = HLogFactory.createHLog(fs, FSUtils.getRootDir(conf), dir.toString(), conf);
    try {
      assertEquals(0, ((FSHLog) hlog).getNumRolledLogFiles());
      HRegionInfo hri1 = new HRegionInfo(table1, HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_END_ROW);
      HRegionInfo hri2 = new HRegionInfo(table2, HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_END_ROW);
      // ensure that we don't split the regions.
      hri1.setSplit(false);
      hri2.setSplit(false);
      // variables to mock region sequenceIds.
      final AtomicLong sequenceId1 = new AtomicLong(1);
      final AtomicLong sequenceId2 = new AtomicLong(1);
      // start with the testing logic: insert a waledit, and roll writer
      addEdits(hlog, hri1, table1, 1, sequenceId1);
      hlog.rollWriter();
      // assert that the wal is rolled
      assertEquals(1, ((FSHLog) hlog).getNumRolledLogFiles());
      // add edits in the second wal file, and roll writer.
      addEdits(hlog, hri1, table1, 1, sequenceId1);
      hlog.rollWriter();
      // assert that the wal is rolled
      assertEquals(2, ((FSHLog) hlog).getNumRolledLogFiles());
      // add a waledit to table1, and flush the region.
      addEdits(hlog, hri1, table1, 3, sequenceId1);
      flushRegion(hlog, hri1.getEncodedNameAsBytes());
      // roll log; all old logs should be archived.
      hlog.rollWriter();
      assertEquals(0, ((FSHLog) hlog).getNumRolledLogFiles());
      // add an edit to table2, and roll writer
      addEdits(hlog, hri2, table2, 1, sequenceId2);
      hlog.rollWriter();
      assertEquals(1, ((FSHLog) hlog).getNumRolledLogFiles());
      // add edits for table1, and roll writer
      addEdits(hlog, hri1, table1, 2, sequenceId1);
      hlog.rollWriter();
      assertEquals(2, ((FSHLog) hlog).getNumRolledLogFiles());
      // add edits for table2, and flush hri1.
      addEdits(hlog, hri2, table2, 2, sequenceId2);
      flushRegion(hlog, hri1.getEncodedNameAsBytes());
      // the log : region-sequenceId map is
      // log1: region2 (unflushed)
      // log2: region1 (flushed)
      // log3: region2 (unflushed)
      // roll the writer; log2 should be archived.
      hlog.rollWriter();
      assertEquals(2, ((FSHLog) hlog).getNumRolledLogFiles());
      // flush region2, and all logs should be archived.
      addEdits(hlog, hri2, table2, 2, sequenceId2);
      flushRegion(hlog, hri2.getEncodedNameAsBytes());
      hlog.rollWriter();
      assertEquals(0, ((FSHLog) hlog).getNumRolledLogFiles());
    } finally {
      if (hlog != null) hlog.close();
    }
  }

  /**
   * On rolling a wal after reaching the threshold, {@link HLog#rollWriter()} returns the list of
   * regions which should be flushed in order to archive the oldest wal file.
   * <p>
   * This method tests this behavior by inserting edits and rolling the wal enough times to reach
   * the max number of logs threshold. It checks whether we get the "right regions" for flush on
   * rolling the wal.
   * @throws Exception
   */
  @Test
  public void testFindMemStoresEligibleForFlush() throws Exception {
    LOG.debug("testFindMemStoresEligibleForFlush");
    Configuration conf1 = HBaseConfiguration.create(conf);
    conf1.setInt("hbase.regionserver.maxlogs", 1);
    HLog hlog = HLogFactory.createHLog(fs, FSUtils.getRootDir(conf1), dir.toString(), conf1);
    TableName t1 = TableName.valueOf("t1");
    TableName t2 = TableName.valueOf("t2");
    HRegionInfo hri1 = new HRegionInfo(t1, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    HRegionInfo hri2 = new HRegionInfo(t2, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    // variables to mock region sequenceIds
    final AtomicLong sequenceId1 = new AtomicLong(1);
    final AtomicLong sequenceId2 = new AtomicLong(1);
    // add edits and roll the wal
    try {
      addEdits(hlog, hri1, t1, 2, sequenceId1);
      hlog.rollWriter();
      // add some more edits and roll the wal. This would reach the log number threshold
      addEdits(hlog, hri1, t1, 2, sequenceId1);
      hlog.rollWriter();
      // with above rollWriter call, the max logs limit is reached.
      assertTrue(((FSHLog) hlog).getNumRolledLogFiles() == 2);

      // get the regions to flush; since there is only one region in the oldest wal, it should
      // return only one region.
      byte[][] regionsToFlush = ((FSHLog) hlog).findRegionsToForceFlush();
      assertEquals(1, regionsToFlush.length);
      assertEquals(hri1.getEncodedNameAsBytes(), regionsToFlush[0]);
      // insert edits in second region
      addEdits(hlog, hri2, t2, 2, sequenceId2);
      // get the regions to flush, it should still read region1.
      regionsToFlush = ((FSHLog) hlog).findRegionsToForceFlush();
      assertEquals(regionsToFlush.length, 1);
      assertEquals(hri1.getEncodedNameAsBytes(), regionsToFlush[0]);
      // flush region 1, and roll the wal file. Only last wal which has entries for region1 should
      // remain.
      flushRegion(hlog, hri1.getEncodedNameAsBytes());
      hlog.rollWriter();
      // only one wal should remain now (that is for the second region).
      assertEquals(1, ((FSHLog) hlog).getNumRolledLogFiles());
      // flush the second region
      flushRegion(hlog, hri2.getEncodedNameAsBytes());
      hlog.rollWriter(true);
      // no wal should remain now.
      assertEquals(0, ((FSHLog) hlog).getNumRolledLogFiles());
      // add edits both to region 1 and region 2, and roll.
      addEdits(hlog, hri1, t1, 2, sequenceId1);
      addEdits(hlog, hri2, t2, 2, sequenceId2);
      hlog.rollWriter();
      // add edits and roll the writer, to reach the max logs limit.
      assertEquals(1, ((FSHLog) hlog).getNumRolledLogFiles());
      addEdits(hlog, hri1, t1, 2, sequenceId1);
      hlog.rollWriter();
      // it should return two regions to flush, as the oldest wal file has entries
      // for both regions.
      regionsToFlush = ((FSHLog) hlog).findRegionsToForceFlush();
      assertEquals(2, regionsToFlush.length);
      // flush both regions
      flushRegion(hlog, hri1.getEncodedNameAsBytes());
      flushRegion(hlog, hri2.getEncodedNameAsBytes());
      hlog.rollWriter(true);
      assertEquals(0, ((FSHLog) hlog).getNumRolledLogFiles());
      // Add an edit to region1, and roll the wal.
      addEdits(hlog, hri1, t1, 2, sequenceId1);
      // tests partial flush: roll on a partial flush, and ensure that wal is not archived.
      hlog.startCacheFlush(hri1.getEncodedNameAsBytes());
      hlog.rollWriter();
      hlog.completeCacheFlush(hri1.getEncodedNameAsBytes());
      assertEquals(1, ((FSHLog) hlog).getNumRolledLogFiles());
    } finally {
      if (hlog != null) hlog.close();
    }
  }

  /**
   * Simulates HLog append ops for a region and tests
   * {@link FSHLog#areAllRegionsFlushed(Map, Map, Map)} API.
   * It compares the region sequenceIds with oldestFlushing and oldestUnFlushed entries.
   * If a region's entries are larger than min of (oldestFlushing, oldestUnFlushed), then the
   * region should be flushed before archiving this WAL.
  */
  @Test
  public void testAllRegionsFlushed() {
    LOG.debug("testAllRegionsFlushed");
    Map<byte[], Long> oldestFlushingSeqNo = new HashMap<byte[], Long>();
    Map<byte[], Long> oldestUnFlushedSeqNo = new HashMap<byte[], Long>();
    Map<byte[], Long> seqNo = new HashMap<byte[], Long>();
    // create a table
    TableName t1 = TableName.valueOf("t1");
    // create a region
    HRegionInfo hri1 = new HRegionInfo(t1, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    // variables to mock region sequenceIds
    final AtomicLong sequenceId1 = new AtomicLong(1);
    // test empty map
    assertTrue(FSHLog.areAllRegionsFlushed(seqNo, oldestFlushingSeqNo, oldestUnFlushedSeqNo));
    // add entries in the region
    seqNo.put(hri1.getEncodedNameAsBytes(), sequenceId1.incrementAndGet());
    oldestUnFlushedSeqNo.put(hri1.getEncodedNameAsBytes(), sequenceId1.get());
    // should say region1 is not flushed.
    assertFalse(FSHLog.areAllRegionsFlushed(seqNo, oldestFlushingSeqNo, oldestUnFlushedSeqNo));
    // test with entries in oldestFlushing map.
    oldestUnFlushedSeqNo.clear();
    oldestFlushingSeqNo.put(hri1.getEncodedNameAsBytes(), sequenceId1.get());
    assertFalse(FSHLog.areAllRegionsFlushed(seqNo, oldestFlushingSeqNo, oldestUnFlushedSeqNo));
    // simulate region flush, i.e., clear oldestFlushing and oldestUnflushed maps
    oldestFlushingSeqNo.clear();
    oldestUnFlushedSeqNo.clear();
    assertTrue(FSHLog.areAllRegionsFlushed(seqNo, oldestFlushingSeqNo, oldestUnFlushedSeqNo));
    // insert some large values for region1
    oldestUnFlushedSeqNo.put(hri1.getEncodedNameAsBytes(), 1000l);
    seqNo.put(hri1.getEncodedNameAsBytes(), 1500l);
    assertFalse(FSHLog.areAllRegionsFlushed(seqNo, oldestFlushingSeqNo, oldestUnFlushedSeqNo));

    // tests when oldestUnFlushed/oldestFlushing contains larger value.
    // It means region is flushed.
    oldestFlushingSeqNo.put(hri1.getEncodedNameAsBytes(), 1200l);
    oldestUnFlushedSeqNo.clear();
    seqNo.put(hri1.getEncodedNameAsBytes(), 1199l);
    assertTrue(FSHLog.areAllRegionsFlushed(seqNo, oldestFlushingSeqNo, oldestUnFlushedSeqNo));
  }

  /**
   * helper method to simulate region flush for a WAL.
   * @param hlog
   * @param regionEncodedName
   */
  private void flushRegion(HLog hlog, byte[] regionEncodedName) {
    hlog.startCacheFlush(regionEncodedName);
    hlog.completeCacheFlush(regionEncodedName);
  }

  static class DumbWALActionsListener implements WALActionsListener {
    int increments = 0;

    @Override
    public void visitLogEntryBeforeWrite(HRegionInfo info, HLogKey logKey,
                                         WALEdit logEdit) {
      increments++;
    }

    @Override
    public void visitLogEntryBeforeWrite(HTableDescriptor htd, HLogKey logKey, WALEdit logEdit) {
      //To change body of implemented methods use File | Settings | File Templates.
      increments++;
    }

    @Override
    public void preLogRoll(Path oldFile, Path newFile) {
      // TODO Auto-generated method stub
    }

    @Override
    public void postLogRoll(Path oldFile, Path newFile) {
      // TODO Auto-generated method stub
    }

    @Override
    public void preLogArchive(Path oldFile, Path newFile) {
      // TODO Auto-generated method stub
    }

    @Override
    public void postLogArchive(Path oldFile, Path newFile) {
      // TODO Auto-generated method stub
    }

    @Override
    public void logRollRequested() {
      // TODO Auto-generated method stub

    }

    @Override
    public void logCloseRequested() {
      // not interested
    }
  }

}

