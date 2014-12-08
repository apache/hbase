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
package org.apache.hadoop.hbase.wal;

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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

// imports for things that haven't moved from regionserver.wal yet.
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogReader;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 * WAL tests that can be reused across providers.
 */
@Category(MediumTests.class)
public class TestWALFactory {
  protected static final Log LOG = LogFactory.getLog(TestWALFactory.class);

  protected static Configuration conf;
  private static MiniDFSCluster cluster;
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static Path hbaseDir;

  protected FileSystem fs;
  protected Path dir;
  protected WALFactory wals;

  @Rule
  public final TestName currentTest = new TestName();

  @Before
  public void setUp() throws Exception {
    fs = cluster.getFileSystem();
    dir = new Path(hbaseDir, currentTest.getMethodName());
    wals = new WALFactory(conf, null, currentTest.getMethodName());
  }

  @After
  public void tearDown() throws Exception {
    // testAppendClose closes the FileSystem, which will prevent us from closing cleanly here.
    try {
      wals.close();
    } catch (IOException exception) {
      LOG.warn("Encountered exception while closing wal factory. If you have other errors, this" +
          " may be the cause. Message: " + exception);
      LOG.debug("Exception details for failure to close wal factory.", exception);
    }
    FileStatus[] entries = fs.listStatus(new Path("/"));
    for (FileStatus dir : entries) {
      fs.delete(dir.getPath(), true);
    }
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

    hbaseDir = TEST_UTIL.createRootDir();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void canCloseSingleton() throws IOException {
    WALFactory.getInstance(conf).close();
  }

  /**
   * Just write multiple logs then split.  Before fix for HADOOP-2283, this
   * would fail.
   * @throws IOException
   */
  @Test
  public void testSplit() throws IOException {
    final TableName tableName = TableName.valueOf(currentTest.getMethodName());
    final byte [] rowName = tableName.getName();
    final Path logdir = new Path(hbaseDir,
        DefaultWALProvider.getWALDirectoryName(currentTest.getMethodName()));
    Path oldLogDir = new Path(hbaseDir, HConstants.HREGION_OLDLOGDIR_NAME);
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
    for (int ii = 0; ii < howmany; ii++) {
      for (int i = 0; i < howmany; i++) {
        final WAL log = wals.getWAL(infos[i].getEncodedNameAsBytes());
        for (int j = 0; j < howmany; j++) {
          WALEdit edit = new WALEdit();
          byte [] family = Bytes.toBytes("column");
          byte [] qualifier = Bytes.toBytes(Integer.toString(j));
          byte [] column = Bytes.toBytes("column:" + Integer.toString(j));
          edit.add(new KeyValue(rowName, family, qualifier,
              System.currentTimeMillis(), column));
          LOG.info("Region " + i + ": " + edit);
          log.append(htd, infos[i], new WALKey(infos[i].getEncodedNameAsBytes(), tableName,
              System.currentTimeMillis()), edit, sequenceId, true, null);
        }
        log.sync();
        log.rollWriter();
      }
    }
    wals.shutdown();
    List<Path> splits = WALSplitter.split(hbaseDir, logdir, oldLogDir, fs, conf, wals);
    verifySplits(splits, howmany);
  }

  /**
   * Test new HDFS-265 sync.
   * @throws Exception
   */
  @Test
  public void Broken_testSync() throws Exception {
    TableName tableName = TableName.valueOf(currentTest.getMethodName());
    // First verify that using streams all works.
    Path p = new Path(dir, currentTest.getMethodName() + ".fsdos");
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

    final AtomicLong sequenceId = new AtomicLong(1);
    final int total = 20;
    WAL.Reader reader = null;

    try {
      HRegionInfo info = new HRegionInfo(tableName,
                  null,null, false);
      HTableDescriptor htd = new HTableDescriptor();
      htd.addFamily(new HColumnDescriptor(tableName.getName()));
      final WAL wal = wals.getWAL(info.getEncodedNameAsBytes());

      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), tableName.getName()));
        wal.append(htd, info, new WALKey(info.getEncodedNameAsBytes(), tableName,
            System.currentTimeMillis()), kvs, sequenceId, true, null);
      }
      // Now call sync and try reading.  Opening a Reader before you sync just
      // gives you EOFE.
      wal.sync();
      // Open a Reader.
      Path walPath = DefaultWALProvider.getCurrentFileName(wal);
      reader = wals.createReader(fs, walPath);
      int count = 0;
      WAL.Entry entry = new WAL.Entry();
      while ((entry = reader.next(entry)) != null) count++;
      assertEquals(total, count);
      reader.close();
      // Add test that checks to see that an open of a Reader works on a file
      // that has had a sync done on it.
      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), tableName.getName()));
        wal.append(htd, info, new WALKey(info.getEncodedNameAsBytes(), tableName,
            System.currentTimeMillis()), kvs, sequenceId, true, null);
      }
      wal.sync();
      reader = wals.createReader(fs, walPath);
      count = 0;
      while((entry = reader.next(entry)) != null) count++;
      assertTrue(count >= total);
      reader.close();
      // If I sync, should see double the edits.
      wal.sync();
      reader = wals.createReader(fs, walPath);
      count = 0;
      while((entry = reader.next(entry)) != null) count++;
      assertEquals(total * 2, count);
      reader.close();
      // Now do a test that ensures stuff works when we go over block boundary,
      // especially that we return good length on file.
      final byte [] value = new byte[1025 * 1024];  // Make a 1M value.
      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), value));
        wal.append(htd, info, new WALKey(info.getEncodedNameAsBytes(), tableName,
            System.currentTimeMillis()), kvs, sequenceId, true, null);
      }
      // Now I should have written out lots of blocks.  Sync then read.
      wal.sync();
      reader = wals.createReader(fs, walPath);
      count = 0;
      while((entry = reader.next(entry)) != null) count++;
      assertEquals(total * 3, count);
      reader.close();
      // shutdown and ensure that Reader gets right length also.
      wal.shutdown();
      reader = wals.createReader(fs, walPath);
      count = 0;
      while((entry = reader.next(entry)) != null) count++;
      assertEquals(total * 3, count);
      reader.close();
    } finally {
      if (reader != null) reader.close();
    }
  }

  private void verifySplits(final List<Path> splits, final int howmany)
  throws IOException {
    assertEquals(howmany * howmany, splits.size());
    for (int i = 0; i < splits.size(); i++) {
      LOG.info("Verifying=" + splits.get(i));
      WAL.Reader reader = wals.createReader(fs, splits.get(i));
      try {
        int count = 0;
        String previousRegion = null;
        long seqno = -1;
        WAL.Entry entry = new WAL.Entry();
        while((entry = reader.next(entry)) != null) {
          WALKey key = entry.getKey();
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
        TableName.valueOf(currentTest.getMethodName());
    HRegionInfo regioninfo = new HRegionInfo(tableName,
             HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false);

    final WAL wal = wals.getWAL(regioninfo.getEncodedNameAsBytes());
    final AtomicLong sequenceId = new AtomicLong(1);
    final int total = 20;

    HTableDescriptor htd = new HTableDescriptor();
    htd.addFamily(new HColumnDescriptor(tableName.getName()));

    for (int i = 0; i < total; i++) {
      WALEdit kvs = new WALEdit();
      kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), tableName.getName()));
      wal.append(htd, regioninfo, new WALKey(regioninfo.getEncodedNameAsBytes(), tableName,
          System.currentTimeMillis()), kvs, sequenceId, true, null);
    }
    // Now call sync to send the data to HDFS datanodes
    wal.sync();
     int namenodePort = cluster.getNameNodePort();
    final Path walPath = DefaultWALProvider.getCurrentFileName(wal);


    // Stop the cluster.  (ensure restart since we're sharing MiniDFSCluster)
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();
      dfs.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_ENTER);
      TEST_UTIL.shutdownMiniDFSCluster();
      try {
        // wal.writer.close() will throw an exception,
        // but still call this since it closes the LogSyncer thread first
        wal.shutdown();
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
          cluster = TEST_UTIL.startMiniDFSClusterForTestWAL(namenodePort);
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
      throw new Exception("Timed out waiting for WAL.recoverLog()");
    }

    if (t.exception != null)
      throw t.exception;

    // Make sure you can read all the content
    WAL.Reader reader = wals.createReader(fs, walPath);
    int count = 0;
    WAL.Entry entry = new WAL.Entry();
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
    WAL.Reader reader = null;
    try {
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
      final WAL log = wals.getWAL(info.getEncodedNameAsBytes());

      final long txid = log.append(htd, info, new WALKey(info.getEncodedNameAsBytes(), tableName,
          System.currentTimeMillis()), cols, sequenceId, true, null);
      log.sync(txid);
      log.startCacheFlush(info.getEncodedNameAsBytes());
      log.completeCacheFlush(info.getEncodedNameAsBytes());
      log.shutdown();
      Path filename = DefaultWALProvider.getCurrentFileName(log);
      // Now open a reader on the log and assert append worked.
      reader = wals.createReader(fs, filename);
      // Above we added all columns on a single row so we only read one
      // entry in the below... thats why we have '1'.
      for (int i = 0; i < 1; i++) {
        WAL.Entry entry = reader.next(null);
        if (entry == null) break;
        WALKey key = entry.getKey();
        WALEdit val = entry.getEdit();
        assertTrue(Bytes.equals(info.getEncodedNameAsBytes(), key.getEncodedRegionName()));
        assertTrue(tableName.equals(key.getTablename()));
        Cell cell = val.getCells().get(0);
        assertTrue(Bytes.equals(row, cell.getRow()));
        assertEquals((byte)(i + '0'), cell.getValue()[0]);
        System.out.println(key + " " + val);
      }
    } finally {
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
    WAL.Reader reader = null;
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
      final WAL log = wals.getWAL(hri.getEncodedNameAsBytes());
      final long txid = log.append(htd, hri, new WALKey(hri.getEncodedNameAsBytes(), tableName,
          System.currentTimeMillis()), cols, sequenceId, true, null);
      log.sync(txid);
      log.startCacheFlush(hri.getEncodedNameAsBytes());
      log.completeCacheFlush(hri.getEncodedNameAsBytes());
      log.shutdown();
      Path filename = DefaultWALProvider.getCurrentFileName(log);
      // Now open a reader on the log and assert append worked.
      reader = wals.createReader(fs, filename);
      WAL.Entry entry = reader.next();
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
    final DumbWALActionsListener visitor = new DumbWALActionsListener();
    final AtomicLong sequenceId = new AtomicLong(1);
    long timestamp = System.currentTimeMillis();
    HTableDescriptor htd = new HTableDescriptor();
    htd.addFamily(new HColumnDescriptor("column"));

    HRegionInfo hri = new HRegionInfo(tableName,
        HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    final WAL log = wals.getWAL(hri.getEncodedNameAsBytes());
    log.registerWALActionsListener(visitor);
    for (int i = 0; i < COL_COUNT; i++) {
      WALEdit cols = new WALEdit();
      cols.add(new KeyValue(row, Bytes.toBytes("column"),
          Bytes.toBytes(Integer.toString(i)),
          timestamp, new byte[]{(byte) (i + '0')}));
      log.append(htd, hri, new WALKey(hri.getEncodedNameAsBytes(), tableName,
          System.currentTimeMillis()), cols, sequenceId, true, null);
    }
    log.sync();
    assertEquals(COL_COUNT, visitor.increments);
    log.unregisterWALActionsListener(visitor);
    WALEdit cols = new WALEdit();
    cols.add(new KeyValue(row, Bytes.toBytes("column"),
        Bytes.toBytes(Integer.toString(11)),
        timestamp, new byte[]{(byte) (11 + '0')}));
    log.append(htd, hri, new WALKey(hri.getEncodedNameAsBytes(), tableName,
        System.currentTimeMillis()), cols, sequenceId, true, null);
    log.sync();
    assertEquals(COL_COUNT, visitor.increments);
  }

  /**
   * A loaded WAL coprocessor won't break existing WAL test cases.
   */
  @Test
  public void testWALCoprocessorLoaded() throws Exception {
    // test to see whether the coprocessor is loaded or not.
    WALCoprocessorHost host = wals.getWAL(UNSPECIFIED_REGION).getCoprocessorHost();
    Coprocessor c = host.findCoprocessor(SampleRegionWALObserver.class.getName());
    assertNotNull(c);
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
    Path path = new Path(dir, "tempwal");
    SequenceFileLogWriter sflw = null;
    WAL.Reader reader = null;
    try {
      HRegionInfo hri = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      HTableDescriptor htd = new HTableDescriptor(tableName);
      fs.mkdirs(dir);
      // Write log in pre-PB format.
      sflw = new SequenceFileLogWriter();
      sflw.init(fs, path, conf, false);
      for (int i = 0; i < recordCount; ++i) {
        WALKey key = new HLogKey(
            hri.getEncodedNameAsBytes(), tableName, i, timestamp, HConstants.DEFAULT_CLUSTER_ID);
        WALEdit edit = new WALEdit();
        for (int j = 0; j < columnCount; ++j) {
          if (i == 0) {
            htd.addFamily(new HColumnDescriptor("column" + j));
          }
          String value = i + "" + j;
          edit.add(new KeyValue(row, row, row, timestamp, Bytes.toBytes(value)));
        }
        sflw.append(new WAL.Entry(key, edit));
      }
      sflw.sync();
      sflw.close();

      // Now read the log using standard means.
      reader = wals.createReader(fs, path);
      assertTrue(reader instanceof SequenceFileLogReader);
      for (int i = 0; i < recordCount; ++i) {
        WAL.Entry entry = reader.next();
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
      WAL.Entry entry = reader.next();
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

  static class DumbWALActionsListener extends WALActionsListener.Base {
    int increments = 0;

    @Override
    public void visitLogEntryBeforeWrite(HRegionInfo info, WALKey logKey,
                                         WALEdit logEdit) {
      increments++;
    }

    @Override
    public void visitLogEntryBeforeWrite(HTableDescriptor htd, WALKey logKey, WALEdit logEdit) {
      //To change body of implemented methods use File | Settings | File Templates.
      increments++;
    }
  }

  private static final byte[] UNSPECIFIED_REGION = new byte[]{};

}
