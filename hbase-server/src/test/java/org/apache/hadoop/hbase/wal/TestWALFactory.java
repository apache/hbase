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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALObserver;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * WAL tests that can be reused across providers.
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestWALFactory {
  private static final Log LOG = LogFactory.getLog(TestWALFactory.class);

  protected static Configuration conf;
  private static MiniDFSCluster cluster;
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static Path hbaseDir;
  protected static Path hbaseWALDir;

  protected FileSystem fs;
  protected Path dir;
  protected WALFactory wals;
  private ServerName currentServername;

  @Rule
  public final TestName currentTest = new TestName();

  @Before
  public void setUp() throws Exception {
    fs = cluster.getFileSystem();
    dir = new Path(hbaseDir, currentTest.getMethodName());
    this.currentServername = ServerName.valueOf(currentTest.getMethodName(), 16010, 1);
    wals = new WALFactory(conf, null, this.currentServername.toString());
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
    TEST_UTIL.getConfiguration().setInt("hbase.lease.recovery.timeout", 10000);
    TEST_UTIL.getConfiguration().setInt("hbase.lease.recovery.dfs.timeout", 1000);
    TEST_UTIL.getConfiguration().set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
        SampleRegionWALObserver.class.getName());
    TEST_UTIL.startMiniDFSCluster(3);

    conf = TEST_UTIL.getConfiguration();
    cluster = TEST_UTIL.getDFSCluster();

    hbaseDir = TEST_UTIL.createRootDir();
    hbaseWALDir = TEST_UTIL.createWALRootDir();
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
    final MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl(1);
    final int howmany = 3;
    HRegionInfo[] infos = new HRegionInfo[3];
    Path tabledir = FSUtils.getTableDir(hbaseWALDir, tableName);
    fs.mkdirs(tabledir);
    for(int i = 0; i < howmany; i++) {
      infos[i] = new HRegionInfo(tableName,
                Bytes.toBytes("" + i), Bytes.toBytes("" + (i+1)), false);
      fs.mkdirs(new Path(tabledir, infos[i].getEncodedName()));
      LOG.info("allo " + new Path(tabledir, infos[i].getEncodedName()).toString());
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("column"));
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for(byte[] fam : htd.getFamiliesKeys()) {
      scopes.put(fam, 0);
    }

    // Add edits for three regions.
    for (int ii = 0; ii < howmany; ii++) {
      for (int i = 0; i < howmany; i++) {
        final WAL log =
            wals.getWAL(infos[i].getEncodedNameAsBytes(), infos[i].getTable().getNamespace());
        for (int j = 0; j < howmany; j++) {
          WALEdit edit = new WALEdit();
          byte [] family = Bytes.toBytes("column");
          byte [] qualifier = Bytes.toBytes(Integer.toString(j));
          byte [] column = Bytes.toBytes("column:" + Integer.toString(j));
          edit.add(new KeyValue(rowName, family, qualifier,
              System.currentTimeMillis(), column));
          LOG.info("Region " + i + ": " + edit);
          WALKey walKey =  new WALKey(infos[i].getEncodedNameAsBytes(), tableName,
              System.currentTimeMillis(), mvcc, scopes);
          log.append(infos[i], walKey, edit, true);
          walKey.getWriteEntry();
        }
        log.sync();
        log.rollWriter(true);
      }
    }
    wals.shutdown();
    // The below calculation of logDir relies on insider information... WALSplitter should be connected better
    // with the WAL system.... not requiring explicit path. The oldLogDir is just made up not used.
    Path logDir =
        new Path(new Path(hbaseWALDir, HConstants.HREGION_LOGDIR_NAME),
            this.currentServername.toString());
    Path oldLogDir = new Path(hbaseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    List<Path> splits = WALSplitter.split(hbaseWALDir, logDir, oldLogDir, fs, conf, wals);
    verifySplits(splits, howmany);
  }

  /**
   * Test new HDFS-265 sync.
   * @throws Exception
   */
  @Test
  public void Broken_testSync() throws Exception {
    TableName tableName = TableName.valueOf(currentTest.getMethodName());
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl(1);
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

    final int total = 20;
    WAL.Reader reader = null;

    try {
      HRegionInfo info = new HRegionInfo(tableName,
                  null,null, false);
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor(tableName.getName()));
      NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for(byte[] fam : htd.getFamiliesKeys()) {
        scopes.put(fam, 0);
      }
      final WAL wal = wals.getWAL(info.getEncodedNameAsBytes(), info.getTable().getNamespace());

      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), tableName.getName()));
        wal.append(info, new WALKey(info.getEncodedNameAsBytes(), tableName,
            System.currentTimeMillis(), mvcc, scopes), kvs, true);
      }
      // Now call sync and try reading.  Opening a Reader before you sync just
      // gives you EOFE.
      wal.sync();
      // Open a Reader.
      Path walPath = AbstractFSWALProvider.getCurrentFileName(wal);
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
        wal.append(info, new WALKey(info.getEncodedNameAsBytes(), tableName,
            System.currentTimeMillis(), mvcc, scopes), kvs, true);
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
        wal.append(info, new WALKey(info.getEncodedNameAsBytes(), tableName,
            System.currentTimeMillis(), mvcc, scopes), kvs,  true);
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
          LOG.info("oldseqno=" + seqno + ", newseqno=" + key.getSequenceId());
          assertTrue(seqno < key.getSequenceId());
          seqno = key.getSequenceId();
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

    final WAL wal =
        wals.getWAL(regioninfo.getEncodedNameAsBytes(), regioninfo.getTable().getNamespace());
    final int total = 20;

    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(tableName.getName()));
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for(byte[] fam : htd.getFamiliesKeys()) {
      scopes.put(fam, 0);
    }
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    for (int i = 0; i < total; i++) {
      WALEdit kvs = new WALEdit();
      kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), tableName.getName()));
      wal.append(regioninfo, new WALKey(regioninfo.getEncodedNameAsBytes(), tableName,
          System.currentTimeMillis(), mvcc, scopes),
        kvs, true);
    }
    // Now call sync to send the data to HDFS datanodes
    wal.sync();
     int namenodePort = cluster.getNameNodePort();
    final Path walPath = AbstractFSWALProvider.getCurrentFileName(wal);


    // Stop the cluster.  (ensure restart since we're sharing MiniDFSCluster)
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();
      dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
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
    final HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf(currentTest.getMethodName())).addFamily(new HColumnDescriptor(
            "column"));
    NavigableMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>(
        Bytes.BYTES_COMPARATOR);
    for(byte[] fam : htd.getFamiliesKeys()) {
      scopes.put(fam, 0);
    }
    final byte [] row = Bytes.toBytes("row");
    WAL.Reader reader = null;
    try {
      final MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl(1);

      // Write columns named 1, 2, 3, etc. and then values of single byte
      // 1, 2, 3...
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      for (int i = 0; i < COL_COUNT; i++) {
        cols.add(new KeyValue(row, Bytes.toBytes("column"),
            Bytes.toBytes(Integer.toString(i)),
          timestamp, new byte[] { (byte)(i + '0') }));
      }
      HRegionInfo info = new HRegionInfo(htd.getTableName(),
        row,Bytes.toBytes(Bytes.toString(row) + "1"), false);
      final WAL log = wals.getWAL(info.getEncodedNameAsBytes(), info.getTable().getNamespace());

      final long txid = log.append(info,
        new WALKey(info.getEncodedNameAsBytes(), htd.getTableName(), System.currentTimeMillis(),
            mvcc, scopes),
        cols, true);
      log.sync(txid);
      log.startCacheFlush(info.getEncodedNameAsBytes(), htd.getFamiliesKeys());
      log.completeCacheFlush(info.getEncodedNameAsBytes());
      log.shutdown();
      Path filename = AbstractFSWALProvider.getCurrentFileName(log);
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
        assertTrue(htd.getTableName().equals(key.getTablename()));
        Cell cell = val.getCells().get(0);
        assertTrue(Bytes.equals(row, 0, row.length, cell.getRowArray(), cell.getRowOffset(),
          cell.getRowLength()));
        assertEquals((byte)(i + '0'), CellUtil.cloneValue(cell)[0]);
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
    final HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf(currentTest.getMethodName())).addFamily(new HColumnDescriptor(
            "column"));
    NavigableMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>(
        Bytes.BYTES_COMPARATOR);
    for(byte[] fam : htd.getFamiliesKeys()) {
      scopes.put(fam, 0);
    }
    final byte [] row = Bytes.toBytes("row");
    WAL.Reader reader = null;
    final MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl(1);
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
      HRegionInfo hri = new HRegionInfo(htd.getTableName(),
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      final WAL log = wals.getWAL(hri.getEncodedNameAsBytes(), hri.getTable().getNamespace());
      final long txid = log.append(hri,
        new WALKey(hri.getEncodedNameAsBytes(), htd.getTableName(), System.currentTimeMillis(),
            mvcc, scopes),
        cols, true);
      log.sync(txid);
      log.startCacheFlush(hri.getEncodedNameAsBytes(), htd.getFamiliesKeys());
      log.completeCacheFlush(hri.getEncodedNameAsBytes());
      log.shutdown();
      Path filename = AbstractFSWALProvider.getCurrentFileName(log);
      // Now open a reader on the log and assert append worked.
      reader = wals.createReader(fs, filename);
      WAL.Entry entry = reader.next();
      assertEquals(COL_COUNT, entry.getEdit().size());
      int idx = 0;
      for (Cell val : entry.getEdit().getCells()) {
        assertTrue(Bytes.equals(hri.getEncodedNameAsBytes(),
          entry.getKey().getEncodedRegionName()));
        assertTrue(htd.getTableName().equals(entry.getKey().getTablename()));
        assertTrue(Bytes.equals(row, 0, row.length, val.getRowArray(), val.getRowOffset(),
          val.getRowLength()));
        assertEquals((byte) (idx + '0'), CellUtil.cloneValue(val)[0]);
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
    final TableName tableName = TableName.valueOf(currentTest.getMethodName());
    final byte [] row = Bytes.toBytes("row");
    final DumbWALActionsListener visitor = new DumbWALActionsListener();
    final MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl(1);
    long timestamp = System.currentTimeMillis();
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("column"));
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for(byte[] fam : htd.getFamiliesKeys()) {
      scopes.put(fam, 0);
    }
    HRegionInfo hri = new HRegionInfo(tableName,
        HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    final WAL log = wals.getWAL(hri.getEncodedNameAsBytes(), hri.getTable().getNamespace());
    log.registerWALActionsListener(visitor);
    for (int i = 0; i < COL_COUNT; i++) {
      WALEdit cols = new WALEdit();
      cols.add(new KeyValue(row, Bytes.toBytes("column"),
          Bytes.toBytes(Integer.toString(i)),
          timestamp, new byte[]{(byte) (i + '0')}));
      log.append(hri, new WALKey(hri.getEncodedNameAsBytes(), tableName,
          System.currentTimeMillis(), mvcc, scopes), cols, true);
    }
    log.sync();
    assertEquals(COL_COUNT, visitor.increments);
    log.unregisterWALActionsListener(visitor);
    WALEdit cols = new WALEdit();
    cols.add(new KeyValue(row, Bytes.toBytes("column"),
        Bytes.toBytes(Integer.toString(11)),
        timestamp, new byte[]{(byte) (11 + '0')}));
    log.append(hri, new WALKey(hri.getEncodedNameAsBytes(), tableName,
        System.currentTimeMillis(), mvcc, scopes), cols, true);
    log.sync();
    assertEquals(COL_COUNT, visitor.increments);
  }

  /**
   * A loaded WAL coprocessor won't break existing WAL test cases.
   */
  @Test
  public void testWALCoprocessorLoaded() throws Exception {
    // test to see whether the coprocessor is loaded or not.
    WALCoprocessorHost host = wals.getWAL(UNSPECIFIED_REGION, null).getCoprocessorHost();
    Coprocessor c = host.findCoprocessor(SampleRegionWALObserver.class.getName());
    assertNotNull(c);
  }

  static class DumbWALActionsListener extends WALActionsListener.Base {
    int increments = 0;

    @Override
    public void visitLogEntryBeforeWrite(HRegionInfo info, WALKey logKey,
                                         WALEdit logEdit) {
      increments++;
    }

    @Override
    public void visitLogEntryBeforeWrite(WALKey logKey, WALEdit logEdit) {
      // To change body of implemented methods use File | Settings | File
      // Templates.
      increments++;
    }
  }

  private static final byte[] UNSPECIFIED_REGION = new byte[]{};

}
