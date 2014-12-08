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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter.CorruptedLogFileException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

/**
 * Testing {@link HLog} splitting code.
 */
@Category(LargeTests.class)
public class TestHLogSplit {
  {
    // Uncomment the following lines if more verbosity is needed for
    // debugging (see HBASE-12285 for details).
    //((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
  }
  private final static Log LOG = LogFactory.getLog(TestHLogSplit.class);

  private Configuration conf;
  private FileSystem fs;

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final Path HBASEDIR = new Path("/hbase");
  private static final Path HLOGDIR = new Path(HBASEDIR, "hlog");
  private static final Path OLDLOGDIR = new Path(HBASEDIR, "hlog.old");
  private static final Path CORRUPTDIR = new Path(HBASEDIR, HConstants.CORRUPT_DIR_NAME);

  private static final int NUM_WRITERS = 10;
  private static final int ENTRIES = 10; // entries per writer per region

  private static final TableName TABLE_NAME =
      TableName.valueOf("t1");
  private static final byte[] FAMILY = "f1".getBytes();
  private static final byte[] QUALIFIER = "q1".getBytes();
  private static final byte[] VALUE = "v1".getBytes();
  private static final String HLOG_FILE_PREFIX = "hlog.dat.";
  private static List<String> REGIONS = new ArrayList<String>();
  private static final String HBASE_SKIP_ERRORS = "hbase.hlog.split.skip.errors";
  private static final Path TABLEDIR = FSUtils.getTableDir(HBASEDIR, TABLE_NAME);
  private static String ROBBER;
  private static String ZOMBIE;
  private static String [] GROUP = new String [] {"supergroup"};
  private RecoveryMode mode;

  static enum Corruptions {
    INSERT_GARBAGE_ON_FIRST_LINE,
    INSERT_GARBAGE_IN_THE_MIDDLE,
    APPEND_GARBAGE,
    TRUNCATE,
    TRUNCATE_TRAILER
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    FSUtils.setRootDir(TEST_UTIL.getConfiguration(), HBASEDIR);
    TEST_UTIL.getConfiguration().setClass("hbase.regionserver.hlog.writer.impl",
      InstrumentedSequenceFileLogWriter.class, HLog.Writer.class);
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.broken.append", true);
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    // This is how you turn off shortcircuit read currently.  TODO: Fix.  Should read config.
    System.setProperty("hbase.tests.use.shortcircuit.reads", "false");
    // Create fake maping user to group and set it to the conf.
    Map<String, String []> u2g_map = new HashMap<String, String []>(2);
    ROBBER = User.getCurrent().getName() + "-robber";
    ZOMBIE = User.getCurrent().getName() + "-zombie";
    u2g_map.put(ROBBER, GROUP);
    u2g_map.put(ZOMBIE, GROUP);
    DFSTestUtil.updateConfWithFakeGroupMapping(TEST_UTIL.getConfiguration(), u2g_map);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    TEST_UTIL.startMiniDFSCluster(2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  @Before
  public void setUp() throws Exception {
    flushToConsole("Cleaning up cluster for new test\n"
        + "--------------------------");
    conf = TEST_UTIL.getConfiguration();
    fs = TEST_UTIL.getDFSCluster().getFileSystem();
    FileStatus[] entries = fs.listStatus(new Path("/"));
    flushToConsole("Num entries in /:" + entries.length);
    for (FileStatus dir : entries){
      assertTrue("Deleting " + dir.getPath(), fs.delete(dir.getPath(), true));
    }
    // create the HLog directory because recursive log creates are not allowed
    fs.mkdirs(HLOGDIR);
    REGIONS.clear();
    Collections.addAll(REGIONS, "bbb", "ccc");
    InstrumentedSequenceFileLogWriter.activateFailure = false;
    this.mode = (conf.getBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false) ? 
        RecoveryMode.LOG_REPLAY : RecoveryMode.LOG_SPLITTING);
  }

  @After
  public void tearDown() throws Exception {
  }

  /**
   * Simulates splitting a WAL out from under a regionserver that is still trying to write it.  Ensures we do not
   * lose edits.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=300000)
  public void testLogCannotBeWrittenOnceParsed() throws IOException, InterruptedException {
    final AtomicLong counter = new AtomicLong(0);
    AtomicBoolean stop = new AtomicBoolean(false);
    // Region we'll write edits too and then later examine to make sure they all made it in.
    final String region = REGIONS.get(0);
    Thread zombie = new ZombieLastLogWriterRegionServer(this.conf, counter, stop, region);
    try {
      long startCount = counter.get();
      zombie.start();
      // Wait till writer starts going.
      while (startCount == counter.get()) Threads.sleep(1);
      // Give it a second to write a few appends.
      Threads.sleep(1000);
      final Configuration conf2 = HBaseConfiguration.create(this.conf);
      final User robber = User.createUserForTesting(conf2, ROBBER, GROUP);
      int count = robber.runAs(new PrivilegedExceptionAction<Integer>() {
        @Override
        public Integer run() throws Exception {
          FileSystem fs = FileSystem.get(conf2);
          int expectedFiles = fs.listStatus(HLOGDIR).length;
          HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf2);
          Path[] logfiles = getLogForRegion(HBASEDIR, TABLE_NAME, region);
          assertEquals(expectedFiles, logfiles.length);
          int count = 0;
          for (Path logfile: logfiles) {
            count += countHLog(logfile, fs, conf2);
          }
          return count;
        }
      });
      LOG.info("zombie=" + counter.get() + ", robber=" + count);
      assertTrue("The log file could have at most 1 extra log entry, but can't have less. Zombie could write " +
        counter.get() + " and logfile had only " + count,
        counter.get() == count || counter.get() + 1 == count);
    } finally {
      stop.set(true);
      zombie.interrupt();
      Threads.threadDumpingIsAlive(zombie);
    }
  }

  /**
   * This thread will keep writing to a 'wal' file even after the split process has started.
   * It simulates a region server that was considered dead but woke up and wrote some more to he last log entry.
   * Does its writing as an alternate user in another filesystem instance to simulate better it being a regionserver.
   */
  static class ZombieLastLogWriterRegionServer extends Thread {
    final AtomicLong editsCount;
    final AtomicBoolean stop;
    // final User user;
    /**
     * Region to write edits for.
     */
    final String region;
    final Configuration conf;
    final User user;

    public ZombieLastLogWriterRegionServer(final Configuration conf, AtomicLong counter, AtomicBoolean stop,
        final String region)
    throws IOException, InterruptedException {
      super("ZombieLastLogWriterRegionServer");
      setDaemon(true);
      this.stop = stop;
      this.editsCount = counter;
      this.region = region;
      this.conf = HBaseConfiguration.create(conf);
      this.user = User.createUserForTesting(this.conf, ZOMBIE, GROUP);
    }

    @Override
    public void run() {
      try {
        doWriting();
      } catch (IOException e) {
        LOG.warn(getName() + " Writer exiting " + e);
      } catch (InterruptedException e) {
        LOG.warn(getName() + " Writer exiting " + e);
      }
    }

    private void doWriting() throws IOException, InterruptedException {
      this.user.runAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // Index of the WAL we want to keep open.  generateHLogs will leave open the WAL whose index we supply here.
          int walToKeepOpen = 2;
          // How many files to write.
          final int numOfWriters = walToKeepOpen + 1;
          // The below method writes numOfWriters files each with ENTRIES entries for a total of numOfWriters * ENTRIES
          // added per column family in the region.
          HLog.Writer[] writers = null;
          try {
            DistributedFileSystem dfs = (DistributedFileSystem)FileSystem.get(conf);
            writers = generateHLogs(dfs, numOfWriters, ENTRIES, walToKeepOpen);
          } catch (IOException e1) {
            throw new RuntimeException("Failed", e1);
          }
          // Update counter so has all edits written so far.
          editsCount.addAndGet(numOfWriters * NUM_WRITERS);
          // This WAL should be open still after our call to generateHLogs -- we asked it leave it open.
          HLog.Writer writer = writers[walToKeepOpen];
          loop(writer);
          return null;
        }
      });
    }

    private void loop(final HLog.Writer writer) {
      byte [] regionBytes = Bytes.toBytes(this.region);
      while (true) {
        try {
          long seq = appendEntry(writer, TABLE_NAME, regionBytes, ("r" + editsCount.get()).getBytes(),
            regionBytes, QUALIFIER, VALUE, 0);
          long count = editsCount.incrementAndGet();
          flushToConsole(getName() + " sync count=" + count + ", seq=" + seq);
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            //
          }
        } catch (IOException ex) {
          flushToConsole(getName() + " ex " + ex.toString());
          if (ex instanceof RemoteException) {
            flushToConsole("Juliet: got RemoteException " + ex.getMessage() +
              " while writing " + (editsCount.get() + 1));
          } else {
            flushToConsole(getName() + " failed to write....at " + editsCount.get());
            assertTrue("Failed to write " + editsCount.get(), false);
          }
          break;
        } catch (Throwable t) {
          flushToConsole(getName() + " HOW? " + t);
          t.printStackTrace();
          break;
        }
      }
      flushToConsole(getName() + " Writer exiting");
    }
  }

  /**
   * @throws IOException
   * @see https://issues.apache.org/jira/browse/HBASE-3020
   */
  @Test (timeout=300000)
  public void testRecoveredEditsPathForMeta() throws IOException {
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    byte [] encoded = HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes();
    Path tdir = FSUtils.getTableDir(HBASEDIR, TableName.META_TABLE_NAME);
    Path regiondir = new Path(tdir,
        HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    fs.mkdirs(regiondir);
    long now = System.currentTimeMillis();
    HLog.Entry entry =
        new HLog.Entry(new HLogKey(encoded,
            TableName.META_TABLE_NAME, 1, now, HConstants.DEFAULT_CLUSTER_ID),
      new WALEdit());
    Path p = HLogSplitter.getRegionSplitEditsPath(fs, entry, HBASEDIR, true);
    String parentOfParent = p.getParent().getParent().getName();
    assertEquals(parentOfParent, HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
  }

  /**
   * Test old recovered edits file doesn't break HLogSplitter.
   * This is useful in upgrading old instances.
   */
  @Test (timeout=300000)
  public void testOldRecoveredEditsFileSidelined() throws IOException {
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    byte [] encoded = HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes();
    Path tdir = FSUtils.getTableDir(HBASEDIR, TableName.META_TABLE_NAME);
    Path regiondir = new Path(tdir,
        HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    fs.mkdirs(regiondir);
    long now = System.currentTimeMillis();
    HLog.Entry entry =
        new HLog.Entry(new HLogKey(encoded,
            TableName.META_TABLE_NAME, 1, now, HConstants.DEFAULT_CLUSTER_ID),
      new WALEdit());
    Path parent = HLogUtil.getRegionDirRecoveredEditsDir(regiondir);
    assertEquals(parent.getName(), HConstants.RECOVERED_EDITS_DIR);
    fs.createNewFile(parent); // create a recovered.edits file

    Path p = HLogSplitter.getRegionSplitEditsPath(fs, entry, HBASEDIR, true);
    String parentOfParent = p.getParent().getParent().getName();
    assertEquals(parentOfParent, HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    HLogFactory.createRecoveredEditsWriter(fs, p, conf).close();
  }

  @Test (timeout=300000)
  public void testSplitPreservesEdits() throws IOException{
    final String REGION = "region__1";
    REGIONS.removeAll(REGIONS);
    REGIONS.add(REGION);

    generateHLogs(1, 10, -1);
    fs.initialize(fs.getUri(), conf);
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    Path originalLog = (fs.listStatus(OLDLOGDIR))[0].getPath();
    Path[] splitLog = getLogForRegion(HBASEDIR, TABLE_NAME, REGION);
    assertEquals(1, splitLog.length);

    assertEquals("edits differ after split", true, logsAreEqual(originalLog, splitLog[0]));
  }


  @Test (timeout=300000)
  public void testEmptyLogFiles() throws IOException {

    injectEmptyFile(".empty", true);
    generateHLogs(Integer.MAX_VALUE);
    injectEmptyFile("empty", true);

    // make fs act as a different client now
    // initialize will create a new DFSClient with a new client ID
    fs.initialize(fs.getUri(), conf);

    int expectedFiles = fs.listStatus(HLOGDIR).length - 2; // less 2 empty files
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    for (String region : REGIONS) {
      Path[] logfiles = getLogForRegion(HBASEDIR, TABLE_NAME, region);
      assertEquals(expectedFiles, logfiles.length);
      int count = 0;
      for (Path logfile: logfiles) {
        count += countHLog(logfile, fs, conf);
      }
      assertEquals(NUM_WRITERS * ENTRIES, count);
    }
  }


  @Test (timeout=300000)
  public void testEmptyOpenLogFiles() throws IOException {
    injectEmptyFile(".empty", false);
    generateHLogs(Integer.MAX_VALUE);
    injectEmptyFile("empty", false);

    // make fs act as a different client now
    // initialize will create a new DFSClient with a new client ID
    fs.initialize(fs.getUri(), conf);

    int expectedFiles = fs.listStatus(HLOGDIR).length - 2 ; // less 2 empty files
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    for (String region : REGIONS) {
      Path[] logfiles = getLogForRegion(HBASEDIR, TABLE_NAME, region);
      assertEquals(expectedFiles, logfiles.length);
      int count = 0;
      for (Path logfile: logfiles) {
        count += countHLog(logfile, fs, conf);
      }
      assertEquals(NUM_WRITERS * ENTRIES, count);
    }
  }

  @Test (timeout=300000)
  public void testOpenZeroLengthReportedFileButWithDataGetsSplit() throws IOException {
    // generate logs but leave hlog.dat.5 open.
    generateHLogs(5);

    fs.initialize(fs.getUri(), conf);

    int expectedFiles = fs.listStatus(HLOGDIR).length;
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    for (String region : REGIONS) {
      Path[] logfiles = getLogForRegion(HBASEDIR, TABLE_NAME, region);
      assertEquals(expectedFiles, logfiles.length);
      int count = 0;
      for (Path logfile: logfiles) {
        count += countHLog(logfile, fs, conf);
      }
      assertEquals(NUM_WRITERS * ENTRIES, count);
    }
  }


  @Test (timeout=300000)
  public void testTralingGarbageCorruptionFileSkipErrorsPasses() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateHLogs(Integer.MAX_VALUE);
    corruptHLog(new Path(HLOGDIR, HLOG_FILE_PREFIX + "5"),
            Corruptions.APPEND_GARBAGE, true, fs);
    fs.initialize(fs.getUri(), conf);

    int expectedFiles = fs.listStatus(HLOGDIR).length;
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    for (String region : REGIONS) {
      Path[] logfiles = getLogForRegion(HBASEDIR, TABLE_NAME, region);
      assertEquals(expectedFiles, logfiles.length);
      int count = 0;
      for (Path logfile: logfiles) {
        count += countHLog(logfile, fs, conf);
      }
      assertEquals(NUM_WRITERS * ENTRIES, count);
    }
  }

  @Test (timeout=300000)
  public void testFirstLineCorruptionLogFileSkipErrorsPasses() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateHLogs(Integer.MAX_VALUE);
    corruptHLog(new Path(HLOGDIR, HLOG_FILE_PREFIX + "5"),
            Corruptions.INSERT_GARBAGE_ON_FIRST_LINE, true, fs);
    fs.initialize(fs.getUri(), conf);

    int expectedFiles = fs.listStatus(HLOGDIR).length - 1; // less 1 corrupted file
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    for (String region : REGIONS) {
      Path[] logfiles = getLogForRegion(HBASEDIR, TABLE_NAME, region);
      assertEquals(expectedFiles, logfiles.length);
      int count = 0;
      for (Path logfile: logfiles) {
        count += countHLog(logfile, fs, conf);
      }
      assertEquals((NUM_WRITERS - 1) * ENTRIES, count);
    }
  }

  @Test (timeout=300000)
  public void testMiddleGarbageCorruptionSkipErrorsReadsHalfOfFile() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateHLogs(Integer.MAX_VALUE);
    corruptHLog(new Path(HLOGDIR, HLOG_FILE_PREFIX + "5"),
            Corruptions.INSERT_GARBAGE_IN_THE_MIDDLE, false, fs);
    fs.initialize(fs.getUri(), conf);

    int expectedFiles = fs.listStatus(HLOGDIR).length;
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    for (String region : REGIONS) {
      Path[] logfiles = getLogForRegion(HBASEDIR, TABLE_NAME, region);
      assertEquals(expectedFiles, logfiles.length);
      int count = 0;
      for (Path logfile: logfiles) {
        count += countHLog(logfile, fs, conf);
      }
      // the entries in the original logs are alternating regions
      // considering the sequence file header, the middle corruption should
      // affect at least half of the entries
      int goodEntries = (NUM_WRITERS - 1) * ENTRIES;
      int firstHalfEntries = (int) Math.ceil(ENTRIES / 2) - 1;
      assertTrue("The file up to the corrupted area hasn't been parsed",
              goodEntries + firstHalfEntries <= count);
    }
  }

  @Test (timeout=300000)
  public void testCorruptedFileGetsArchivedIfSkipErrors() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    Class<?> backupClass = conf.getClass("hbase.regionserver.hlog.reader.impl",
        Reader.class);
    InstrumentedSequenceFileLogWriter.activateFailure = false;
    HLogFactory.resetLogReaderClass();

    try {
    Path c1 = new Path(HLOGDIR, HLOG_FILE_PREFIX + "0");
      conf.setClass("hbase.regionserver.hlog.reader.impl",
          FaultySequenceFileLogReader.class, HLog.Reader.class);
      for (FaultySequenceFileLogReader.FailureType  failureType : FaultySequenceFileLogReader.FailureType.values()) {
        conf.set("faultysequencefilelogreader.failuretype", failureType.name());
        generateHLogs(1, ENTRIES, -1);
        fs.initialize(fs.getUri(), conf);
        HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
        FileStatus[] archivedLogs = fs.listStatus(CORRUPTDIR);
        assertEquals("expected a different file", c1.getName(), archivedLogs[0]
            .getPath().getName());
        assertEquals(archivedLogs.length, 1);
        fs.delete(new Path(OLDLOGDIR, HLOG_FILE_PREFIX + "0"), false);
      }
    } finally {
      conf.setClass("hbase.regionserver.hlog.reader.impl", backupClass,
          Reader.class);
      HLogFactory.resetLogReaderClass();
    }
  }

  @Test (timeout=300000, expected = IOException.class)
  public void testTrailingGarbageCorruptionLogFileSkipErrorsFalseThrows()
      throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    Class<?> backupClass = conf.getClass("hbase.regionserver.hlog.reader.impl",
        Reader.class);
    InstrumentedSequenceFileLogWriter.activateFailure = false;
    HLogFactory.resetLogReaderClass();

    try {
      conf.setClass("hbase.regionserver.hlog.reader.impl",
          FaultySequenceFileLogReader.class, HLog.Reader.class);
      conf.set("faultysequencefilelogreader.failuretype", FaultySequenceFileLogReader.FailureType.BEGINNING.name());
      generateHLogs(Integer.MAX_VALUE);
      fs.initialize(fs.getUri(), conf);
      HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    } finally {
      conf.setClass("hbase.regionserver.hlog.reader.impl", backupClass,
          Reader.class);
      HLogFactory.resetLogReaderClass();
    }
  }

  @Test (timeout=300000)
  public void testCorruptedLogFilesSkipErrorsFalseDoesNotTouchLogs()
      throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    Class<?> backupClass = conf.getClass("hbase.regionserver.hlog.reader.impl",
        Reader.class);
    InstrumentedSequenceFileLogWriter.activateFailure = false;
    HLogFactory.resetLogReaderClass();

    try {
      conf.setClass("hbase.regionserver.hlog.reader.impl",
          FaultySequenceFileLogReader.class, HLog.Reader.class);
      conf.set("faultysequencefilelogreader.failuretype", FaultySequenceFileLogReader.FailureType.BEGINNING.name());
      generateHLogs(-1);
      fs.initialize(fs.getUri(), conf);
      try {
        HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
      } catch (IOException e) {
        assertEquals(
            "if skip.errors is false all files should remain in place",
            NUM_WRITERS, fs.listStatus(HLOGDIR).length);
      }
    } finally {
      conf.setClass("hbase.regionserver.hlog.reader.impl", backupClass,
          Reader.class);
      HLogFactory.resetLogReaderClass();
    }
  }

  @Test (timeout=300000)
  public void testEOFisIgnored() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);

    final String REGION = "region__1";
    REGIONS.removeAll(REGIONS);
    REGIONS.add(REGION);

    int entryCount = 10;
    Path c1 = new Path(HLOGDIR, HLOG_FILE_PREFIX + "0");
    generateHLogs(1, entryCount, -1);
    corruptHLog(c1, Corruptions.TRUNCATE, true, fs);

    fs.initialize(fs.getUri(), conf);
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);

    Path[] splitLog = getLogForRegion(HBASEDIR, TABLE_NAME, REGION);
    assertEquals(1, splitLog.length);

    int actualCount = 0;
    HLog.Reader in = HLogFactory.createReader(fs, splitLog[0], conf);
    @SuppressWarnings("unused")
    HLog.Entry entry;
    while ((entry = in.next()) != null) ++actualCount;
    assertEquals(entryCount-1, actualCount);

    // should not have stored the EOF files as corrupt
    FileStatus[] archivedLogs = fs.listStatus(CORRUPTDIR);
    assertEquals(archivedLogs.length, 0);
  }

  @Test (timeout=300000)
  public void testCorruptWALTrailer() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);

    final String REGION = "region__1";
    REGIONS.removeAll(REGIONS);
    REGIONS.add(REGION);

    int entryCount = 10;
    Path c1 = new Path(HLOGDIR, HLOG_FILE_PREFIX + "0");
    generateHLogs(1, entryCount, -1);
    corruptHLog(c1, Corruptions.TRUNCATE_TRAILER, true, fs);

    fs.initialize(fs.getUri(), conf);
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);

    Path[] splitLog = getLogForRegion(HBASEDIR, TABLE_NAME, REGION);
    assertEquals(1, splitLog.length);

    int actualCount = 0;
    HLog.Reader in = HLogFactory.createReader(fs, splitLog[0], conf);
    @SuppressWarnings("unused")
    HLog.Entry entry;
    while ((entry = in.next()) != null) ++actualCount;
    assertEquals(entryCount, actualCount);

    // should not have stored the EOF files as corrupt
    FileStatus[] archivedLogs = fs.listStatus(CORRUPTDIR);
    assertEquals(archivedLogs.length, 0);
  }

  @Test (timeout=300000)
  public void testLogsGetArchivedAfterSplit() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    generateHLogs(-1);
    fs.initialize(fs.getUri(), conf);
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    FileStatus[] archivedLogs = fs.listStatus(OLDLOGDIR);
    assertEquals("wrong number of files in the archive log", NUM_WRITERS, archivedLogs.length);
  }

  @Test (timeout=300000)
  public void testSplit() throws IOException {
    generateHLogs(-1);
    fs.initialize(fs.getUri(), conf);

    int expectedFiles = fs.listStatus(HLOGDIR).length;
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    for (String region : REGIONS) {
      Path[] logfiles = getLogForRegion(HBASEDIR, TABLE_NAME, region);
      assertEquals(expectedFiles, logfiles.length);
      int count = 0;
      for (Path logfile: logfiles) {
        count += countHLog(logfile, fs, conf);
      }
      assertEquals(NUM_WRITERS * ENTRIES, count);
    }
  }

  @Test (timeout=300000)
  public void testLogDirectoryShouldBeDeletedAfterSuccessfulSplit()
  throws IOException {
    generateHLogs(-1);
    fs.initialize(fs.getUri(), conf);
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    FileStatus [] statuses = null;
    try {
      statuses = fs.listStatus(HLOGDIR);
      if (statuses != null) {
        Assert.fail("Files left in log dir: " +
            Joiner.on(",").join(FileUtil.stat2Paths(statuses)));
      }
    } catch (FileNotFoundException e) {
      // hadoop 0.21 throws FNFE whereas hadoop 0.20 returns null
    }
  }

  @Test(timeout=300000, expected = IOException.class)
  public void testSplitWillFailIfWritingToRegionFails() throws Exception {
    //leave 5th log open so we could append the "trap"
    HLog.Writer [] writer = generateHLogs(4);

    fs.initialize(fs.getUri(), conf);

    String region = "break";
    Path regiondir = new Path(TABLEDIR, region);
    fs.mkdirs(regiondir);

    InstrumentedSequenceFileLogWriter.activateFailure = false;
    appendEntry(writer[4], TABLE_NAME, Bytes.toBytes(region),
        ("r" + 999).getBytes(), FAMILY, QUALIFIER, VALUE, 0);
    writer[4].close();

    try {
      InstrumentedSequenceFileLogWriter.activateFailure = true;
      HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    } catch (IOException e) {
      assertTrue(e.getMessage().
        contains("This exception is instrumented and should only be thrown for testing"));
      throw e;
    } finally {
      InstrumentedSequenceFileLogWriter.activateFailure = false;
    }
  }


  // @Test TODO this test has been disabled since it was created!
  // It currently fails because the second split doesn't output anything
  // -- because there are no region dirs after we move aside the first
  // split result
  public void testSplittingLargeNumberOfRegionsConsistency() throws IOException {

    REGIONS.removeAll(REGIONS);
    for (int i=0; i<100; i++) {
      REGIONS.add("region__"+i);
    }

    generateHLogs(1, 100, -1);
    fs.initialize(fs.getUri(), conf);

    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    fs.rename(OLDLOGDIR, HLOGDIR);
    Path firstSplitPath = new Path(HBASEDIR, TABLE_NAME+ ".first");
    Path splitPath = new Path(HBASEDIR, TABLE_NAME.getNameAsString());
    fs.rename(splitPath,
            firstSplitPath);

    fs.initialize(fs.getUri(), conf);
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    assertEquals(0, compareHLogSplitDirs(firstSplitPath, splitPath));
  }

  @Test (timeout=300000)
  public void testSplitDeletedRegion() throws IOException {
    REGIONS.removeAll(REGIONS);
    String region = "region_that_splits";
    REGIONS.add(region);

    generateHLogs(1);
    fs.initialize(fs.getUri(), conf);

    Path regiondir = new Path(TABLEDIR, region);
    fs.delete(regiondir, true);
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    assertFalse(fs.exists(regiondir));
  }

  @Test (timeout=300000)
  public void testIOEOnOutputThread() throws Exception {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);

    generateHLogs(-1);
    fs.initialize(fs.getUri(), conf);
    FileStatus[] logfiles = fs.listStatus(HLOGDIR);
    assertTrue("There should be some log file",
      logfiles != null && logfiles.length > 0);
    // Set up a splitter that will throw an IOE on the output side
    HLogSplitter logSplitter = new HLogSplitter(
        conf, HBASEDIR, fs, null, null, this.mode) {
      protected HLog.Writer createWriter(FileSystem fs,
          Path logfile, Configuration conf) throws IOException {
        HLog.Writer mockWriter = Mockito.mock(HLog.Writer.class);
        Mockito.doThrow(new IOException("Injected")).when(
          mockWriter).append(Mockito.<HLog.Entry>any());
        return mockWriter;
      }
    };
    // Set up a background thread dumper.  Needs a thread to depend on and then we need to run
    // the thread dumping in a background thread so it does not hold up the test.
    final AtomicBoolean stop = new AtomicBoolean(false);
    final Thread someOldThread = new Thread("Some-old-thread") {
      @Override
      public void run() {
        while(!stop.get()) Threads.sleep(10);
      }
    };
    someOldThread.setDaemon(true);
    someOldThread.start();
    final Thread t = new Thread("Background-thread-dumper") {
      public void run() {
        try {
          Threads.threadDumpingIsAlive(someOldThread);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    t.setDaemon(true);
    t.start();
    try {
      logSplitter.splitLogFile(logfiles[0], null);
      fail("Didn't throw!");
    } catch (IOException ioe) {
      assertTrue(ioe.toString().contains("Injected"));
    } finally {
      // Setting this to true will turn off the background thread dumper.
      stop.set(true);
    }
  }

  // Test for HBASE-3412
  @Test (timeout=300000)
  public void testMovedHLogDuringRecovery() throws Exception {
    generateHLogs(-1);

    fs.initialize(fs.getUri(), conf);

    // This partial mock will throw LEE for every file simulating
    // files that were moved
    FileSystem spiedFs = Mockito.spy(fs);
    // The "File does not exist" part is very important,
    // that's how it comes out of HDFS
    Mockito.doThrow(new LeaseExpiredException("Injected: File does not exist")).
        when(spiedFs).append(Mockito.<Path>any());

    try {
      HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, spiedFs, conf);
      assertEquals(NUM_WRITERS, fs.listStatus(OLDLOGDIR).length);
      assertFalse(fs.exists(HLOGDIR));
    } catch (IOException e) {
      fail("There shouldn't be any exception but: " + e.toString());
    }
  }

  @Test (timeout=300000)
  public void testRetryOpenDuringRecovery() throws Exception {
    generateHLogs(-1);

    fs.initialize(fs.getUri(), conf);

    FileSystem spiedFs = Mockito.spy(fs);
    // The "Cannot obtain block length", "Could not obtain the last block",
    // and "Blocklist for [^ ]* has changed.*" part is very important,
    // that's how it comes out of HDFS. If HDFS changes the exception
    // message, this test needs to be adjusted accordingly.
    //
    // When DFSClient tries to open a file, HDFS needs to locate
    // the last block of the file and get its length. However, if the
    // last block is under recovery, HDFS may have problem to obtain
    // the block length, in which case, retry may help.
    Mockito.doAnswer(new Answer<FSDataInputStream>() {
      private final String[] errors = new String[] {
        "Cannot obtain block length", "Could not obtain the last block",
        "Blocklist for " + OLDLOGDIR + " has changed"};
      private int count = 0;

      public FSDataInputStream answer(InvocationOnMock invocation) throws Throwable {
            if (count < 3) {
                throw new IOException(errors[count++]);
            }
            return (FSDataInputStream)invocation.callRealMethod();
        }
    }).when(spiedFs).open(Mockito.<Path>any(), Mockito.anyInt());

    try {
      HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, spiedFs, conf);
      assertEquals(NUM_WRITERS, fs.listStatus(OLDLOGDIR).length);
      assertFalse(fs.exists(HLOGDIR));
    } catch (IOException e) {
      fail("There shouldn't be any exception but: " + e.toString());
    }
  }

  @Test (timeout=300000)
  public void testTerminationAskedByReporter() throws IOException, CorruptedLogFileException {
    generateHLogs(1, 10, -1);
    FileStatus logfile = fs.listStatus(HLOGDIR)[0];
    fs.initialize(fs.getUri(), conf);

    final AtomicInteger count = new AtomicInteger();

    CancelableProgressable localReporter
      = new CancelableProgressable() {
        @Override
        public boolean progress() {
          count.getAndIncrement();
          return false;
        }
      };

    FileSystem spiedFs = Mockito.spy(fs);
    Mockito.doAnswer(new Answer<FSDataInputStream>() {
      public FSDataInputStream answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(1500); // Sleep a while and wait report status invoked
        return (FSDataInputStream)invocation.callRealMethod();
      }
    }).when(spiedFs).open(Mockito.<Path>any(), Mockito.anyInt());

    try {
      conf.setInt("hbase.splitlog.report.period", 1000);
      boolean ret = HLogSplitter.splitLogFile(
        HBASEDIR, logfile, spiedFs, conf, localReporter, null, null, this.mode);
      assertFalse("Log splitting should failed", ret);
      assertTrue(count.get() > 0);
    } catch (IOException e) {
      fail("There shouldn't be any exception but: " + e.toString());
    } finally {
      // reset it back to its default value
      conf.setInt("hbase.splitlog.report.period", 59000);
    }
  }

  /**
   * Test log split process with fake data and lots of edits to trigger threading
   * issues.
   */
  @Test (timeout=300000)
  public void testThreading() throws Exception {
    doTestThreading(20000, 128*1024*1024, 0);
  }

  /**
   * Test blocking behavior of the log split process if writers are writing slower
   * than the reader is reading.
   */
  @Test (timeout=300000)
  public void testThreadingSlowWriterSmallBuffer() throws Exception {
    doTestThreading(200, 1024, 50);
  }

  /**
   * Sets up a log splitter with a mock reader and writer. The mock reader generates
   * a specified number of edits spread across 5 regions. The mock writer optionally
   * sleeps for each edit it is fed.
   * *
   * After the split is complete, verifies that the statistics show the correct number
   * of edits output into each region.
   *
   * @param numFakeEdits number of fake edits to push through pipeline
   * @param bufferSize size of in-memory buffer
   * @param writerSlowness writer threads will sleep this many ms per edit
   */
  private void doTestThreading(final int numFakeEdits,
      final int bufferSize,
      final int writerSlowness) throws Exception {

    Configuration localConf = new Configuration(conf);
    localConf.setInt("hbase.regionserver.hlog.splitlog.buffersize", bufferSize);

    // Create a fake log file (we'll override the reader to produce a stream of edits)
    Path logPath = new Path(HLOGDIR, HLOG_FILE_PREFIX + ".fake");
    FSDataOutputStream out = fs.create(logPath);
    out.close();

    // Make region dirs for our destination regions so the output doesn't get skipped
    final List<String> regions = ImmutableList.of("r0", "r1", "r2", "r3", "r4");
    makeRegionDirs(fs, regions);

    // Create a splitter that reads and writes the data without touching disk
    HLogSplitter logSplitter = new HLogSplitter(
        localConf, HBASEDIR, fs, null, null, this.mode) {

      /* Produce a mock writer that doesn't write anywhere */
      protected HLog.Writer createWriter(FileSystem fs, Path logfile, Configuration conf)
      throws IOException {
        HLog.Writer mockWriter = Mockito.mock(HLog.Writer.class);
        Mockito.doAnswer(new Answer<Void>() {
          int expectedIndex = 0;

          @Override
          public Void answer(InvocationOnMock invocation) {
            if (writerSlowness > 0) {
              try {
                Thread.sleep(writerSlowness);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
              }
            }
            HLog.Entry entry = (Entry) invocation.getArguments()[0];
            WALEdit edit = entry.getEdit();
            List<KeyValue> keyValues = edit.getKeyValues();
            assertEquals(1, keyValues.size());
            KeyValue kv = keyValues.get(0);

            // Check that the edits come in the right order.
            assertEquals(expectedIndex, Bytes.toInt(kv.getRow()));
            expectedIndex++;
            return null;
          }
        }).when(mockWriter).append(Mockito.<HLog.Entry>any());
        return mockWriter;
      }

      /* Produce a mock reader that generates fake entries */
      protected Reader getReader(FileSystem fs, Path curLogFile,
          Configuration conf, CancelableProgressable reporter) throws IOException {
        Reader mockReader = Mockito.mock(Reader.class);
        Mockito.doAnswer(new Answer<HLog.Entry>() {
          int index = 0;

          @Override
          public HLog.Entry answer(InvocationOnMock invocation) throws Throwable {
            if (index >= numFakeEdits) return null;

            // Generate r0 through r4 in round robin fashion
            int regionIdx = index % regions.size();
            byte region[] = new byte[] {(byte)'r', (byte) (0x30 + regionIdx)};

            HLog.Entry ret = createTestEntry(TABLE_NAME, region,
                Bytes.toBytes((int)(index / regions.size())),
                FAMILY, QUALIFIER, VALUE, index);
            index++;
            return ret;
          }
        }).when(mockReader).next();
        return mockReader;
      }
    };

    logSplitter.splitLogFile(fs.getFileStatus(logPath), null);

    // Verify number of written edits per region
    Map<byte[], Long> outputCounts = logSplitter.outputSink.getOutputCounts();
    for (Map.Entry<byte[], Long> entry : outputCounts.entrySet()) {
      LOG.info("Got " + entry.getValue() + " output edits for region " +
          Bytes.toString(entry.getKey()));
      assertEquals((long)entry.getValue(), numFakeEdits / regions.size());
    }
    assertEquals(regions.size(), outputCounts.size());
  }

  // HBASE-2312: tests the case where a RegionServer enters a GC pause,
  // comes back online after the master declared it dead and started to split.
  // Want log rolling after a master split to fail
  @Test (timeout=300000)
  @Ignore("Need HADOOP-6886, HADOOP-6840, & HDFS-617 for this. HDFS 0.20.205.1+ should have this")
  public void testLogRollAfterSplitStart() throws IOException {
    HLog log = null;
    String logName = "testLogRollAfterSplitStart";
    Path thisTestsDir = new Path(HBASEDIR, logName);

    try {
      // put some entries in an HLog
      TableName tableName =
          TableName.valueOf(this.getClass().getName());
      HRegionInfo regioninfo = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      log = HLogFactory.createHLog(fs, HBASEDIR, logName, conf);
      final AtomicLong sequenceId = new AtomicLong(1);

      final int total = 20;
      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), tableName.getName()));
        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.addFamily(new HColumnDescriptor("column"));
        log.append(regioninfo, tableName, kvs, System.currentTimeMillis(), htd, sequenceId);
      }
      // Send the data to HDFS datanodes and close the HDFS writer
      log.sync();
      ((FSHLog) log).cleanupCurrentWriter(log.getFilenum());

      /* code taken from ProcessServerShutdown.process()
       * handles RS shutdowns (as observed by the Master)
       */
      // rename the directory so a rogue RS doesn't create more HLogs
      Path rsSplitDir = new Path(thisTestsDir.getParent(),
                                 thisTestsDir.getName() + "-splitting");
      fs.rename(thisTestsDir, rsSplitDir);
      LOG.debug("Renamed region directory: " + rsSplitDir);

      // Process the old log files
      HLogSplitter.split(HBASEDIR, rsSplitDir, OLDLOGDIR, fs, conf);

      // Now, try to roll the HLog and verify failure
      try {
        log.rollWriter();
        Assert.fail("rollWriter() did not throw any exception.");
      } catch (IOException ioe) {
        if (ioe.getCause().getMessage().contains("FileNotFound")) {
          LOG.info("Got the expected exception: ", ioe.getCause());
        } else {
          Assert.fail("Unexpected exception: " + ioe);
        }
      }
    } finally {
      if (log != null) {
        log.close();
      }
      if (fs.exists(thisTestsDir)) {
        fs.delete(thisTestsDir, true);
      }
    }
  }

  /**
   * This thread will keep adding new log files
   * It simulates a region server that was considered dead but woke up and wrote
   * some more to a new hlog
   */
  class ZombieNewLogWriterRegionServer extends Thread {
    AtomicBoolean stop;
    CountDownLatch latch;
    public ZombieNewLogWriterRegionServer(CountDownLatch latch, AtomicBoolean stop) {
      super("ZombieNewLogWriterRegionServer");
      this.latch = latch;
      this.stop = stop;
    }

    @Override
    public void run() {
      if (stop.get()) {
        return;
      }
      Path tableDir = FSUtils.getTableDir(HBASEDIR, TABLE_NAME);
      Path regionDir = new Path(tableDir, REGIONS.get(0));
      Path recoveredEdits = new Path(regionDir, HConstants.RECOVERED_EDITS_DIR);
      String region = "juliet";
      Path julietLog = new Path(HLOGDIR, HLOG_FILE_PREFIX + ".juliet");
      try {

        while (!fs.exists(recoveredEdits) && !stop.get()) {
          LOG.info("Juliet: split not started, sleeping a bit...");
          Threads.sleep(10);
        }

        fs.mkdirs(new Path(tableDir, region));
        HLog.Writer writer = HLogFactory.createWALWriter(fs,
          julietLog, conf);
        appendEntry(writer, TableName.valueOf("juliet"), ("juliet").getBytes(),
            ("r").getBytes(), FAMILY, QUALIFIER, VALUE, 0);
        writer.close();
        LOG.info("Juliet file creator: created file " + julietLog);
        latch.countDown();
      } catch (IOException e1) {
        LOG.error("Failed to create file " + julietLog, e1);
        assertTrue("Failed to create file " + julietLog, false);
      }
    }
  }

  @Test (timeout=300000)
  public void testSplitLogFileWithOneRegion() throws IOException {
    LOG.info("testSplitLogFileWithOneRegion");
    final String REGION = "region__1";
    REGIONS.removeAll(REGIONS);
    REGIONS.add(REGION);

    generateHLogs(1, 10, -1);
    fs.initialize(fs.getUri(), conf);
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);

    Path originalLog = (fs.listStatus(OLDLOGDIR))[0].getPath();
    Path[] splitLog = getLogForRegion(HBASEDIR, TABLE_NAME, REGION);
    assertEquals(1, splitLog.length);

    assertEquals(true, logsAreEqual(originalLog, splitLog[0]));
  }

  @Test (timeout=300000)
  public void testSplitLogFileDeletedRegionDir() throws IOException {
    LOG.info("testSplitLogFileDeletedRegionDir");
    final String REGION = "region__1";
    REGIONS.removeAll(REGIONS);
    REGIONS.add(REGION);

    generateHLogs(1, 10, -1);
    fs.initialize(fs.getUri(), conf);

    Path regiondir = new Path(TABLEDIR, REGION);
    LOG.info("Region directory is" + regiondir);
    fs.delete(regiondir, true);

    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);

    assertTrue(!fs.exists(regiondir));
    assertTrue(true);
  }

  @Test (timeout=300000)
  public void testSplitLogFileEmpty() throws IOException {
    LOG.info("testSplitLogFileEmpty");
    injectEmptyFile(".empty", true);

    fs.initialize(fs.getUri(), conf);

    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    Path tdir = FSUtils.getTableDir(HBASEDIR, TABLE_NAME);
    assertFalse(fs.exists(tdir));

    assertEquals(0, countHLog(fs.listStatus(OLDLOGDIR)[0].getPath(), fs, conf));
  }

  @Test (timeout=300000)
  public void testSplitLogFileMultipleRegions() throws IOException {
    LOG.info("testSplitLogFileMultipleRegions");
    generateHLogs(1, 10, -1);
    fs.initialize(fs.getUri(), conf);

    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);
    for (String region : REGIONS) {
      Path[] recovered = getLogForRegion(HBASEDIR, TABLE_NAME, region);
      assertEquals(1, recovered.length);
      assertEquals(10, countHLog(recovered[0], fs, conf));
    }
  }

  @Test (timeout=300000)
  public void testSplitLogFileFirstLineCorruptionLog()
  throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateHLogs(1, 10, -1);
    FileStatus logfile = fs.listStatus(HLOGDIR)[0];

    corruptHLog(logfile.getPath(),
        Corruptions.INSERT_GARBAGE_ON_FIRST_LINE, true, fs);

    fs.initialize(fs.getUri(), conf);
    HLogSplitter.split(HBASEDIR, HLOGDIR, OLDLOGDIR, fs, conf);

    final Path corruptDir = new Path(FSUtils.getRootDir(conf), conf.get(
        "hbase.regionserver.hlog.splitlog.corrupt.dir", HConstants.CORRUPT_DIR_NAME));
    assertEquals(1, fs.listStatus(corruptDir).length);
  }

  /**
   * @throws IOException
   * @see https://issues.apache.org/jira/browse/HBASE-4862
   */
  @Test (timeout=300000)
  public void testConcurrentSplitLogAndReplayRecoverEdit() throws IOException {
    LOG.info("testConcurrentSplitLogAndReplayRecoverEdit");
    // Generate hlogs for our destination region
    String regionName = "r0";
    final Path regiondir = new Path(TABLEDIR, regionName);
    REGIONS = new ArrayList<String>();
    REGIONS.add(regionName);
    generateHLogs(-1);

    HLogFactory.createHLog(fs, regiondir, regionName, conf);
    FileStatus[] logfiles = fs.listStatus(HLOGDIR);
    assertTrue("There should be some log file",
      logfiles != null && logfiles.length > 0);

    HLogSplitter logSplitter = new HLogSplitter(
        conf, HBASEDIR, fs, null, null, this.mode) {
      protected HLog.Writer createWriter(FileSystem fs, Path logfile, Configuration conf)
      throws IOException {
        HLog.Writer writer = HLogFactory.createRecoveredEditsWriter(fs, logfile, conf);
        // After creating writer, simulate region's
        // replayRecoveredEditsIfAny() which gets SplitEditFiles of this
        // region and delete them, excluding files with '.temp' suffix.
        NavigableSet<Path> files = HLogUtil.getSplitEditFilesSorted(fs, regiondir);
        if (files != null && !files.isEmpty()) {
          for (Path file : files) {
            if (!this.fs.delete(file, false)) {
              LOG.error("Failed delete of " + file);
            } else {
              LOG.debug("Deleted recovered.edits file=" + file);
            }
          }
        }
        return writer;
      }
    };
    try{
      logSplitter.splitLogFile(logfiles[0], null);
    } catch (IOException e) {
      LOG.info(e);
      Assert.fail("Throws IOException when spliting "
          + "log, it is most likely because writing file does not "
          + "exist which is caused by concurrent replayRecoveredEditsIfAny()");
    }
    if (fs.exists(CORRUPTDIR)) {
      if (fs.listStatus(CORRUPTDIR).length > 0) {
        Assert.fail("There are some corrupt logs, "
                + "it is most likely caused by concurrent replayRecoveredEditsIfAny()");
      }
    }
  }

  private static void flushToConsole(String s) {
    System.out.println(s);
    System.out.flush();
  }


  private HLog.Writer [] generateHLogs(int leaveOpen) throws IOException {
    return generateHLogs(NUM_WRITERS, ENTRIES, leaveOpen);
  }

  private HLog.Writer [] generateHLogs(final int writers, final int entries, final int leaveOpen) throws IOException {
    return generateHLogs((DistributedFileSystem)this.fs, writers, entries, leaveOpen);
  }

  private static void makeRegionDirs(FileSystem fs, List<String> regions) throws IOException {
    for (String region : regions) {
      flushToConsole("Creating dir for region " + region);
      fs.mkdirs(new Path(TABLEDIR, region));
    }
  }

  private static HLog.Writer [] generateHLogs(final DistributedFileSystem dfs, int writers, int entries, int leaveOpen)
  throws IOException {
    makeRegionDirs(dfs, REGIONS);
    dfs.mkdirs(HLOGDIR);
    HLog.Writer [] ws = new HLog.Writer[writers];
    int seq = 0;
    for (int i = 0; i < writers; i++) {
      ws[i] = HLogFactory.createWALWriter(dfs, new Path(HLOGDIR, HLOG_FILE_PREFIX + i), dfs.getConf());
      for (int j = 0; j < entries; j++) {
        int prefix = 0;
        for (String region : REGIONS) {
          String row_key = region + prefix++ + i + j;
          appendEntry(ws[i], TABLE_NAME, region.getBytes(), row_key.getBytes(), FAMILY, QUALIFIER, VALUE, seq++);
        }
      }
      if (i != leaveOpen) {
        ws[i].close();
        LOG.info("Closing writer " + i);
      }
    }
    return ws;
  }

  private Path[] getLogForRegion(Path rootdir, TableName table, String region)
  throws IOException {
    Path tdir = FSUtils.getTableDir(rootdir, table);
    @SuppressWarnings("deprecation")
    Path editsdir = HLogUtil.getRegionDirRecoveredEditsDir(HRegion.getRegionDir(tdir,
      Bytes.toString(region.getBytes())));
    FileStatus [] files = this.fs.listStatus(editsdir);
    Path[] paths = new Path[files.length];
    for (int i = 0; i < files.length; i++) {
      paths[i] = files[i].getPath();
    }
    return paths;
  }

  private void corruptHLog(Path path, Corruptions corruption, boolean close,
                           FileSystem fs) throws IOException {

    FSDataOutputStream out;
    int fileSize = (int) fs.listStatus(path)[0].getLen();

    FSDataInputStream in = fs.open(path);
    byte[] corrupted_bytes = new byte[fileSize];
    in.readFully(0, corrupted_bytes, 0, fileSize);
    in.close();

    switch (corruption) {
      case APPEND_GARBAGE:
        fs.delete(path, false);
        out = fs.create(path);
        out.write(corrupted_bytes);
        out.write("-----".getBytes());
        closeOrFlush(close, out);
        break;

      case INSERT_GARBAGE_ON_FIRST_LINE:
        fs.delete(path, false);
        out = fs.create(path);
        out.write(0);
        out.write(corrupted_bytes);
        closeOrFlush(close, out);
        break;

      case INSERT_GARBAGE_IN_THE_MIDDLE:
        fs.delete(path, false);
        out = fs.create(path);
        int middle = (int) Math.floor(corrupted_bytes.length / 2);
        out.write(corrupted_bytes, 0, middle);
        out.write(0);
        out.write(corrupted_bytes, middle, corrupted_bytes.length - middle);
        closeOrFlush(close, out);
        break;

      case TRUNCATE:
        fs.delete(path, false);
        out = fs.create(path);
        out.write(corrupted_bytes, 0, fileSize
          - (32 + ProtobufLogReader.PB_WAL_COMPLETE_MAGIC.length + Bytes.SIZEOF_INT));
        closeOrFlush(close, out);
        break;

      case TRUNCATE_TRAILER:
        fs.delete(path, false);
        out = fs.create(path);
        out.write(corrupted_bytes, 0, fileSize - Bytes.SIZEOF_INT);// trailer is truncated.
        closeOrFlush(close, out);
        break;
    }
  }

  private void closeOrFlush(boolean close, FSDataOutputStream out)
  throws IOException {
    if (close) {
      out.close();
    } else {
      Method syncMethod = null;
      try {
        syncMethod = out.getClass().getMethod("hflush", new Class<?> []{});
      } catch (NoSuchMethodException e) {
        try {
          syncMethod = out.getClass().getMethod("sync", new Class<?> []{});
        } catch (NoSuchMethodException ex) {
          throw new IOException("This version of Hadoop supports " +
              "neither Syncable.sync() nor Syncable.hflush().");
        }
      }
      try {
        syncMethod.invoke(out, new Object[]{});
      } catch (Exception e) {
        throw new IOException(e);
      }
      // Not in 0out.hflush();
    }
  }

  @SuppressWarnings("unused")
  private void dumpHLog(Path log, FileSystem fs, Configuration conf) throws IOException {
    HLog.Entry entry;
    HLog.Reader in = HLogFactory.createReader(fs, log, conf);
    while ((entry = in.next()) != null) {
      System.out.println(entry);
    }
  }

  private int countHLog(Path log, FileSystem fs, Configuration conf) throws IOException {
    int count = 0;
    HLog.Reader in = HLogFactory.createReader(fs, log, conf);
    while (in.next() != null) {
      count++;
    }
    return count;
  }


  public static long appendEntry(HLog.Writer writer, TableName table, byte[] region,
                          byte[] row, byte[] family, byte[] qualifier,
                          byte[] value, long seq)
          throws IOException {
    LOG.info(Thread.currentThread().getName() + " append");
    writer.append(createTestEntry(table, region, row, family, qualifier, value, seq));
    LOG.info(Thread.currentThread().getName() + " sync");
    writer.sync();
    return seq;
  }

  private static HLog.Entry createTestEntry(
      TableName table, byte[] region,
      byte[] row, byte[] family, byte[] qualifier,
      byte[] value, long seq) {
    long time = System.nanoTime();
    WALEdit edit = new WALEdit();
    seq++;
    edit.add(new KeyValue(row, family, qualifier, time, KeyValue.Type.Put, value));
    return new HLog.Entry(new HLogKey(region, table, seq, time,
        HConstants.DEFAULT_CLUSTER_ID), edit);
  }


  private void injectEmptyFile(String suffix, boolean closeFile)
          throws IOException {
    HLog.Writer writer = HLogFactory.createWALWriter(
        fs, new Path(HLOGDIR, HLOG_FILE_PREFIX + suffix), conf);
    if (closeFile) writer.close();
  }

  @SuppressWarnings("unused")
  private void listLogs(FileSystem fs, Path dir) throws IOException {
    for (FileStatus file : fs.listStatus(dir)) {
      System.out.println(file.getPath());
    }

  }

  private int compareHLogSplitDirs(Path p1, Path p2) throws IOException {
    FileStatus[] f1 = fs.listStatus(p1);
    FileStatus[] f2 = fs.listStatus(p2);
    assertNotNull("Path " + p1 + " doesn't exist", f1);
    assertNotNull("Path " + p2 + " doesn't exist", f2);

    System.out.println("Files in " + p1 + ": " +
        Joiner.on(",").join(FileUtil.stat2Paths(f1)));
    System.out.println("Files in " + p2 + ": " +
        Joiner.on(",").join(FileUtil.stat2Paths(f2)));
    assertEquals(f1.length, f2.length);

    for (int i = 0; i < f1.length; i++) {
      // Regions now have a directory named RECOVERED_EDITS_DIR and in here
      // are split edit files. In below presume only 1.
      Path rd1 = HLogUtil.getRegionDirRecoveredEditsDir(f1[i].getPath());
      FileStatus[] rd1fs = fs.listStatus(rd1);
      assertEquals(1, rd1fs.length);
      Path rd2 = HLogUtil.getRegionDirRecoveredEditsDir(f2[i].getPath());
      FileStatus[] rd2fs = fs.listStatus(rd2);
      assertEquals(1, rd2fs.length);
      if (!logsAreEqual(rd1fs[0].getPath(), rd2fs[0].getPath())) {
        return -1;
      }
    }
    return 0;
  }

  private boolean logsAreEqual(Path p1, Path p2) throws IOException {
    HLog.Reader in1, in2;
    in1 = HLogFactory.createReader(fs, p1, conf);
    in2 = HLogFactory.createReader(fs, p2, conf);
    HLog.Entry entry1;
    HLog.Entry entry2;
    while ((entry1 = in1.next()) != null) {
      entry2 = in2.next();
      if ((entry1.getKey().compareTo(entry2.getKey()) != 0) ||
              (!entry1.getEdit().toString().equals(entry2.getEdit().toString()))) {
        return false;
      }
    }
    return true;
  }
}
