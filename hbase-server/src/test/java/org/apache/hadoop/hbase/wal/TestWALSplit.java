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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.FaultySequenceFileLogReader;
import org.apache.hadoop.hbase.regionserver.wal.InstrumentedLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.hadoop.hbase.wal.WALSplitter.CorruptedLogFileException;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Testing {@link WAL} splitting code.
 */
@Category({RegionServerTests.class, LargeTests.class})
public class TestWALSplit {
  {
    // Uncomment the following lines if more verbosity is needed for
    // debugging (see HBASE-12285 for details).
    //((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
  }
  private final static Log LOG = LogFactory.getLog(TestWALSplit.class);

  private static Configuration conf;
  private FileSystem fs;

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private Path HBASEDIR;
  private Path WALDIR;
  private Path OLDLOGDIR;
  private Path CORRUPTDIR;
  private Path TABLEDIR;

  private static final int NUM_WRITERS = 10;
  private static final int ENTRIES = 10; // entries per writer per region

  private static final String FILENAME_BEING_SPLIT = "testfile";
  private static final TableName TABLE_NAME =
      TableName.valueOf("t1");
  private static final byte[] FAMILY = "f1".getBytes();
  private static final byte[] QUALIFIER = "q1".getBytes();
  private static final byte[] VALUE = "v1".getBytes();
  private static final String WAL_FILE_PREFIX = "wal.dat.";
  private static List<String> REGIONS = new ArrayList<String>();
  private static final String HBASE_SKIP_ERRORS = "hbase.hlog.split.skip.errors";
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
    conf = TEST_UTIL.getConfiguration();
    conf.setClass("hbase.regionserver.hlog.writer.impl",
        InstrumentedLogWriter.class, Writer.class);
    // This is how you turn off shortcircuit read currently.  TODO: Fix.  Should read config.
    System.setProperty("hbase.tests.use.shortcircuit.reads", "false");
    // Create fake maping user to group and set it to the conf.
    Map<String, String []> u2g_map = new HashMap<String, String []>(2);
    ROBBER = User.getCurrent().getName() + "-robber";
    ZOMBIE = User.getCurrent().getName() + "-zombie";
    u2g_map.put(ROBBER, GROUP);
    u2g_map.put(ZOMBIE, GROUP);
    DFSTestUtil.updateConfWithFakeGroupMapping(conf, u2g_map);
    conf.setInt("dfs.heartbeat.interval", 1);
    TEST_UTIL.startMiniDFSCluster(2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  @Rule
  public TestName name = new TestName();
  private WALFactory wals = null;

  @Before
  public void setUp() throws Exception {
    LOG.info("Cleaning up cluster for new test.");
    fs = TEST_UTIL.getDFSCluster().getFileSystem();
    HBASEDIR = TEST_UTIL.createRootDir();
    OLDLOGDIR = new Path(HBASEDIR, HConstants.HREGION_OLDLOGDIR_NAME);
    CORRUPTDIR = new Path(HBASEDIR, HConstants.CORRUPT_DIR_NAME);
    TABLEDIR = FSUtils.getTableDir(HBASEDIR, TABLE_NAME);
    REGIONS.clear();
    Collections.addAll(REGIONS, "bbb", "ccc");
    InstrumentedLogWriter.activateFailure = false;
    this.mode = (conf.getBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false) ?
        RecoveryMode.LOG_REPLAY : RecoveryMode.LOG_SPLITTING);
    wals = new WALFactory(conf, null, name.getMethodName());
    WALDIR = new Path(HBASEDIR, AbstractFSWALProvider.getWALDirectoryName(name.getMethodName()));
    //fs.mkdirs(WALDIR);
  }

  @After
  public void tearDown() throws Exception {
    try {
      wals.close();
    } catch(IOException exception) {
      // Some tests will move WALs out from under us. In those cases, we'll get an error on close.
      LOG.info("Ignoring an error while closing down our WALFactory. Fine for some tests, but if" +
          " you see a failure look here.");
      LOG.debug("exception details", exception);
    } finally {
      wals = null;
      fs.delete(HBASEDIR, true);
    }
  }

  /**
   * Simulates splitting a WAL out from under a regionserver that is still trying to write it.
   * Ensures we do not lose edits.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=300000)
  public void testLogCannotBeWrittenOnceParsed() throws IOException, InterruptedException {
    final AtomicLong counter = new AtomicLong(0);
    AtomicBoolean stop = new AtomicBoolean(false);
    // Region we'll write edits too and then later examine to make sure they all made it in.
    final String region = REGIONS.get(0);
    final int numWriters = 3;
    Thread zombie = new ZombieLastLogWriterRegionServer(counter, stop, region, numWriters);
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
          StringBuilder ls = new StringBuilder("Contents of WALDIR (").append(WALDIR)
              .append("):\n");
          for (FileStatus status : fs.listStatus(WALDIR)) {
            ls.append("\t").append(status.toString()).append("\n");
          }
          LOG.debug(ls);
          LOG.info("Splitting WALs out from under zombie. Expecting " + numWriters + " files.");
          WALSplitter.split(HBASEDIR, WALDIR, OLDLOGDIR, fs, conf2, wals);
          LOG.info("Finished splitting out from under zombie.");
          Path[] logfiles = getLogForRegion(HBASEDIR, TABLE_NAME, region);
          assertEquals("wrong number of split files for region", numWriters, logfiles.length);
          int count = 0;
          for (Path logfile: logfiles) {
            count += countWAL(logfile);
          }
          return count;
        }
      });
      LOG.info("zombie=" + counter.get() + ", robber=" + count);
      assertTrue("The log file could have at most 1 extra log entry, but can't have less. " +
              "Zombie could write " + counter.get() + " and logfile had only " + count,
          counter.get() == count || counter.get() + 1 == count);
    } finally {
      stop.set(true);
      zombie.interrupt();
      Threads.threadDumpingIsAlive(zombie);
    }
  }

  /**
   * This thread will keep writing to a 'wal' file even after the split process has started.
   * It simulates a region server that was considered dead but woke up and wrote some more to the
   * last log entry. Does its writing as an alternate user in another filesystem instance to
   * simulate better it being a regionserver.
   */
  class ZombieLastLogWriterRegionServer extends Thread {
    final AtomicLong editsCount;
    final AtomicBoolean stop;
    final int numOfWriters;
    /**
     * Region to write edits for.
     */
    final String region;
    final User user;

    public ZombieLastLogWriterRegionServer(AtomicLong counter, AtomicBoolean stop,
        final String region, final int writers)
        throws IOException, InterruptedException {
      super("ZombieLastLogWriterRegionServer");
      setDaemon(true);
      this.stop = stop;
      this.editsCount = counter;
      this.region = region;
      this.user = User.createUserForTesting(conf, ZOMBIE, GROUP);
      numOfWriters = writers;
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
          // Index of the WAL we want to keep open.  generateWALs will leave open the WAL whose
          // index we supply here.
          int walToKeepOpen = numOfWriters - 1;
          // The below method writes numOfWriters files each with ENTRIES entries for a total of
          // numOfWriters * ENTRIES added per column family in the region.
          Writer writer = null;
          try {
            writer = generateWALs(numOfWriters, ENTRIES, walToKeepOpen);
          } catch (IOException e1) {
            throw new RuntimeException("Failed", e1);
          }
          // Update counter so has all edits written so far.
          editsCount.addAndGet(numOfWriters * ENTRIES);
          loop(writer);
          // If we've been interruped, then things should have shifted out from under us.
          // closing should error
          try {
            writer.close();
            fail("Writing closing after parsing should give an error.");
          } catch (IOException exception) {
            LOG.debug("ignoring error when closing final writer.", exception);
          }
          return null;
        }
      });
    }

    private void loop(final Writer writer) {
      byte [] regionBytes = Bytes.toBytes(this.region);
      while (!stop.get()) {
        try {
          long seq = appendEntry(writer, TABLE_NAME, regionBytes,
              ("r" + editsCount.get()).getBytes(), regionBytes, QUALIFIER, VALUE, 0);
          long count = editsCount.incrementAndGet();
          LOG.info(getName() + " sync count=" + count + ", seq=" + seq);
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            //
          }
        } catch (IOException ex) {
          LOG.error(getName() + " ex " + ex.toString());
          if (ex instanceof RemoteException) {
            LOG.error("Juliet: got RemoteException " + ex.getMessage() +
                " while writing " + (editsCount.get() + 1));
          } else {
            LOG.error(getName() + " failed to write....at " + editsCount.get());
            fail("Failed to write " + editsCount.get());
          }
          break;
        } catch (Throwable t) {
          LOG.error(getName() + " HOW? " + t);
          LOG.debug("exception details", t);
          break;
        }
      }
      LOG.info(getName() + " Writer exiting");
    }
  }

  /**
   * @throws IOException
   * @see https://issues.apache.org/jira/browse/HBASE-3020
   */
  @Test (timeout=300000)
  public void testRecoveredEditsPathForMeta() throws IOException {
    byte [] encoded = HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes();
    Path tdir = FSUtils.getTableDir(HBASEDIR, TableName.META_TABLE_NAME);
    Path regiondir = new Path(tdir,
        HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    fs.mkdirs(regiondir);
    long now = System.currentTimeMillis();
    Entry entry =
        new Entry(new WALKey(encoded,
            TableName.META_TABLE_NAME, 1, now, HConstants.DEFAULT_CLUSTER_ID),
            new WALEdit());
    Path p = WALSplitter.getRegionSplitEditsPath(fs, entry, HBASEDIR,
        FILENAME_BEING_SPLIT);
    String parentOfParent = p.getParent().getParent().getName();
    assertEquals(parentOfParent, HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
  }

  /**
   * Test old recovered edits file doesn't break WALSplitter.
   * This is useful in upgrading old instances.
   */
  @Test (timeout=300000)
  public void testOldRecoveredEditsFileSidelined() throws IOException {
    byte [] encoded = HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes();
    Path tdir = FSUtils.getTableDir(HBASEDIR, TableName.META_TABLE_NAME);
    Path regiondir = new Path(tdir,
        HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    fs.mkdirs(regiondir);
    long now = System.currentTimeMillis();
    Entry entry =
        new Entry(new WALKey(encoded,
            TableName.META_TABLE_NAME, 1, now, HConstants.DEFAULT_CLUSTER_ID),
            new WALEdit());
    Path parent = WALSplitter.getRegionDirRecoveredEditsDir(regiondir);
    assertEquals(parent.getName(), HConstants.RECOVERED_EDITS_DIR);
    fs.createNewFile(parent); // create a recovered.edits file

    Path p = WALSplitter.getRegionSplitEditsPath(fs, entry, HBASEDIR,
        FILENAME_BEING_SPLIT);
    String parentOfParent = p.getParent().getParent().getName();
    assertEquals(parentOfParent, HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    WALFactory.createRecoveredEditsWriter(fs, p, conf).close();
  }

  private void useDifferentDFSClient() throws IOException {
    // make fs act as a different client now
    // initialize will create a new DFSClient with a new client ID
    fs.initialize(fs.getUri(), conf);
  }

  @Test (timeout=300000)
  public void testSplitPreservesEdits() throws IOException{
    final String REGION = "region__1";
    REGIONS.clear();
    REGIONS.add(REGION);

    generateWALs(1, 10, -1, 0);
    useDifferentDFSClient();
    WALSplitter.split(HBASEDIR, WALDIR, OLDLOGDIR, fs, conf, wals);
    Path originalLog = (fs.listStatus(OLDLOGDIR))[0].getPath();
    Path[] splitLog = getLogForRegion(HBASEDIR, TABLE_NAME, REGION);
    assertEquals(1, splitLog.length);

    assertTrue("edits differ after split", logsAreEqual(originalLog, splitLog[0]));
  }

  @Test (timeout=300000)
  public void testSplitRemovesRegionEventsEdits() throws IOException{
    final String REGION = "region__1";
    REGIONS.clear();
    REGIONS.add(REGION);

    generateWALs(1, 10, -1, 100);
    useDifferentDFSClient();
    WALSplitter.split(HBASEDIR, WALDIR, OLDLOGDIR, fs, conf, wals);
    Path originalLog = (fs.listStatus(OLDLOGDIR))[0].getPath();
    Path[] splitLog = getLogForRegion(HBASEDIR, TABLE_NAME, REGION);
    assertEquals(1, splitLog.length);

    assertFalse("edits differ after split", logsAreEqual(originalLog, splitLog[0]));
  }

  /**
   * @param expectedEntries -1 to not assert
   * @return the count across all regions
   */
  private int splitAndCount(final int expectedFiles, final int expectedEntries)
      throws IOException {
    useDifferentDFSClient();
    WALSplitter.split(HBASEDIR, WALDIR, OLDLOGDIR, fs, conf, wals);
    int result = 0;
    for (String region : REGIONS) {
      Path[] logfiles = getLogForRegion(HBASEDIR, TABLE_NAME, region);
      assertEquals(expectedFiles, logfiles.length);
      int count = 0;
      for (Path logfile: logfiles) {
        count += countWAL(logfile);
      }
      if (-1 != expectedEntries) {
        assertEquals(expectedEntries, count);
      }
      result += count;
    }
    return result;
  }

  @Test (timeout=300000)
  public void testEmptyLogFiles() throws IOException {
    testEmptyLogFiles(true);
  }

  @Test (timeout=300000)
  public void testEmptyOpenLogFiles() throws IOException {
    testEmptyLogFiles(false);
  }

  private void testEmptyLogFiles(final boolean close) throws IOException {
    // we won't create the hlog dir until getWAL got called, so
    // make dir here when testing empty log file
    fs.mkdirs(WALDIR);
    injectEmptyFile(".empty", close);
    generateWALs(Integer.MAX_VALUE);
    injectEmptyFile("empty", close);
    splitAndCount(NUM_WRITERS, NUM_WRITERS * ENTRIES); // skip 2 empty
  }

  @Test (timeout=300000)
  public void testOpenZeroLengthReportedFileButWithDataGetsSplit() throws IOException {
    // generate logs but leave wal.dat.5 open.
    generateWALs(5);
    splitAndCount(NUM_WRITERS, NUM_WRITERS * ENTRIES);
  }

  @Test (timeout=300000)
  public void testTralingGarbageCorruptionFileSkipErrorsPasses() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateWALs(Integer.MAX_VALUE);
    corruptWAL(new Path(WALDIR, WAL_FILE_PREFIX + "5"),
        Corruptions.APPEND_GARBAGE, true);
    splitAndCount(NUM_WRITERS, NUM_WRITERS * ENTRIES);
  }

  @Test (timeout=300000)
  public void testFirstLineCorruptionLogFileSkipErrorsPasses() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateWALs(Integer.MAX_VALUE);
    corruptWAL(new Path(WALDIR, WAL_FILE_PREFIX + "5"),
        Corruptions.INSERT_GARBAGE_ON_FIRST_LINE, true);
    splitAndCount(NUM_WRITERS - 1, (NUM_WRITERS - 1) * ENTRIES); //1 corrupt
  }

  @Test (timeout=300000)
  public void testMiddleGarbageCorruptionSkipErrorsReadsHalfOfFile() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateWALs(Integer.MAX_VALUE);
    corruptWAL(new Path(WALDIR, WAL_FILE_PREFIX + "5"),
        Corruptions.INSERT_GARBAGE_IN_THE_MIDDLE, false);
    // the entries in the original logs are alternating regions
    // considering the sequence file header, the middle corruption should
    // affect at least half of the entries
    int goodEntries = (NUM_WRITERS - 1) * ENTRIES;
    int firstHalfEntries = (int) Math.ceil(ENTRIES / 2) - 1;
    int allRegionsCount = splitAndCount(NUM_WRITERS, -1);
    assertTrue("The file up to the corrupted area hasn't been parsed",
        REGIONS.size() * (goodEntries + firstHalfEntries) <= allRegionsCount);
  }

  @Test (timeout=300000)
  public void testCorruptedFileGetsArchivedIfSkipErrors() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    for (FaultySequenceFileLogReader.FailureType  failureType :
        FaultySequenceFileLogReader.FailureType.values()) {
      final Set<String> walDirContents = splitCorruptWALs(failureType);
      final Set<String> archivedLogs = new HashSet<String>();
      final StringBuilder archived = new StringBuilder("Archived logs in CORRUPTDIR:");
      for (FileStatus log : fs.listStatus(CORRUPTDIR)) {
        archived.append("\n\t").append(log.toString());
        archivedLogs.add(log.getPath().getName());
      }
      LOG.debug(archived.toString());
      assertEquals(failureType.name() + ": expected to find all of our wals corrupt.",
          walDirContents, archivedLogs);
    }
  }

  /**
   * @return set of wal names present prior to split attempt.
   * @throws IOException if the split process fails
   */
  private Set<String> splitCorruptWALs(final FaultySequenceFileLogReader.FailureType failureType)
      throws IOException {
    Class<?> backupClass = conf.getClass("hbase.regionserver.hlog.reader.impl",
        Reader.class);
    InstrumentedLogWriter.activateFailure = false;

    try {
      conf.setClass("hbase.regionserver.hlog.reader.impl",
          FaultySequenceFileLogReader.class, Reader.class);
      conf.set("faultysequencefilelogreader.failuretype", failureType.name());
      // Clean up from previous tests or previous loop
      try {
        wals.shutdown();
      } catch (IOException exception) {
        // since we're splitting out from under the factory, we should expect some closing failures.
        LOG.debug("Ignoring problem closing WALFactory.", exception);
      }
      wals.close();
      try {
        for (FileStatus log : fs.listStatus(CORRUPTDIR)) {
          fs.delete(log.getPath(), true);
        }
      } catch (FileNotFoundException exception) {
        LOG.debug("no previous CORRUPTDIR to clean.");
      }
      // change to the faulty reader
      wals = new WALFactory(conf, null, name.getMethodName());
      generateWALs(-1);
      // Our reader will render all of these files corrupt.
      final Set<String> walDirContents = new HashSet<String>();
      for (FileStatus status : fs.listStatus(WALDIR)) {
        walDirContents.add(status.getPath().getName());
      }
      useDifferentDFSClient();
      WALSplitter.split(HBASEDIR, WALDIR, OLDLOGDIR, fs, conf, wals);
      return walDirContents;
    } finally {
      conf.setClass("hbase.regionserver.hlog.reader.impl", backupClass,
          Reader.class);
    }
  }

  @Test (timeout=300000, expected = IOException.class)
  public void testTrailingGarbageCorruptionLogFileSkipErrorsFalseThrows()
      throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    splitCorruptWALs(FaultySequenceFileLogReader.FailureType.BEGINNING);
  }

  @Test (timeout=300000)
  public void testCorruptedLogFilesSkipErrorsFalseDoesNotTouchLogs()
      throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    try {
      splitCorruptWALs(FaultySequenceFileLogReader.FailureType.BEGINNING);
    } catch (IOException e) {
      LOG.debug("split with 'skip errors' set to 'false' correctly threw");
    }
    assertEquals("if skip.errors is false all files should remain in place",
        NUM_WRITERS, fs.listStatus(WALDIR).length);
  }

  private void ignoreCorruption(final Corruptions corruption, final int entryCount,
      final int expectedCount) throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);

    final String REGION = "region__1";
    REGIONS.clear();
    REGIONS.add(REGION);

    Path c1 = new Path(WALDIR, WAL_FILE_PREFIX + "0");
    generateWALs(1, entryCount, -1, 0);
    corruptWAL(c1, corruption, true);

    useDifferentDFSClient();
    WALSplitter.split(HBASEDIR, WALDIR, OLDLOGDIR, fs, conf, wals);

    Path[] splitLog = getLogForRegion(HBASEDIR, TABLE_NAME, REGION);
    assertEquals(1, splitLog.length);

    int actualCount = 0;
    Reader in = wals.createReader(fs, splitLog[0]);
    @SuppressWarnings("unused")
    Entry entry;
    while ((entry = in.next()) != null) ++actualCount;
    assertEquals(expectedCount, actualCount);
    in.close();

    // should not have stored the EOF files as corrupt
    FileStatus[] archivedLogs = fs.listStatus(CORRUPTDIR);
    assertEquals(archivedLogs.length, 0);

  }

  @Test (timeout=300000)
  public void testEOFisIgnored() throws IOException {
    int entryCount = 10;
    ignoreCorruption(Corruptions.TRUNCATE, entryCount, entryCount-1);
  }

  @Test (timeout=300000)
  public void testCorruptWALTrailer() throws IOException {
    int entryCount = 10;
    ignoreCorruption(Corruptions.TRUNCATE_TRAILER, entryCount, entryCount);
  }

  @Test (timeout=300000)
  public void testLogsGetArchivedAfterSplit() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    generateWALs(-1);
    useDifferentDFSClient();
    WALSplitter.split(HBASEDIR, WALDIR, OLDLOGDIR, fs, conf, wals);
    FileStatus[] archivedLogs = fs.listStatus(OLDLOGDIR);
    assertEquals("wrong number of files in the archive log", NUM_WRITERS, archivedLogs.length);
  }

  @Test (timeout=300000)
  public void testSplit() throws IOException {
    generateWALs(-1);
    splitAndCount(NUM_WRITERS, NUM_WRITERS * ENTRIES);
  }

  @Test (timeout=300000)
  public void testLogDirectoryShouldBeDeletedAfterSuccessfulSplit()
      throws IOException {
    generateWALs(-1);
    useDifferentDFSClient();
    WALSplitter.split(HBASEDIR, WALDIR, OLDLOGDIR, fs, conf, wals);
    FileStatus [] statuses = null;
    try {
      statuses = fs.listStatus(WALDIR);
      if (statuses != null) {
        fail("Files left in log dir: " +
            Joiner.on(",").join(FileUtil.stat2Paths(statuses)));
      }
    } catch (FileNotFoundException e) {
      // hadoop 0.21 throws FNFE whereas hadoop 0.20 returns null
    }
  }

  @Test(timeout=300000, expected = IOException.class)
  public void testSplitWillFailIfWritingToRegionFails() throws Exception {
    //leave 5th log open so we could append the "trap"
    Writer writer = generateWALs(4);
    useDifferentDFSClient();

    String region = "break";
    Path regiondir = new Path(TABLEDIR, region);
    fs.mkdirs(regiondir);

    InstrumentedLogWriter.activateFailure = false;
    appendEntry(writer, TABLE_NAME, Bytes.toBytes(region),
        ("r" + 999).getBytes(), FAMILY, QUALIFIER, VALUE, 0);
    writer.close();

    try {
      InstrumentedLogWriter.activateFailure = true;
      WALSplitter.split(HBASEDIR, WALDIR, OLDLOGDIR, fs, conf, wals);
    } catch (IOException e) {
      assertTrue(e.getMessage().
          contains("This exception is instrumented and should only be thrown for testing"));
      throw e;
    } finally {
      InstrumentedLogWriter.activateFailure = false;
    }
  }

  @Test (timeout=300000)
  public void testSplitDeletedRegion() throws IOException {
    REGIONS.clear();
    String region = "region_that_splits";
    REGIONS.add(region);

    generateWALs(1);
    useDifferentDFSClient();

    Path regiondir = new Path(TABLEDIR, region);
    fs.delete(regiondir, true);
    WALSplitter.split(HBASEDIR, WALDIR, OLDLOGDIR, fs, conf, wals);
    assertFalse(fs.exists(regiondir));
  }

  @Test (timeout=300000)
  public void testIOEOnOutputThread() throws Exception {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);

    generateWALs(-1);
    useDifferentDFSClient();
    FileStatus[] logfiles = fs.listStatus(WALDIR);
    assertTrue("There should be some log file",
        logfiles != null && logfiles.length > 0);
    // wals with no entries (like the one we don't use in the factory)
    // won't cause a failure since nothing will ever be written.
    // pick the largest one since it's most likely to have entries.
    int largestLogFile = 0;
    long largestSize = 0;
    for (int i = 0; i < logfiles.length; i++) {
      if (logfiles[i].getLen() > largestSize) {
        largestLogFile = i;
        largestSize = logfiles[i].getLen();
      }
    }
    assertTrue("There should be some log greater than size 0.", 0 < largestSize);
    // Set up a splitter that will throw an IOE on the output side
    WALSplitter logSplitter = new WALSplitter(wals,
        conf, HBASEDIR, fs, null, null, this.mode) {
      @Override
      protected Writer createWriter(Path logfile) throws IOException {
        Writer mockWriter = Mockito.mock(Writer.class);
        Mockito.doThrow(new IOException("Injected")).when(
            mockWriter).append(Mockito.<Entry>any());
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
      logSplitter.splitLogFile(logfiles[largestLogFile], null);
      fail("Didn't throw!");
    } catch (IOException ioe) {
      assertTrue(ioe.toString().contains("Injected"));
    } finally {
      // Setting this to true will turn off the background thread dumper.
      stop.set(true);
    }
  }

  /**
   * @param spiedFs should be instrumented for failure.
   */
  private void retryOverHdfsProblem(final FileSystem spiedFs) throws Exception {
    generateWALs(-1);
    useDifferentDFSClient();

    try {
      WALSplitter.split(HBASEDIR, WALDIR, OLDLOGDIR, spiedFs, conf, wals);
      assertEquals(NUM_WRITERS, fs.listStatus(OLDLOGDIR).length);
      assertFalse(fs.exists(WALDIR));
    } catch (IOException e) {
      fail("There shouldn't be any exception but: " + e.toString());
    }
  }

  // Test for HBASE-3412
  @Test (timeout=300000)
  public void testMovedWALDuringRecovery() throws Exception {
    // This partial mock will throw LEE for every file simulating
    // files that were moved
    FileSystem spiedFs = Mockito.spy(fs);
    // The "File does not exist" part is very important,
    // that's how it comes out of HDFS
    Mockito.doThrow(new LeaseExpiredException("Injected: File does not exist")).
        when(spiedFs).append(Mockito.<Path>any());
    retryOverHdfsProblem(spiedFs);
  }

  @Test (timeout=300000)
  public void testRetryOpenDuringRecovery() throws Exception {
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
    retryOverHdfsProblem(spiedFs);
  }

  @Test (timeout=300000)
  public void testTerminationAskedByReporter() throws IOException, CorruptedLogFileException {
    generateWALs(1, 10, -1);
    FileStatus logfile = fs.listStatus(WALDIR)[0];
    useDifferentDFSClient();

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
      boolean ret = WALSplitter.splitLogFile(
          HBASEDIR, logfile, spiedFs, conf, localReporter, null, null, this.mode, wals);
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
    Path logPath = new Path(WALDIR, WAL_FILE_PREFIX + ".fake");
    FSDataOutputStream out = fs.create(logPath);
    out.close();

    // Make region dirs for our destination regions so the output doesn't get skipped
    final List<String> regions = ImmutableList.of("r0", "r1", "r2", "r3", "r4");
    makeRegionDirs(regions);

    // Create a splitter that reads and writes the data without touching disk
    WALSplitter logSplitter = new WALSplitter(wals,
        localConf, HBASEDIR, fs, null, null, this.mode) {

      /* Produce a mock writer that doesn't write anywhere */
      @Override
      protected Writer createWriter(Path logfile) throws IOException {
        Writer mockWriter = Mockito.mock(Writer.class);
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
            Entry entry = (Entry) invocation.getArguments()[0];
            WALEdit edit = entry.getEdit();
            List<Cell> cells = edit.getCells();
            assertEquals(1, cells.size());
            Cell cell = cells.get(0);

            // Check that the edits come in the right order.
            assertEquals(expectedIndex, Bytes.toInt(cell.getRowArray(), cell.getRowOffset(),
                cell.getRowLength()));
            expectedIndex++;
            return null;
          }
        }).when(mockWriter).append(Mockito.<Entry>any());
        return mockWriter;
      }

      /* Produce a mock reader that generates fake entries */
      @Override
      protected Reader getReader(Path curLogFile, CancelableProgressable reporter)
          throws IOException {
        Reader mockReader = Mockito.mock(Reader.class);
        Mockito.doAnswer(new Answer<Entry>() {
          int index = 0;

          @Override
          public Entry answer(InvocationOnMock invocation) throws Throwable {
            if (index >= numFakeEdits) return null;

            // Generate r0 through r4 in round robin fashion
            int regionIdx = index % regions.size();
            byte region[] = new byte[] {(byte)'r', (byte) (0x30 + regionIdx)};

            Entry ret = createTestEntry(TABLE_NAME, region,
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
    assertEquals("Should have as many outputs as regions", regions.size(), outputCounts.size());
  }

  // Does leaving the writer open in testSplitDeletedRegion matter enough for two tests?
  @Test (timeout=300000)
  public void testSplitLogFileDeletedRegionDir() throws IOException {
    LOG.info("testSplitLogFileDeletedRegionDir");
    final String REGION = "region__1";
    REGIONS.clear();
    REGIONS.add(REGION);

    generateWALs(1, 10, -1);
    useDifferentDFSClient();

    Path regiondir = new Path(TABLEDIR, REGION);
    LOG.info("Region directory is" + regiondir);
    fs.delete(regiondir, true);
    WALSplitter.split(HBASEDIR, WALDIR, OLDLOGDIR, fs, conf, wals);
    assertFalse(fs.exists(regiondir));
  }

  @Test (timeout=300000)
  public void testSplitLogFileEmpty() throws IOException {
    LOG.info("testSplitLogFileEmpty");
    // we won't create the hlog dir until getWAL got called, so
    // make dir here when testing empty log file
    fs.mkdirs(WALDIR);
    injectEmptyFile(".empty", true);
    useDifferentDFSClient();

    WALSplitter.split(HBASEDIR, WALDIR, OLDLOGDIR, fs, conf, wals);
    Path tdir = FSUtils.getTableDir(HBASEDIR, TABLE_NAME);
    assertFalse(fs.exists(tdir));

    assertEquals(0, countWAL(fs.listStatus(OLDLOGDIR)[0].getPath()));
  }

  @Test (timeout=300000)
  public void testSplitLogFileMultipleRegions() throws IOException {
    LOG.info("testSplitLogFileMultipleRegions");
    generateWALs(1, 10, -1);
    splitAndCount(1, 10);
  }

  @Test (timeout=300000)
  public void testSplitLogFileFirstLineCorruptionLog()
      throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateWALs(1, 10, -1);
    FileStatus logfile = fs.listStatus(WALDIR)[0];

    corruptWAL(logfile.getPath(),
        Corruptions.INSERT_GARBAGE_ON_FIRST_LINE, true);

    useDifferentDFSClient();
    WALSplitter.split(HBASEDIR, WALDIR, OLDLOGDIR, fs, conf, wals);

    final Path corruptDir = new Path(FSUtils.getRootDir(conf), HConstants.CORRUPT_DIR_NAME);
    assertEquals(1, fs.listStatus(corruptDir).length);
  }

  /**
   * @throws IOException
   * @see https://issues.apache.org/jira/browse/HBASE-4862
   */
  @Test (timeout=300000)
  public void testConcurrentSplitLogAndReplayRecoverEdit() throws IOException {
    LOG.info("testConcurrentSplitLogAndReplayRecoverEdit");
    // Generate wals for our destination region
    String regionName = "r0";
    final Path regiondir = new Path(TABLEDIR, regionName);
    REGIONS.clear();
    REGIONS.add(regionName);
    generateWALs(-1);

    wals.getWAL(Bytes.toBytes(regionName), null);
    FileStatus[] logfiles = fs.listStatus(WALDIR);
    assertTrue("There should be some log file",
        logfiles != null && logfiles.length > 0);

    WALSplitter logSplitter = new WALSplitter(wals,
        conf, HBASEDIR, fs, null, null, this.mode) {
      @Override
      protected Writer createWriter(Path logfile)
          throws IOException {
        Writer writer = wals.createRecoveredEditsWriter(this.fs, logfile);
        // After creating writer, simulate region's
        // replayRecoveredEditsIfAny() which gets SplitEditFiles of this
        // region and delete them, excluding files with '.temp' suffix.
        NavigableSet<Path> files = WALSplitter.getSplitEditFilesSorted(fs, regiondir);
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
      fail("Throws IOException when spliting "
          + "log, it is most likely because writing file does not "
          + "exist which is caused by concurrent replayRecoveredEditsIfAny()");
    }
    if (fs.exists(CORRUPTDIR)) {
      if (fs.listStatus(CORRUPTDIR).length > 0) {
        fail("There are some corrupt logs, "
            + "it is most likely caused by concurrent replayRecoveredEditsIfAny()");
      }
    }
  }

  private Writer generateWALs(int leaveOpen) throws IOException {
    return generateWALs(NUM_WRITERS, ENTRIES, leaveOpen, 0);
  }

  private Writer generateWALs(int writers, int entries, int leaveOpen) throws IOException {
    return generateWALs(writers, entries, leaveOpen, 7);
  }

  private void makeRegionDirs(List<String> regions) throws IOException {
    for (String region : regions) {
      LOG.debug("Creating dir for region " + region);
      fs.mkdirs(new Path(TABLEDIR, region));
    }
  }

  /**
   * @param leaveOpen index to leave un-closed. -1 to close all.
   * @return the writer that's still open, or null if all were closed.
   */
  private Writer generateWALs(int writers, int entries, int leaveOpen, int regionEvents) throws IOException {
    makeRegionDirs(REGIONS);
    fs.mkdirs(WALDIR);
    Writer [] ws = new Writer[writers];
    int seq = 0;
    int numRegionEventsAdded = 0;
    for (int i = 0; i < writers; i++) {
      ws[i] = wals.createWALWriter(fs, new Path(WALDIR, WAL_FILE_PREFIX + i));
      for (int j = 0; j < entries; j++) {
        int prefix = 0;
        for (String region : REGIONS) {
          String row_key = region + prefix++ + i + j;
          appendEntry(ws[i], TABLE_NAME, region.getBytes(), row_key.getBytes(), FAMILY, QUALIFIER,
              VALUE, seq++);

          if (numRegionEventsAdded < regionEvents) {
            numRegionEventsAdded ++;
            appendRegionEvent(ws[i], region);
          }
        }
      }
      if (i != leaveOpen) {
        ws[i].close();
        LOG.info("Closing writer " + i);
      }
    }
    if (leaveOpen < 0 || leaveOpen >= writers) {
      return null;
    }
    return ws[leaveOpen];
  }



  private Path[] getLogForRegion(Path rootdir, TableName table, String region)
      throws IOException {
    Path tdir = FSUtils.getTableDir(rootdir, table);
    @SuppressWarnings("deprecation")
    Path editsdir = WALSplitter.getRegionDirRecoveredEditsDir(HRegion.getRegionDir(tdir,
        Bytes.toString(region.getBytes())));
    FileStatus[] files = fs.listStatus(editsdir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        if (WALSplitter.isSequenceIdFile(p)) {
          return false;
        }
        return true;
      }
    });
    Path[] paths = new Path[files.length];
    for (int i = 0; i < files.length; i++) {
      paths[i] = files[i].getPath();
    }
    return paths;
  }

  private void corruptWAL(Path path, Corruptions corruption, boolean close) throws IOException {
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

  private int countWAL(Path log) throws IOException {
    int count = 0;
    Reader in = wals.createReader(fs, log);
    while (in.next() != null) {
      count++;
    }
    in.close();
    return count;
  }

  private static void appendRegionEvent(Writer w, String region) throws IOException {
    WALProtos.RegionEventDescriptor regionOpenDesc = ProtobufUtil.toRegionEventDescriptor(
        WALProtos.RegionEventDescriptor.EventType.REGION_OPEN,
        TABLE_NAME.toBytes(),
        region.getBytes(),
        String.valueOf(region.hashCode()).getBytes(),
        1,
        ServerName.parseServerName("ServerName:9099"), ImmutableMap.<byte[], List<Path>>of());
    final long time = EnvironmentEdgeManager.currentTime();
    KeyValue kv = new KeyValue(region.getBytes(), WALEdit.METAFAMILY, WALEdit.REGION_EVENT,
        time, regionOpenDesc.toByteArray());
    final WALKey walKey = new WALKey(region.getBytes(), TABLE_NAME, 1, time,
        HConstants.DEFAULT_CLUSTER_ID);
    w.append(
        new Entry(walKey, new WALEdit().add(kv)));
  }

  public static long appendEntry(Writer writer, TableName table, byte[] region,
      byte[] row, byte[] family, byte[] qualifier,
      byte[] value, long seq)
      throws IOException {
    LOG.info(Thread.currentThread().getName() + " append");
    writer.append(createTestEntry(table, region, row, family, qualifier, value, seq));
    LOG.info(Thread.currentThread().getName() + " sync");
    writer.sync();
    return seq;
  }

  private static Entry createTestEntry(
      TableName table, byte[] region,
      byte[] row, byte[] family, byte[] qualifier,
      byte[] value, long seq) {
    long time = System.nanoTime();

    seq++;
    final KeyValue cell = new KeyValue(row, family, qualifier, time, KeyValue.Type.Put, value);
    WALEdit edit = new WALEdit();
    edit.add(cell);
    return new Entry(new WALKey(region, table, seq, time,
        HConstants.DEFAULT_CLUSTER_ID), edit);
  }

  private void injectEmptyFile(String suffix, boolean closeFile)
      throws IOException {
    Writer writer = wals.createWALWriter(fs, new Path(WALDIR, WAL_FILE_PREFIX + suffix),
        conf);
    if (closeFile) writer.close();
  }

  private boolean logsAreEqual(Path p1, Path p2) throws IOException {
    Reader in1, in2;
    in1 = wals.createReader(fs, p1);
    in2 = wals.createReader(fs, p2);
    Entry entry1;
    Entry entry2;
    while ((entry1 = in1.next()) != null) {
      entry2 = in2.next();
      if ((entry1.getKey().compareTo(entry2.getKey()) != 0) ||
          (!entry1.getEdit().toString().equals(entry2.getEdit().toString()))) {
        return false;
      }
    }
    in1.close();
    in2.close();
    return true;
  }
}
