/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
/**
 * Testing {@link HLog} splitting code.
 */
@Category(MediumTests.class)
public class TestHLogSplit {
  private static final Log LOG = LogFactory.getLog(TestHLogSplit.class);

  private Configuration conf;
  private FileSystem fs;

  private final static HBaseTestingUtility
          TEST_UTIL = new HBaseTestingUtility();


  private static final Path hbaseDir = new Path("/hbase");
  private static final Path hlogDir = new Path(hbaseDir, "hlog");
  private static final Path oldLogDir = new Path(hbaseDir, "hlog.old");
  private static final Path corruptDir = new Path(hbaseDir, ".corrupt");

  private static final int NUM_WRITERS = 10;
  private static final int ENTRIES = 10; // entries per writer per region
  private static final int NUM_CLOSE_THREADS = 10;

  private HLog.Writer[] writer = new HLog.Writer[NUM_WRITERS];
  private long seq = 0;
  private static final byte[] TABLE_NAME = "t1".getBytes();
  private static final byte[] FAMILY = "f1".getBytes();
  private static final byte[] QUALIFIER = "q1".getBytes();
  private static final byte[] VALUE = "v1".getBytes();
  private static final String HLOG_FILE_PREFIX = "hlog.dat.";
  private static List<String> regions;
  private static final String HBASE_SKIP_ERRORS = "hbase.hlog.split.skip.errors";
  private static final Path tabledir = new Path(hbaseDir,
      Bytes.toString(TABLE_NAME));

  private static final ExecutorService logCloseThreadPool =
      Executors.newFixedThreadPool(NUM_CLOSE_THREADS,
          new DaemonThreadFactory("split-logClose-thread-"));

  static enum Corruptions {
    INSERT_GARBAGE_ON_FIRST_LINE,
    INSERT_GARBAGE_IN_THE_MIDDLE,
    APPEND_GARBAGE,
    TRUNCATE,
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().
            setInt("hbase.regionserver.flushlogentries", 1);
    TEST_UTIL.getConfiguration().
            setBoolean("dfs.support.append", true);
    TEST_UTIL.getConfiguration().
            setStrings("hbase.rootdir", hbaseDir.toString());
    TEST_UTIL.getConfiguration().
            setClass("hbase.regionserver.hlog.writer.impl",
                InstrumentedSequenceFileLogWriter.class, HLog.Writer.class);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.HLOG_FORMAT_BACKWARD_COMPATIBILITY, false);
    TEST_UTIL.startMiniDFSCluster(2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  @Before
  public void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    fs = TEST_UTIL.getDFSCluster().getFileSystem();
    FileStatus[] entries = fs.listStatus(new Path("/"));
    for (FileStatus dir : entries){
      fs.delete(dir.getPath(), true);
    }
    // create the HLog directory because recursive log creates are not allowed
    fs.mkdirs(hlogDir);
    seq = 0;
    regions = new ArrayList<String>();
    Collections.addAll(regions, "bbb", "ccc");
    InstrumentedSequenceFileLogWriter.activateFailure = false;
    // Set the soft lease for hdfs to be down from default of 5 minutes or so.
    TEST_UTIL.setNameNodeNameSystemLeasePeriod(100, 50000);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test(expected = IOException.class)
  public void testSplitFailsIfNewHLogGetsCreatedAfterSplitStarted()
  throws IOException {
    AtomicBoolean stop = new AtomicBoolean(false);
    generateHLogs(-1);
    fs.initialize(fs.getUri(), conf);
    try {
    (new ZombieNewLogWriterRegionServer(stop)).start();
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);
    } finally {
      stop.set(true);
    }
  }


  @Test
  public void testSplitPreservesEdits() throws IOException{
    final String REGION = "region__1";
    regions.removeAll(regions);
    regions.add(REGION);

    generateHLogs(1, 10, -1);
    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    Path originalLog = (fs.listStatus(oldLogDir))[0].getPath();
    originalLog = (fs.listStatus(originalLog))[0].getPath();
    Path splitLog = getLogForRegion(hbaseDir, TABLE_NAME, REGION);

    assertEquals("edits differ after split", true, logsAreEqual(originalLog, splitLog));
  }


  @Test
  public void testEmptyLogFiles() throws IOException {

    injectEmptyFile(".empty", true);
    generateHLogs(Integer.MAX_VALUE);
    injectEmptyFile("empty", true);

    // make fs act as a different client now
    // initialize will create a new DFSClient with a new client ID
    fs.initialize(fs.getUri(), conf);

    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);


    for (String region : regions) {
      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, region);
      assertEquals(NUM_WRITERS * ENTRIES, countHLog(logfile, fs, conf));
    }

  }


  @Test
  public void testEmptyOpenLogFiles() throws IOException {
    injectEmptyFile(".empty", false);
    generateHLogs(Integer.MAX_VALUE);
    injectEmptyFile("empty", false);

    // make fs act as a different client now
    // initialize will create a new DFSClient with a new client ID
    fs.initialize(fs.getUri(), conf);

    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    for (String region : regions) {
      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, region);
      assertEquals(NUM_WRITERS * ENTRIES, countHLog(logfile, fs, conf));
    }
  }

  @Test
  public void testOpenZeroLengthReportedFileButWithDataGetsSplit() throws IOException {
    // generate logs but leave hlog.dat.5 open.
    generateHLogs(5);

    fs.initialize(fs.getUri(), conf);

    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    for (String region : regions) {
      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, region);
      assertEquals(NUM_WRITERS * ENTRIES, countHLog(logfile, fs, conf));
    }


  }


  @Test
  public void testTralingGarbageCorruptionFileSkipErrorsPasses() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateHLogs(Integer.MAX_VALUE);
    corruptHLog(new Path(hlogDir, HLOG_FILE_PREFIX + "5"),
            Corruptions.APPEND_GARBAGE, true, fs);
    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    for (String region : regions) {
      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, region);
      assertEquals(NUM_WRITERS * ENTRIES, countHLog(logfile, fs, conf));
    }


  }

  @Test
  public void testFirstLineCorruptionLogFileSkipErrorsPasses() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateHLogs(Integer.MAX_VALUE);
    corruptHLog(new Path(hlogDir, HLOG_FILE_PREFIX + "5"),
            Corruptions.INSERT_GARBAGE_ON_FIRST_LINE, true, fs);
    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    for (String region : regions) {
      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, region);
      assertEquals((NUM_WRITERS - 1) * ENTRIES, countHLog(logfile, fs, conf));
    }


  }


  @Test
  public void testMiddleGarbageCorruptionSkipErrorsReadsHalfOfFile() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateHLogs(Integer.MAX_VALUE);
    corruptHLog(new Path(hlogDir, HLOG_FILE_PREFIX + "5"),
            Corruptions.INSERT_GARBAGE_IN_THE_MIDDLE, false, fs);
    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    for (String region : regions) {
      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, region);
      // the entries in the original logs are alternating regions
      // considering the sequence file header, the middle corruption should
      // affect at least half of the entries
      int goodEntries = (NUM_WRITERS - 1) * ENTRIES;
      int firstHalfEntries = (int) Math.ceil(ENTRIES / 2) - 1;
      assertTrue("The file up to the corrupted area hasn't been parsed",
              goodEntries + firstHalfEntries <= countHLog(logfile, fs, conf));
    }
  }

  // TODO: fix this test (HBASE-2935)
  //@Test
  public void testCorruptedFileGetsArchivedIfSkipErrors() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);

    Path c1 = new Path(hlogDir, HLOG_FILE_PREFIX + "0");
    Path c2 = new Path(hlogDir, HLOG_FILE_PREFIX + "5");
    Path c3 = new Path(hlogDir, HLOG_FILE_PREFIX + (NUM_WRITERS - 1));
    generateHLogs(-1);
    corruptHLog(c1, Corruptions.INSERT_GARBAGE_IN_THE_MIDDLE, false, fs);
    corruptHLog(c2, Corruptions.APPEND_GARBAGE, true, fs);
    corruptHLog(c3, Corruptions.INSERT_GARBAGE_ON_FIRST_LINE, true, fs);

    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    FileStatus[] archivedLogs = fs.listStatus(corruptDir);

    assertEquals("expected a different file", c1.getName(), archivedLogs[0].getPath().getName());
    assertEquals("expected a different file", c2.getName(), archivedLogs[1].getPath().getName());
    assertEquals("expected a different file", c3.getName(), archivedLogs[2].getPath().getName());
    assertEquals(archivedLogs.length, 3);

  }

  @Test
  public void testEOFisIgnored() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);

    final String REGION = "region__1";
    regions.removeAll(regions);
    regions.add(REGION);

    int entryCount = 10;
    Path c1 = new Path(hlogDir, HLOG_FILE_PREFIX + "0");
    generateHLogs(1, entryCount, -1);
    corruptHLog(c1, Corruptions.TRUNCATE, true, fs);

    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    Path originalLog = (fs.listStatus(oldLogDir))[0].getPath();
    Path splitLog = getLogForRegion(hbaseDir, TABLE_NAME, REGION);

    int actualCount = 0;
    HLog.Reader in = HLog.getReader(fs, splitLog, conf);
    HLog.Entry entry;
    while ((entry = in.next()) != null) ++actualCount;
    assertEquals(entryCount-1, actualCount);

    // should not have stored the EOF files as corrupt
    FileStatus[] archivedLogs = fs.listStatus(corruptDir);
    assertEquals(archivedLogs.length, 0);
  }

  @Test
  public void testLogsGetArchivedAfterSplit() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);

    generateHLogs(-1);

    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    FileStatus[] archivedLogs = fs.listStatus(oldLogDir);
    archivedLogs = fs.listStatus(archivedLogs[0].getPath());

    assertEquals("wrong number of files in the archive log", NUM_WRITERS, archivedLogs.length);
  }



  // TODO: fix this test (HBASE-2935)
  //@Test(expected = IOException.class)
  public void testTrailingGarbageCorruptionLogFileSkipErrorsFalseThrows() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    generateHLogs(Integer.MAX_VALUE);
    corruptHLog(new Path(hlogDir, HLOG_FILE_PREFIX + "5"),
            Corruptions.APPEND_GARBAGE, true, fs);

    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);
  }

  // TODO: fix this test (HBASE-2935)
  //@Test
  public void testCorruptedLogFilesSkipErrorsFalseDoesNotTouchLogs() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, false);
    generateHLogs(-1);
    corruptHLog(new Path(hlogDir, HLOG_FILE_PREFIX + "5"),
            Corruptions.APPEND_GARBAGE, true, fs);
    fs.initialize(fs.getUri(), conf);
    try {
      HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);
    } catch (IOException e) {/* expected */}

    assertEquals("if skip.errors is false all files should remain in place",
            NUM_WRITERS, fs.listStatus(hlogDir).length);
  }


  @Test
  public void testSplit() throws IOException {
    generateHLogs(-1);
    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);



    for (String region : regions) {
      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, region);
      assertEquals(NUM_WRITERS * ENTRIES, countHLog(logfile, fs, conf));

    }
  }

  @Test
  public void testLogDirectoryShouldBeDeletedAfterSuccessfulSplit()
  throws IOException {
    generateHLogs(-1);
    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);
    FileStatus [] statuses = null;
    try {
      statuses = fs.listStatus(hlogDir);
      assertTrue(statuses == null || statuses.length == 0);
    } catch (FileNotFoundException e) {
      // hadoop 0.21 throws FNFE whereas hadoop 0.20 returns null
    }
  }
/* DISABLED for now.  TODO: HBASE-2645
  @Test
  public void testLogCannotBeWrittenOnceParsed() throws IOException {
    AtomicLong counter = new AtomicLong(0);
    AtomicBoolean stop = new AtomicBoolean(false);
    generateHLogs(9);
    fs.initialize(fs.getUri(), conf);

    Thread zombie = new ZombieLastLogWriterRegionServer(writer[9], counter, stop);



    try {
      zombie.start();

      HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

      Path logfile = getLogForRegion(hbaseDir, TABLE_NAME, "juliet");

      // It's possible that the writer got an error while appending and didn't count it
      // however the entry will in fact be written to file and split with the rest
      long numberOfEditsInRegion = countHLog(logfile, fs, conf);
      assertTrue("The log file could have at most 1 extra log entry, but " +
              "can't have less. Zombie could write "+counter.get() +" and logfile had only"+ numberOfEditsInRegion+" "  + logfile, counter.get() == numberOfEditsInRegion ||
                      counter.get() + 1 == numberOfEditsInRegion);
    } finally {
      stop.set(true);
    }
  }
*/

  @Test
  public void testSplitWillNotTouchLogsIfNewHLogGetsCreatedAfterSplitStarted()
  throws IOException {
    AtomicBoolean stop = new AtomicBoolean(false);
    generateHLogs(-1);
    fs.initialize(fs.getUri(), conf);
    Thread zombie = new ZombieNewLogWriterRegionServer(stop);

    try {
      zombie.start();
      try {
        HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);
      } catch (IOException ex) {/* expected */}
      int logFilesNumber = fs.listStatus(hlogDir).length;

      assertEquals("Log files should not be archived if there's an extra file after split",
              NUM_WRITERS + 1, logFilesNumber);
    } finally {
      stop.set(true);
    }

  }



  @Test(expected = IOException.class)
  public void testSplitWillFailIfWritingToRegionFails() throws Exception {
    //leave 5th log open so we could append the "trap"
    generateHLogs(4);

    fs.initialize(fs.getUri(), conf);

    InstrumentedSequenceFileLogWriter.activateFailure = false;
    appendEntry(writer[4], TABLE_NAME, Bytes.toBytes("break"), ("r" + 999).getBytes(), FAMILY, QUALIFIER, VALUE, 0);
    writer[4].close();


    try {
      InstrumentedSequenceFileLogWriter.activateFailure = true;
      HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    } catch (IOException e) {
      assertEquals("java.io.IOException: This exception is instrumented and should only be thrown for testing", e.getMessage());
      throw e;
    } finally {
      InstrumentedSequenceFileLogWriter.activateFailure = false;
    }
  }


//  @Test
  public void testSplittingLargeNumberOfRegionsConsistency() throws IOException {

    regions.removeAll(regions);
    for (int i=0; i<100; i++) {
      regions.add("region__"+i);
    }

    generateHLogs(1, 100, -1);
    fs.initialize(fs.getUri(), conf);

    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);
    fs.rename(oldLogDir, hlogDir);
    Path firstSplitPath = new Path(hbaseDir, Bytes.toString(TABLE_NAME) + ".first");
    Path splitPath = new Path(hbaseDir, Bytes.toString(TABLE_NAME));
    fs.rename(splitPath,
            firstSplitPath);


    fs.initialize(fs.getUri(), conf);
    HLog.splitLog(hbaseDir, hlogDir, oldLogDir, fs, conf);

    assertEquals(0, compareHLogSplitDirs(firstSplitPath, splitPath));
  }

  /* HBASE-2312: tests the case where a RegionServer enters a GC pause or goes
   * renegade due to Connect/ZK bugs.  When the master declares it dead, HDFS
   * should deny writes and prevent it from rolling HLogs so Log Splitting can
   * safely commence on the master.
   * */
  @Test
  public void testLogRollAfterSplitStart() throws IOException,
      InterruptedException, ExecutionException {
    // set flush interval to a large number so it doesn't interrupt us
    final String F_INTERVAL = "hbase.regionserver.optionallogflushinterval";
    long oldFlushInterval = conf.getLong(F_INTERVAL, 1000);
    conf.setLong(F_INTERVAL, 1000*1000*100);
    HLog log = null;
    Path thisTestsDir = new Path(hbaseDir, "testLogRollAfterSplitStart");
    Path rsSplitDir = new Path(thisTestsDir.getParent(),
                               thisTestsDir.getName()
                               + HConstants.HLOG_SPLITTING_EXT);

    try {
      // put some entries in an HLog
      byte [] tableName = Bytes.toBytes(this.getClass().getName());
      HRegionInfo regioninfo = new HRegionInfo(new HTableDescriptor(tableName),
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false);
      log = new HLog(fs, thisTestsDir, oldLogDir, conf, null);
      final int total = 20;
      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        kvs.add(new KeyValue(Bytes.toBytes(i), tableName, tableName));
        log.append(regioninfo, tableName, kvs, System.currentTimeMillis()).get();
      }
      // Send the data to HDFS datanodes and close the HDFS writer
      log.sync(true);
      log.cleanupCurrentWriter(log.getFilenum());

      /* code taken from ProcessServerShutdown.process()
       * handles RS shutdowns (as observed by the Master)
       */
      // rename the directory so a rogue RS doesn't create more HLogs
      fs.rename(thisTestsDir, rsSplitDir);
      LOG.debug("Renamed region directory: " + rsSplitDir);

      // Process the old log files
      // TODO: find a way a keep the log.writer around and call this
      //       currently, you look like the current leaseholder
      HLog.splitLog(hbaseDir, rsSplitDir, oldLogDir, fs, conf);

      // Now, try to write more data.
      // verify that this fails and the subsequent roll of the HLog also fails
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
      conf.setLong(F_INTERVAL, oldFlushInterval);
      if (log != null) {
        log.close();
      }
      if (fs.exists(thisTestsDir)) {
        fs.delete(thisTestsDir, true);
      }
      if (fs.exists(rsSplitDir)) {
        fs.delete(rsSplitDir, true);
      }
    }
  }

  /**
   * This thread will keep writing to the file after the split process has started
   * It simulates a region server that was considered dead but woke up and wrote
   * some more to he last log entry
   */
  class ZombieLastLogWriterRegionServer extends Thread {
    AtomicLong editsCount;
    AtomicBoolean stop;
    Path log;
    HLog.Writer lastLogWriter;
    public ZombieLastLogWriterRegionServer(HLog.Writer writer, AtomicLong counter, AtomicBoolean stop) {
      this.stop = stop;
      this.editsCount = counter;
      this.lastLogWriter = writer;
    }

    @Override
    public void run() {
      if (stop.get()){
        return;
      }
      flushToConsole("starting");
      while (true) {
        try {

          appendEntry(lastLogWriter, TABLE_NAME, "juliet".getBytes(),
                  ("r" + editsCount).getBytes(), FAMILY, QUALIFIER, VALUE, 0);
          lastLogWriter.sync();
          editsCount.incrementAndGet();
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            //
          }


        } catch (IOException ex) {
          if (ex instanceof RemoteException) {
            flushToConsole("Juliet: got RemoteException " +
                    ex.getMessage() + " while writing " + (editsCount.get() + 1));
            break;
          } else {
            assertTrue("Failed to write " + editsCount.get(), false);
          }

        }
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
    public ZombieNewLogWriterRegionServer(AtomicBoolean stop) {
      super("ZombieNewLogWriterRegionServer");
      this.stop = stop;
    }

    @Override
    public void run() {
      if (stop.get()) {
        return;
      }
      boolean splitStarted = false;
      Path p = new Path(hbaseDir, new String(TABLE_NAME));
      while (!splitStarted) {
        try {
          FileStatus [] statuses = fs.listStatus(p);
          // In 0.20, listStatus comes back with a null if file doesn't exit.
          // In 0.21, it throws FNFE.
          if (statuses != null && statuses.length > 0) {
            // Done.
            break;
          }
        } catch (FileNotFoundException e) {
          // Expected in hadoop 0.21
        } catch (IOException e1) {
          assertTrue("Failed to list status ", false);
        }
        flushToConsole("Juliet: split not started, sleeping a bit...");
        Threads.sleep(10);
      }

      Path julietLog = new Path(hlogDir, HLOG_FILE_PREFIX + ".juliet");
      try {
        HLog.Writer writer = HLog.createWriter(fs,
                julietLog, conf);
        appendEntry(writer, "juliet".getBytes(), ("juliet").getBytes(),
                ("r").getBytes(), FAMILY, QUALIFIER, VALUE, 0);
        writer.close();
        flushToConsole("Juliet file creator: created file " + julietLog);
      } catch (IOException e1) {
        assertTrue("Failed to create file " + julietLog, false);
      }
    }
  }

  private CancelableProgressable reporter = new CancelableProgressable() {
    int count = 0;

    @Override
    public boolean progress() {
      count++;
      LOG.debug("progress = " + count);
      return true;
    }
  };

  @Test
  public void testSplitLogFileWithOneRegion() throws IOException {
    LOG.info("testSplitLogFileWithOneRegion");
    final String REGION = "region__1";
    regions.removeAll(regions);
    regions.add(REGION);

    generateHLogs(1, 10, -1);
    FileStatus logfile = fs.listStatus(hlogDir)[0];
    fs.initialize(fs.getUri(), conf);
    HLogSplitter.splitLogFileToTemp(hbaseDir, "tmpdir", logfile, fs, conf,
        reporter, logCloseThreadPool, null);
    HLogSplitter.moveRecoveredEditsFromTemp("tmpdir", hbaseDir, oldLogDir,
        logfile.getPath().toString(), conf);

    Path originalLog = (fs.listStatus(oldLogDir))[0].getPath();
    originalLog = (fs.listStatus(originalLog))[0].getPath();

    Path splitLog = getLogForRegion(hbaseDir, TABLE_NAME, REGION);

    assertEquals(true, logsAreEqual(originalLog, splitLog));
  }

  @Test
  public void testSplitLogFileDeletedRegionDir() throws IOException {
    LOG.info("testSplitLogFileDeletedRegionDir");
    final String REGION = "region__1";
    regions.removeAll(regions);
    regions.add(REGION);

    generateHLogs(1, 10, -1);
    FileStatus logfile = fs.listStatus(hlogDir)[0];
    fs.initialize(fs.getUri(), conf);

    Path regiondir = new Path(tabledir, REGION);
    LOG.info("Region directory is" + regiondir);
    fs.delete(regiondir, true);

    HLogSplitter.splitLogFileToTemp(hbaseDir, "tmpdir", logfile, fs, conf,
        reporter, logCloseThreadPool, null);
    HLogSplitter.moveRecoveredEditsFromTemp("tmpdir", hbaseDir, oldLogDir,
        logfile.getPath().toString(), conf);
    // This test passes if there are no exceptions when
    // the region directory has been removed

    assertTrue(!fs.exists(regiondir));
  }

  @Test
  public void testSplitLogFileEmpty() throws IOException {
    LOG.info("testSplitLogFileEmpty");
    injectEmptyFile(".empty", true);
    FileStatus logfile = fs.listStatus(hlogDir)[0];

    fs.initialize(fs.getUri(), conf);

    HLogSplitter.splitLogFileToTemp(hbaseDir, "tmpdir", logfile, fs, conf,
        reporter, logCloseThreadPool, null);
    HLogSplitter.moveRecoveredEditsFromTemp("tmpdir", hbaseDir, oldLogDir,
        logfile.getPath().toString(), conf);
    Path tdir = HTableDescriptor.getTableDir(hbaseDir, TABLE_NAME);
    FileStatus[] files = this.fs.listStatus(tdir);
    assertTrue(files == null || files.length == 0);

    assertEquals(0, countHLog(fs.listStatus(oldLogDir)[0].getPath(), fs, conf));
  }

  @Test
  public void testSplitLogFileMultipleRegions() throws IOException {
    LOG.info("testSplitLogFileMultipleRegions");
    generateHLogs(1, 10, -1);
    FileStatus logfile = fs.listStatus(hlogDir)[0];
    fs.initialize(fs.getUri(), conf);

    HLogSplitter.splitLogFileToTemp(hbaseDir, "tmpdir", logfile, fs, conf,
        reporter, logCloseThreadPool, null);
    HLogSplitter.moveRecoveredEditsFromTemp("tmpdir", hbaseDir, oldLogDir,
        logfile.getPath().toString(), conf);
    for (String region : regions) {
      Path recovered = getLogForRegion(hbaseDir, TABLE_NAME, region);
      assertEquals(10, countHLog(recovered, fs, conf));
    }
  }

  @Test
  public void testSplitLogFileFirstLineCorruptionLog() throws IOException {
    conf.setBoolean(HBASE_SKIP_ERRORS, true);
    generateHLogs(1, 10, -1);
    FileStatus logfile = fs.listStatus(hlogDir)[0];

    corruptHLog(logfile.getPath(), Corruptions.INSERT_GARBAGE_ON_FIRST_LINE,
        true, fs);

    fs.initialize(fs.getUri(), conf);
    HLogSplitter.splitLogFileToTemp(hbaseDir, "tmpdir", logfile, fs, conf,
        reporter, logCloseThreadPool, null);
    HLogSplitter.moveRecoveredEditsFromTemp("tmpdir", hbaseDir, oldLogDir,
        logfile.getPath().toString(), conf);

    final Path corruptDir = new Path(conf.get(HConstants.HBASE_DIR), conf.get(
        "hbase.regionserver.hlog.splitlog.corrupt.dir", ".corrupt"));
    assertEquals(1, fs.listStatus(corruptDir).length);
  }

  @Test
  public void testUnableToCloseSplitFile() throws IOException {
    regions.clear();
    String region = "testUnableToCloseSplitFile";
    regions.add(region);
    generateHLogs(-1);

    fs.initialize(fs.getUri(), conf);
    FileStatus logfile = fs.listStatus(hlogDir)[0];
    InstrumentedSequenceFileLogWriter.activateCloseIOE = true;
    // Log split should fail because writer cannot be closed
    boolean hasIOE = false;
    try {
      HLogSplitter.splitLogFileToTemp(hbaseDir, "tmpdir", logfile, fs, conf,
          reporter, logCloseThreadPool, null);
    } catch (IOException ioe) {
      LOG.debug("Expected IOE because the log cannot be closed");
      hasIOE = true;
    }
    assertTrue(hasIOE);

    InstrumentedSequenceFileLogWriter.activateCloseIOE = false;
    // Log split succeeds this time
    assertTrue(HLogSplitter.splitLogFileToTemp(hbaseDir, "tmpdir", logfile, fs, conf,
        reporter, logCloseThreadPool, null));

    HLogSplitter.moveRecoveredEditsFromTemp("tmpdir", hbaseDir, oldLogDir,
        logfile.getPath().toString(), conf);

    Path originalLog = (fs.listStatus(oldLogDir))[0].getPath();
    originalLog = (fs.listStatus(originalLog))[0].getPath();
    Path splitLog = getLogForRegion(hbaseDir, TABLE_NAME, region);

    LOG.debug("Original log path " + originalLog + " , split log path " + splitLog);
    assertTrue(logsAreEqual(originalLog, splitLog));
  }

  private void flushToConsole(String s) {
    System.out.println(s);
    System.out.flush();
  }


  private void generateHLogs(int leaveOpen) throws IOException {
    generateHLogs(NUM_WRITERS, ENTRIES, leaveOpen);
  }

  private void generateHLogs(int writers, int entries, int leaveOpen) throws IOException {
    for (int i = 0; i < writers; i++) {
      writer[i] = HLog.createWriter(fs, new Path(hlogDir, HLOG_FILE_PREFIX + i), conf);
      for (int j = 0; j < entries; j++) {
        int prefix = 0;
        for (String region : regions) {
          String row_key = region + prefix++ + i + j;
          appendEntry(writer[i], TABLE_NAME, region.getBytes(),
                  row_key.getBytes(), FAMILY, QUALIFIER, VALUE, seq);
        }
      }
      if (i != leaveOpen) {
        writer[i].close();
        flushToConsole("Closing writer " + i);
      }
    }
  }

  private Path getLogForRegion(Path rootdir, byte[] table, String region)
  throws IOException {
    Path tdir = HTableDescriptor.getTableDir(rootdir, table);
    Path editsdir = HLog.getRegionDirRecoveredEditsDir(HRegion.getRegionDir(tdir,
      HRegionInfo.encodeRegionName(region.getBytes())));
    FileStatus [] files = this.fs.listStatus(editsdir);
    assertEquals(1, files.length);
    return files[0].getPath();
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
        out = fs.append(path);
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
        out.write(corrupted_bytes, 0, fileSize-32);
        closeOrFlush(close, out);

        break;
    }


  }

  private void closeOrFlush(boolean close, FSDataOutputStream out)
  throws IOException {
    if (close) {
      out.close();
    } else {
      out.sync();
      // Not in 0out.hflush();
    }
  }

  @SuppressWarnings("unused")
  private void dumpHLog(Path log, FileSystem fs, Configuration conf) throws IOException {
    HLog.Entry entry;
    HLog.Reader in = HLog.getReader(fs, log, conf);
    while ((entry = in.next()) != null) {
      System.out.println(entry);
    }
  }

  private int countHLog(Path log, FileSystem fs, Configuration conf) throws IOException {
    int count = 0;
    HLog.Reader in = HLog.getReader(fs, log, conf);
    while (in.next() != null) {
      count++;
    }
    return count;
  }


  public long appendEntry(HLog.Writer writer, byte[] table, byte[] region,
                          byte[] row, byte[] family, byte[] qualifier,
                          byte[] value, long seq)
          throws IOException {

    long time = System.nanoTime();
    WALEdit edit = new WALEdit();
    seq++;
    edit.add(new KeyValue(row, family, qualifier, time, KeyValue.Type.Put, value));
    writer.append(new HLog.Entry(new HLogKey(region, table, seq, time), edit));
    writer.sync();
    return seq;

  }


  private void injectEmptyFile(String suffix, boolean closeFile)
          throws IOException {
    HLog.Writer writer = HLog.createWriter(
            fs, new Path(hlogDir, HLOG_FILE_PREFIX + suffix), conf);
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

    for (int i=0; i<f1.length; i++) {
      // Regions now have a directory named RECOVERED_EDITS_DIR and in here
      // are split edit files.  In below presume only 1.
      Path rd1 = HLog.getRegionDirRecoveredEditsDir(f1[i].getPath());
      FileStatus [] rd1fs = fs.listStatus(rd1);
      assertEquals(1, rd1fs.length);
      Path rd2 = HLog.getRegionDirRecoveredEditsDir(f2[i].getPath());
      FileStatus [] rd2fs = fs.listStatus(rd2);
      assertEquals(1, rd2fs.length);
      if (!logsAreEqual(rd1fs[0].getPath(), rd2fs[0].getPath())) {
        return -1;
      }
    }
    return 0;
  }

  private boolean logsAreEqual(Path p1, Path p2) throws IOException {
    HLog.Reader in1, in2;
    in1 = HLog.getReader(fs, p1, conf);
    in2 = HLog.getReader(fs, p2, conf);
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
