/*
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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceWALReaderThread.WALEntryBatch;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@Category({ ReplicationTests.class, LargeTests.class })
public class TestWALEntryStream {

  private static HBaseTestingUtility TEST_UTIL;
  private static Configuration conf;
  private static FileSystem fs;
  private static MiniDFSCluster cluster;
  private static final TableName tableName = TableName.valueOf("tablename");
  private static final byte[] family = Bytes.toBytes("column");
  private static final byte[] qualifier = Bytes.toBytes("qualifier");
  private static final HRegionInfo info =
      new HRegionInfo(tableName, HConstants.EMPTY_START_ROW, HConstants.LAST_ROW, false);
  private static final HTableDescriptor htd = new HTableDescriptor(tableName);
  private static NavigableMap<byte[], Integer> scopes;

  private WAL log;
  PriorityBlockingQueue<Path> walQueue;
  private PathWatcher pathWatcher;

  @Rule
  public TestName tn = new TestName();
  private final MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniDFSCluster(3);

    cluster = TEST_UTIL.getDFSCluster();
    fs = cluster.getFileSystem();
    scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : htd.getFamiliesKeys()) {
      scopes.put(fam, 0);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    walQueue = new PriorityBlockingQueue<>();
    List<WALActionsListener> listeners = new ArrayList<WALActionsListener>();
    pathWatcher = new PathWatcher();
    listeners.add(pathWatcher);
    final WALFactory wals = new WALFactory(conf, listeners, tn.getMethodName());
    log = wals.getWAL(info.getEncodedNameAsBytes(), info.getTable().getNamespace());
  }

  @After
  public void tearDown() throws Exception {
    log.close();
  }

  // Try out different combinations of row count and KeyValue count
  @Test
  public void testDifferentCounts() throws Exception {
    int[] NB_ROWS = { 1500, 60000 };
    int[] NB_KVS = { 1, 100 };
    // whether compression is used
    Boolean[] BOOL_VALS = { false, true };
    // long lastPosition = 0;
    for (int nbRows : NB_ROWS) {
      for (int walEditKVs : NB_KVS) {
        for (boolean isCompressionEnabled : BOOL_VALS) {
          TEST_UTIL.getConfiguration().setBoolean(HConstants.ENABLE_WAL_COMPRESSION,
            isCompressionEnabled);
          mvcc.advanceTo(1);

          for (int i = 0; i < nbRows; i++) {
            appendToLogPlus(walEditKVs);
          }

          log.rollWriter();

          try (WALEntryStream entryStream =
              new WALEntryStream(walQueue, fs, conf, new MetricsSource("1"))) {
            int i = 0;
            for (WAL.Entry e : entryStream) {
              assertNotNull(e);
              i++;
            }
            assertEquals(nbRows, i);

            // should've read all entries
            assertFalse(entryStream.hasNext());
          }
          // reset everything for next loop
          log.close();
          setUp();
        }
      }
    }
  }

  /**
   * Tests basic reading of log appends
   */
  @Test
  public void testAppendsWithRolls() throws Exception {
    appendToLog();

    long oldPos;
    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, conf, new MetricsSource("1"))) {
      // There's one edit in the log, read it. Reading past it needs to throw exception
      assertTrue(entryStream.hasNext());
      WAL.Entry entry = entryStream.next();
      assertNotNull(entry);
      assertFalse(entryStream.hasNext());
      try {
        entry = entryStream.next();
        fail();
      } catch (NoSuchElementException e) {
        // expected
      }
      oldPos = entryStream.getPosition();
    }

    appendToLog();

    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, conf, oldPos, new MetricsSource("1"))) {
      // Read the newly added entry, make sure we made progress
      WAL.Entry entry = entryStream.next();
      assertNotEquals(oldPos, entryStream.getPosition());
      assertNotNull(entry);
      oldPos = entryStream.getPosition();
    }

    // We rolled but we still should see the end of the first log and get that item
    appendToLog();
    log.rollWriter();
    appendToLog();

    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, conf, oldPos, new MetricsSource("1"))) {
      WAL.Entry entry = entryStream.next();
      assertNotEquals(oldPos, entryStream.getPosition());
      assertNotNull(entry);

      // next item should come from the new log
      entry = entryStream.next();
      assertNotEquals(oldPos, entryStream.getPosition());
      assertNotNull(entry);

      // no more entries to read
      assertFalse(entryStream.hasNext());
      oldPos = entryStream.getPosition();
    }
  }

  /**
   * Tests that if after a stream is opened, more entries come in and then the log is rolled, we
   * don't mistakenly dequeue the current log thinking we're done with it
   */
  @Test
  public void testLogrollWhileStreaming() throws Exception {
    appendToLog("1");
    appendToLog("2");// 2
    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, conf, new MetricsSource("1"))) {
      assertEquals("1", getRow(entryStream.next()));

      appendToLog("3"); // 3 - comes in after reader opened
      log.rollWriter(); // log roll happening while we're reading
      appendToLog("4"); // 4 - this append is in the rolled log

      assertEquals("2", getRow(entryStream.next()));
      assertEquals(2, walQueue.size()); // we should not have dequeued yet since there's still an
                                        // entry in first log
      assertEquals("3", getRow(entryStream.next())); // if implemented improperly, this would be 4
                                                     // and 3 would be skipped
      assertEquals("4", getRow(entryStream.next())); // 4
      assertEquals(1, walQueue.size()); // now we've dequeued and moved on to next log properly
      assertFalse(entryStream.hasNext());
    }
  }

  /**
   * Tests that if writes come in while we have a stream open, we shouldn't miss them
   */
  @Test
  public void testNewEntriesWhileStreaming() throws Exception {
    appendToLog("1");
    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, conf, 0, new MetricsSource("1"))) {
      entryStream.next(); // we've hit the end of the stream at this point

      // some new entries come in while we're streaming
      appendToLog("2");
      appendToLog("3");

      // don't see them
      assertFalse(entryStream.hasNext());

      // But we do if we reset
      entryStream.reset();
      assertEquals("2", getRow(entryStream.next()));
      assertEquals("3", getRow(entryStream.next()));
      assertFalse(entryStream.hasNext());
    }
  }

  @Test
  public void testResumeStreamingFromPosition() throws Exception {
    long lastPosition = 0;
    appendToLog("1");
    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, conf, 0, new MetricsSource("1"))) {
      entryStream.next(); // we've hit the end of the stream at this point
      appendToLog("2");
      appendToLog("3");
      lastPosition = entryStream.getPosition();
    }
    // next stream should picks up where we left off
    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, conf, lastPosition, new MetricsSource("1"))) {
      assertEquals("2", getRow(entryStream.next()));
      assertEquals("3", getRow(entryStream.next()));
      assertFalse(entryStream.hasNext()); // done
      assertEquals(1, walQueue.size());
    }
  }

  /**
   * Tests that if we stop before hitting the end of a stream, we can continue where we left off
   * using the last position
   */
  @Test
  public void testPosition() throws Exception {
    long lastPosition = 0;
    appendEntriesToLog(3);
    // read only one element
    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, conf, lastPosition, new MetricsSource("1"))) {
      entryStream.next();
      lastPosition = entryStream.getPosition();
    }
    // there should still be two more entries from where we left off
    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, conf, lastPosition, new MetricsSource("1"))) {
      assertNotNull(entryStream.next());
      assertNotNull(entryStream.next());
      assertFalse(entryStream.hasNext());
    }
  }


  @Test
  public void testEmptyStream() throws Exception {
    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, conf, 0, new MetricsSource("1"))) {
      assertFalse(entryStream.hasNext());
    }
  }

  @Test
  public void testReplicationSourceWALReaderThread() throws Exception {
    appendEntriesToLog(3);
    // get ending position
    long position;
    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, conf, new MetricsSource("1"))) {
      entryStream.next();
      entryStream.next();
      entryStream.next();
      position = entryStream.getPosition();
    }

    // start up a batcher
    ReplicationSourceManager mockSourceManager = Mockito.mock(ReplicationSourceManager.class);
    ReplicationSourceWALReaderThread batcher = new ReplicationSourceWALReaderThread(mockSourceManager, getQueueInfo(),walQueue, 0,
        fs, conf, getDummyFilter(), new MetricsSource("1"));
    Path walPath = walQueue.peek();
    batcher.start();
    WALEntryBatch entryBatch = batcher.take();

    // should've batched up our entries
    assertNotNull(entryBatch);
    assertEquals(3, entryBatch.getWalEntries().size());
    assertEquals(position, entryBatch.getLastWalPosition());
    assertEquals(walPath, entryBatch.getLastWalPath());
    assertEquals(3, entryBatch.getNbRowKeys());

    appendToLog("foo");
    entryBatch = batcher.take();
    assertEquals(1, entryBatch.getNbEntries());
    assertEquals(getRow(entryBatch.getWalEntries().get(0)), "foo");
  }

  private String getRow(WAL.Entry entry) {
    Cell cell = entry.getEdit().getCells().get(0);
    return Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }

  private void appendToLog(String key) throws IOException {
    final long txid = log.append(htd, info,
      new WALKey(info.getEncodedNameAsBytes(), tableName, System.currentTimeMillis(), mvcc),
      getWALEdit(key), true);
    log.sync(txid);
  }

  private void appendEntriesToLog(int count) throws IOException {
    for (int i = 0; i < count; i++) {
      appendToLog();
    }
  }

  private void appendToLog() throws IOException {
    appendToLogPlus(1);
  }

  private void appendToLogPlus(int count) throws IOException {
    final long txid = log.append(htd, info,
      new WALKey(info.getEncodedNameAsBytes(), tableName, System.currentTimeMillis(), mvcc),
      getWALEdits(count), true);
    log.sync(txid);
  }

  private WALEdit getWALEdits(int count) {
    WALEdit edit = new WALEdit();
    for (int i = 0; i < count; i++) {
      edit.add(new KeyValue(Bytes.toBytes(System.currentTimeMillis()), family, qualifier,
          System.currentTimeMillis(), qualifier));
    }
    return edit;
  }

  private WALEdit getWALEdit(String row) {
    WALEdit edit = new WALEdit();
    edit.add(
      new KeyValue(Bytes.toBytes(row), family, qualifier, System.currentTimeMillis(), qualifier));
    return edit;
  }

  private WALEntryFilter getDummyFilter() {
    return new WALEntryFilter() {

      @Override
      public Entry filter(Entry entry) {
        return entry;
      }
    };
  }

  private ReplicationQueueInfo getQueueInfo() {
    return new ReplicationQueueInfo("1");
  }

  class PathWatcher extends WALActionsListener.Base {

    Path currentPath;

    @Override
    public void preLogRoll(Path oldPath, Path newPath) throws IOException {
      walQueue.add(newPath);
      currentPath = newPath;
    }
  }

}
