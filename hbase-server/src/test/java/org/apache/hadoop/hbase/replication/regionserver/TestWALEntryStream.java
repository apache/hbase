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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestWALEntryStream {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWALEntryStream.class);

  private static HBaseTestingUtility TEST_UTIL;
  private static Configuration CONF;
  private static FileSystem fs;
  private static MiniDFSCluster cluster;
  private static final TableName tableName = TableName.valueOf("tablename");
  private static final byte[] family = Bytes.toBytes("column");
  private static final byte[] qualifier = Bytes.toBytes("qualifier");
  private static final RegionInfo info = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(HConstants.EMPTY_START_ROW).setEndKey(HConstants.LAST_ROW).build();
  private static final NavigableMap<byte[], Integer> scopes = getScopes();

  private static NavigableMap<byte[], Integer> getScopes() {
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    scopes.put(family, 1);
    return scopes;
  }

  private WAL log;
  PriorityBlockingQueue<Path> walQueue;
  private PathWatcher pathWatcher;

  @Rule
  public TestName tn = new TestName();
  private final MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    CONF = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniDFSCluster(3);

    cluster = TEST_UTIL.getDFSCluster();
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    walQueue = new PriorityBlockingQueue<>();
    pathWatcher = new PathWatcher();
    final WALFactory wals = new WALFactory(CONF, tn.getMethodName());
    wals.getWALProvider().addWALActionsListener(pathWatcher);
    log = wals.getWAL(info);
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
            appendToLogAndSync(walEditKVs);
          }

          log.rollWriter();

          try (WALEntryStream entryStream =
              new WALEntryStream(walQueue, fs, CONF, 0, log, null, new MetricsSource("1"))) {
            int i = 0;
            while (entryStream.hasNext()) {
              assertNotNull(entryStream.next());
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
    appendToLogAndSync();
    long oldPos;
    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, CONF, 0, log, null, new MetricsSource("1"))) {
      // There's one edit in the log, read it. Reading past it needs to throw exception
      assertTrue(entryStream.hasNext());
      WAL.Entry entry = entryStream.peek();
      assertSame(entry, entryStream.next());
      assertNotNull(entry);
      assertFalse(entryStream.hasNext());
      assertNull(entryStream.peek());
      assertNull(entryStream.next());
      oldPos = entryStream.getPosition();
    }

    appendToLogAndSync();

    try (WALEntryStream entryStream = new WALEntryStream(walQueue, fs, CONF, oldPos,
        log, null, new MetricsSource("1"))) {
      // Read the newly added entry, make sure we made progress
      WAL.Entry entry = entryStream.next();
      assertNotEquals(oldPos, entryStream.getPosition());
      assertNotNull(entry);
      oldPos = entryStream.getPosition();
    }

    // We rolled but we still should see the end of the first log and get that item
    appendToLogAndSync();
    log.rollWriter();
    appendToLogAndSync();

    try (WALEntryStream entryStream = new WALEntryStream(walQueue, fs, CONF, oldPos,
        log, null, new MetricsSource("1"))) {
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
        new WALEntryStream(walQueue, fs, CONF, 0, log, null, new MetricsSource("1"))) {
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
        new WALEntryStream(walQueue, fs, CONF, 0, log, null, new MetricsSource("1"))) {
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
        new WALEntryStream(walQueue, fs, CONF, 0, log, null, new MetricsSource("1"))) {
      entryStream.next(); // we've hit the end of the stream at this point
      appendToLog("2");
      appendToLog("3");
      lastPosition = entryStream.getPosition();
    }
    // next stream should picks up where we left off
    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, CONF, lastPosition, log, null, new MetricsSource("1"))) {
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
    appendEntriesToLogAndSync(3);
    // read only one element
    try (WALEntryStream entryStream = new WALEntryStream(walQueue, fs, CONF, lastPosition,
        log, null, new MetricsSource("1"))) {
      entryStream.next();
      lastPosition = entryStream.getPosition();
    }
    // there should still be two more entries from where we left off
    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, CONF, lastPosition, log, null, new MetricsSource("1"))) {
      assertNotNull(entryStream.next());
      assertNotNull(entryStream.next());
      assertFalse(entryStream.hasNext());
    }
  }


  @Test
  public void testEmptyStream() throws Exception {
    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, CONF, 0, log, null, new MetricsSource("1"))) {
      assertFalse(entryStream.hasNext());
    }
  }

  private ReplicationSource mockReplicationSource(boolean recovered, Configuration conf) {
    ReplicationSourceManager mockSourceManager = Mockito.mock(ReplicationSourceManager.class);
    when(mockSourceManager.getTotalBufferUsed()).thenReturn(new AtomicLong(0));
    Server mockServer = Mockito.mock(Server.class);
    ReplicationSource source = Mockito.mock(ReplicationSource.class);
    when(source.getSourceManager()).thenReturn(mockSourceManager);
    when(source.getSourceMetrics()).thenReturn(new MetricsSource("1"));
    when(source.getWALFileLengthProvider()).thenReturn(log);
    when(source.getServer()).thenReturn(mockServer);
    when(source.isRecovered()).thenReturn(recovered);
    return source;
  }

  private ReplicationSourceWALReader createReader(boolean recovered, Configuration conf) {
    ReplicationSource source = mockReplicationSource(recovered, conf);
    when(source.isPeerEnabled()).thenReturn(true);
    ReplicationSourceWALReader reader =
      new ReplicationSourceWALReader(fs, conf, walQueue, 0, getDummyFilter(), source);
    reader.start();
    return reader;
  }

  @Test
  public void testReplicationSourceWALReader() throws Exception {
    appendEntriesToLogAndSync(3);
    // get ending position
    long position;
    try (WALEntryStream entryStream =
        new WALEntryStream(walQueue, fs, CONF, 0, log, null, new MetricsSource("1"))) {
      entryStream.next();
      entryStream.next();
      entryStream.next();
      position = entryStream.getPosition();
    }

    // start up a reader
    Path walPath = walQueue.peek();
    ReplicationSourceWALReader reader = createReader(false, CONF);
    WALEntryBatch entryBatch = reader.take();

    // should've batched up our entries
    assertNotNull(entryBatch);
    assertEquals(3, entryBatch.getWalEntries().size());
    assertEquals(position, entryBatch.getLastWalPosition());
    assertEquals(walPath, entryBatch.getLastWalPath());
    assertEquals(3, entryBatch.getNbRowKeys());

    appendToLog("foo");
    entryBatch = reader.take();
    assertEquals(1, entryBatch.getNbEntries());
    assertEquals("foo", getRow(entryBatch.getWalEntries().get(0)));
  }

  @Test
  public void testReplicationSourceWALReaderRecovered() throws Exception {
    appendEntriesToLogAndSync(10);
    Path walPath = walQueue.peek();
    log.rollWriter();
    appendEntriesToLogAndSync(5);
    log.shutdown();

    Configuration conf = new Configuration(CONF);
    conf.setInt("replication.source.nb.capacity", 10);

    ReplicationSourceWALReader reader = createReader(true, conf);

    WALEntryBatch batch = reader.take();
    assertEquals(walPath, batch.getLastWalPath());
    assertEquals(10, batch.getNbEntries());
    assertFalse(batch.isEndOfFile());

    batch = reader.take();
    assertEquals(walPath, batch.getLastWalPath());
    assertEquals(0, batch.getNbEntries());
    assertTrue(batch.isEndOfFile());

    walPath = walQueue.peek();
    batch = reader.take();
    assertEquals(walPath, batch.getLastWalPath());
    assertEquals(5, batch.getNbEntries());
    assertTrue(batch.isEndOfFile());

    assertSame(WALEntryBatch.NO_MORE_DATA, reader.take());
  }

  // Testcase for HBASE-20206
  @Test
  public void testReplicationSourceWALReaderWrongPosition() throws Exception {
    appendEntriesToLogAndSync(1);
    Path walPath = walQueue.peek();
    log.rollWriter();
    appendEntriesToLogAndSync(20);
    TEST_UTIL.waitFor(5000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return fs.getFileStatus(walPath).getLen() > 0;
      }

      @Override
      public String explainFailure() throws Exception {
        return walPath + " has not been closed yet";
      }

    });
    long walLength = fs.getFileStatus(walPath).getLen();

    ReplicationSourceWALReader reader = createReader(false, CONF);

    WALEntryBatch entryBatch = reader.take();
    assertEquals(walPath, entryBatch.getLastWalPath());
    assertTrue("Position " + entryBatch.getLastWalPosition() + " is out of range, file length is " +
      walLength, entryBatch.getLastWalPosition() <= walLength);
    assertEquals(1, entryBatch.getNbEntries());
    assertTrue(entryBatch.isEndOfFile());

    Path walPath2 = walQueue.peek();
    entryBatch = reader.take();
    assertEquals(walPath2, entryBatch.getLastWalPath());
    assertEquals(20, entryBatch.getNbEntries());
    assertFalse(entryBatch.isEndOfFile());

    log.rollWriter();
    appendEntriesToLogAndSync(10);
    entryBatch = reader.take();
    assertEquals(walPath2, entryBatch.getLastWalPath());
    assertEquals(0, entryBatch.getNbEntries());
    assertTrue(entryBatch.isEndOfFile());

    Path walPath3 = walQueue.peek();
    entryBatch = reader.take();
    assertEquals(walPath3, entryBatch.getLastWalPath());
    assertEquals(10, entryBatch.getNbEntries());
    assertFalse(entryBatch.isEndOfFile());
  }

  @Test
  public void testReplicationSourceWALReaderDisabled()
      throws IOException, InterruptedException, ExecutionException {
    appendEntriesToLogAndSync(3);
    // get ending position
    long position;
    try (WALEntryStream entryStream =
      new WALEntryStream(walQueue, fs, CONF, 0, log, null, new MetricsSource("1"))) {
      entryStream.next();
      entryStream.next();
      entryStream.next();
      position = entryStream.getPosition();
    }

    // start up a reader
    Path walPath = walQueue.peek();
    ReplicationSource source = mockReplicationSource(false, CONF);
    AtomicInteger invokeCount = new AtomicInteger(0);
    AtomicBoolean enabled = new AtomicBoolean(false);
    when(source.isPeerEnabled()).then(i -> {
      invokeCount.incrementAndGet();
      return enabled.get();
    });

    ReplicationSourceWALReader reader =
      new ReplicationSourceWALReader(fs, CONF, walQueue, 0, getDummyFilter(), source);
    reader.start();
    Future<WALEntryBatch> future = ForkJoinPool.commonPool().submit(() -> {
      return reader.take();
    });
    // make sure that the isPeerEnabled has been called several times
    TEST_UTIL.waitFor(30000, () -> invokeCount.get() >= 5);
    // confirm that we can read nothing if the peer is disabled
    assertFalse(future.isDone());
    // then enable the peer, we should get the batch
    enabled.set(true);
    WALEntryBatch entryBatch = future.get();

    // should've batched up our entries
    assertNotNull(entryBatch);
    assertEquals(3, entryBatch.getWalEntries().size());
    assertEquals(position, entryBatch.getLastWalPosition());
    assertEquals(walPath, entryBatch.getLastWalPath());
    assertEquals(3, entryBatch.getNbRowKeys());
  }

  private String getRow(WAL.Entry entry) {
    Cell cell = entry.getEdit().getCells().get(0);
    return Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }

  private void appendToLog(String key) throws IOException {
    final long txid = log.append(info,
      new WALKeyImpl(info.getEncodedNameAsBytes(), tableName, System.currentTimeMillis(),
          mvcc, scopes), getWALEdit(key), true);
    log.sync(txid);
  }

  private void appendEntriesToLogAndSync(int count) throws IOException {
    long txid = -1L;
    for (int i = 0; i < count; i++) {
      txid = appendToLog(1);
    }
    log.sync(txid);
  }

  private void appendToLogAndSync() throws IOException {
    appendToLogAndSync(1);
  }

  private void appendToLogAndSync(int count) throws IOException {
    long txid = appendToLog(count);
    log.sync(txid);
  }

  private long appendToLog(int count) throws IOException {
    return log.append(info, new WALKeyImpl(info.getEncodedNameAsBytes(), tableName,
      System.currentTimeMillis(), mvcc, scopes), getWALEdits(count), true);
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

  class PathWatcher implements WALActionsListener {

    Path currentPath;

    @Override
    public void preLogRoll(Path oldPath, Path newPath) throws IOException {
      walQueue.add(newPath);
      currentPath = newPath;
    }
  }

  @Test
  public void testReadBeyondCommittedLength() throws IOException, InterruptedException {
    appendToLog("1");
    appendToLog("2");
    long size = log.getLogFileSizeIfBeingWritten(walQueue.peek()).getAsLong();
    AtomicLong fileLength = new AtomicLong(size - 1);
    try (WALEntryStream entryStream = new WALEntryStream(walQueue, fs, CONF, 0,
        p -> OptionalLong.of(fileLength.get()), null, new MetricsSource("1"))) {
      assertTrue(entryStream.hasNext());
      assertNotNull(entryStream.next());
      // can not get log 2
      assertFalse(entryStream.hasNext());
      Thread.sleep(1000);
      entryStream.reset();
      // still can not get log 2
      assertFalse(entryStream.hasNext());

      // can get log 2 now
      fileLength.set(size);
      entryStream.reset();
      assertTrue(entryStream.hasNext());
      assertNotNull(entryStream.next());

      assertFalse(entryStream.hasNext());
    }
  }
}
