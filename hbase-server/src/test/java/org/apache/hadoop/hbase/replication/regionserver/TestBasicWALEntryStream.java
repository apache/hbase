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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

public abstract class TestBasicWALEntryStream extends WALEntryStreamTestBase {

  @Before
  public void setUp() throws Exception {
    initWAL();
  }

  /**
   * Tests basic reading of log appends
   */
  @Test
  public void testAppendsWithRolls() throws Exception {
    appendToLogAndSync();
    long oldPos;
    try (WALEntryStream entryStream =
      new WALEntryStream(logQueue, CONF, 0, log, null, new MetricsSource("1"), fakeWalGroupId)) {
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

    try (WALEntryStream entryStream = new WALEntryStreamWithRetries(logQueue, CONF, oldPos, log,
      null, new MetricsSource("1"), fakeWalGroupId)) {
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

    try (WALEntryStream entryStream = new WALEntryStreamWithRetries(logQueue, CONF, oldPos, log,
      null, new MetricsSource("1"), fakeWalGroupId)) {
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
    try (WALEntryStream entryStream = new WALEntryStreamWithRetries(logQueue, CONF, 0, log, null,
      new MetricsSource("1"), fakeWalGroupId)) {
      assertEquals("1", getRow(entryStream.next()));

      appendToLog("3"); // 3 - comes in after reader opened
      log.rollWriter(); // log roll happening while we're reading
      appendToLog("4"); // 4 - this append is in the rolled log

      assertEquals("2", getRow(entryStream.next()));
      assertEquals(2, getQueue().size()); // we should not have dequeued yet since there's still an
      // entry in first log
      assertEquals("3", getRow(entryStream.next())); // if implemented improperly, this would be 4
                                                     // and 3 would be skipped
      assertEquals("4", getRow(entryStream.next())); // 4
      assertEquals(1, getQueue().size()); // now we've dequeued and moved on to next log properly
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
      new WALEntryStream(logQueue, CONF, 0, log, null, new MetricsSource("1"), fakeWalGroupId)) {
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
      new WALEntryStream(logQueue, CONF, 0, log, null, new MetricsSource("1"), fakeWalGroupId)) {
      entryStream.next(); // we've hit the end of the stream at this point
      appendToLog("2");
      appendToLog("3");
      lastPosition = entryStream.getPosition();
    }
    // next stream should picks up where we left off
    try (WALEntryStream entryStream = new WALEntryStream(logQueue, CONF, lastPosition, log, null,
      new MetricsSource("1"), fakeWalGroupId)) {
      assertEquals("2", getRow(entryStream.next()));
      assertEquals("3", getRow(entryStream.next()));
      assertFalse(entryStream.hasNext()); // done
      assertEquals(1, getQueue().size());
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
    try (WALEntryStream entryStream = new WALEntryStream(logQueue, CONF, lastPosition, log, null,
      new MetricsSource("1"), fakeWalGroupId)) {
      entryStream.next();
      lastPosition = entryStream.getPosition();
    }
    // there should still be two more entries from where we left off
    try (WALEntryStream entryStream = new WALEntryStream(logQueue, CONF, lastPosition, log, null,
      new MetricsSource("1"), fakeWalGroupId)) {
      assertNotNull(entryStream.next());
      assertNotNull(entryStream.next());
      assertFalse(entryStream.hasNext());
    }
  }

  @Test
  public void testEmptyStream() throws Exception {
    try (WALEntryStream entryStream =
      new WALEntryStream(logQueue, CONF, 0, log, null, new MetricsSource("1"), fakeWalGroupId)) {
      assertFalse(entryStream.hasNext());
    }
  }

  @Test
  public void testWALKeySerialization() throws Exception {
    Map<String, byte[]> attributes = new HashMap<String, byte[]>();
    attributes.put("foo", Bytes.toBytes("foo-value"));
    attributes.put("bar", Bytes.toBytes("bar-value"));
    WALKeyImpl key =
      new WALKeyImpl(info.getEncodedNameAsBytes(), tableName, EnvironmentEdgeManager.currentTime(),
        new ArrayList<UUID>(), 0L, 0L, mvcc, scopes, attributes);
    Assert.assertEquals(attributes, key.getExtendedAttributes());

    WALProtos.WALKey.Builder builder = key.getBuilder(WALCellCodec.getNoneCompressor());
    WALProtos.WALKey serializedKey = builder.build();

    WALKeyImpl deserializedKey = new WALKeyImpl();
    deserializedKey.readFieldsFromPb(serializedKey, WALCellCodec.getNoneUncompressor());

    // equals() only checks region name, sequence id and write time
    Assert.assertEquals(key, deserializedKey);
    // can't use Map.equals() because byte arrays use reference equality
    Assert.assertEquals(key.getExtendedAttributes().keySet(),
      deserializedKey.getExtendedAttributes().keySet());
    for (Map.Entry<String, byte[]> entry : deserializedKey.getExtendedAttributes().entrySet()) {
      Assert.assertArrayEquals(key.getExtendedAttribute(entry.getKey()), entry.getValue());
    }
    Assert.assertEquals(key.getReplicationScopes(), deserializedKey.getReplicationScopes());
  }

  private ReplicationSource mockReplicationSource(boolean recovered, Configuration conf) {
    ReplicationSourceManager mockSourceManager = Mockito.mock(ReplicationSourceManager.class);
    when(mockSourceManager.getTotalBufferUsed()).thenReturn(new AtomicLong(0));
    when(mockSourceManager.getTotalBufferLimit())
      .thenReturn((long) HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_DFAULT);
    Server mockServer = Mockito.mock(Server.class);
    ReplicationSource source = Mockito.mock(ReplicationSource.class);
    when(source.getSourceManager()).thenReturn(mockSourceManager);
    when(source.getSourceMetrics()).thenReturn(new MetricsSource("1"));
    when(source.getWALFileLengthProvider()).thenReturn(log);
    when(source.getServer()).thenReturn(mockServer);
    when(source.isRecovered()).thenReturn(recovered);
    MetricsReplicationGlobalSourceSource globalMetrics =
      Mockito.mock(MetricsReplicationGlobalSourceSource.class);
    when(mockSourceManager.getGlobalMetrics()).thenReturn(globalMetrics);
    return source;
  }

  private ReplicationSourceWALReader createReader(boolean recovered, Configuration conf) {
    ReplicationSource source = mockReplicationSource(recovered, conf);
    when(source.isPeerEnabled()).thenReturn(true);
    ReplicationSourceWALReader reader = new ReplicationSourceWALReader(fs, conf, logQueue, 0,
      getDummyFilter(), source, fakeWalGroupId);
    reader.start();
    return reader;
  }

  private ReplicationSourceWALReader createReaderWithBadReplicationFilter(int numFailures,
    Configuration conf) {
    ReplicationSource source = mockReplicationSource(false, conf);
    when(source.isPeerEnabled()).thenReturn(true);
    ReplicationSourceWALReader reader = new ReplicationSourceWALReader(fs, conf, logQueue, 0,
      getIntermittentFailingFilter(numFailures), source, fakeWalGroupId);
    reader.start();
    return reader;
  }

  @Test
  public void testReplicationSourceWALReader() throws Exception {
    appendEntriesToLogAndSync(3);
    // get ending position
    long position;
    try (WALEntryStream entryStream =
      new WALEntryStream(logQueue, CONF, 0, log, null, new MetricsSource("1"), fakeWalGroupId)) {
      entryStream.next();
      entryStream.next();
      entryStream.next();
      position = entryStream.getPosition();
    }

    // start up a reader
    Path walPath = getQueue().peek();
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
  public void testReplicationSourceWALReaderWithFailingFilter() throws Exception {
    appendEntriesToLogAndSync(3);
    // get ending position
    long position;
    try (WALEntryStream entryStream =
      new WALEntryStream(logQueue, CONF, 0, log, null, new MetricsSource("1"), fakeWalGroupId)) {
      entryStream.next();
      entryStream.next();
      entryStream.next();
      position = entryStream.getPosition();
    }

    // start up a reader
    Path walPath = getQueue().peek();
    int numFailuresInFilter = 5;
    ReplicationSourceWALReader reader =
      createReaderWithBadReplicationFilter(numFailuresInFilter, CONF);
    WALEntryBatch entryBatch = reader.take();
    assertEquals(numFailuresInFilter, FailingWALEntryFilter.numFailures());

    // should've batched up our entries
    assertNotNull(entryBatch);
    assertEquals(3, entryBatch.getWalEntries().size());
    assertEquals(position, entryBatch.getLastWalPosition());
    assertEquals(walPath, entryBatch.getLastWalPath());
    assertEquals(3, entryBatch.getNbRowKeys());
  }

  @Test
  public void testReplicationSourceWALReaderRecovered() throws Exception {
    appendEntriesToLogAndSync(10);
    Path walPath = getQueue().peek();
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

    walPath = getQueue().peek();
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
    Path walPath = getQueue().peek();
    log.rollWriter();
    appendEntriesToLogAndSync(20);
    TEST_UTIL.waitFor(5000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return fs.getFileStatus(walPath).getLen() > 0 &&
          ((AbstractFSWAL<?>) log).getInflightWALCloseCount() == 0;
      }

      @Override
      public String explainFailure() throws Exception {
        return walPath + " has not been closed yet";
      }

    });

    ReplicationSourceWALReader reader = createReader(false, CONF);

    WALEntryBatch entryBatch = reader.take();
    assertEquals(walPath, entryBatch.getLastWalPath());

    long walLength = fs.getFileStatus(walPath).getLen();
    assertTrue("Position " + entryBatch.getLastWalPosition() + " is out of range, file length is " +
      walLength, entryBatch.getLastWalPosition() <= walLength);
    assertEquals(1, entryBatch.getNbEntries());
    assertTrue(entryBatch.isEndOfFile());

    Path walPath2 = getQueue().peek();
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

    Path walPath3 = getQueue().peek();
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
      new WALEntryStream(logQueue, CONF, 0, log, null, new MetricsSource("1"), fakeWalGroupId)) {
      entryStream.next();
      entryStream.next();
      entryStream.next();
      position = entryStream.getPosition();
    }

    // start up a reader
    Path walPath = getQueue().peek();
    ReplicationSource source = mockReplicationSource(false, CONF);
    AtomicInteger invokeCount = new AtomicInteger(0);
    AtomicBoolean enabled = new AtomicBoolean(false);
    when(source.isPeerEnabled()).then(i -> {
      invokeCount.incrementAndGet();
      return enabled.get();
    });

    ReplicationSourceWALReader reader = new ReplicationSourceWALReader(fs, CONF, logQueue, 0,
      getDummyFilter(), source, fakeWalGroupId);
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
    final long txid = log.appendData(info, new WALKeyImpl(info.getEncodedNameAsBytes(), tableName,
      EnvironmentEdgeManager.currentTime(), mvcc, scopes), getWALEdit(key));
    log.sync(txid);
  }

  private void appendEntriesToLogAndSync(int count) throws IOException {
    long txid = -1L;
    for (int i = 0; i < count; i++) {
      txid = appendToLog(1);
    }
    log.sync(txid);
  }

  private WALEdit getWALEdit(String row) {
    WALEdit edit = new WALEdit();
    edit.add(new KeyValue(Bytes.toBytes(row), family, qualifier,
      EnvironmentEdgeManager.currentTime(), qualifier));
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

  private WALEntryFilter getIntermittentFailingFilter(int numFailuresInFilter) {
    return new FailingWALEntryFilter(numFailuresInFilter);
  }

  public static class FailingWALEntryFilter implements WALEntryFilter {
    private int numFailures = 0;
    private static int countFailures = 0;

    public FailingWALEntryFilter(int numFailuresInFilter) {
      numFailures = numFailuresInFilter;
    }

    @Override
    public Entry filter(Entry entry) {
      if (countFailures == numFailures) {
        return entry;
      }
      countFailures = countFailures + 1;
      throw new WALEntryFilterRetryableException("failing filter");
    }

    public static int numFailures() {
      return countFailures;
    }
  }

  @Test
  public void testReadBeyondCommittedLength() throws IOException, InterruptedException {
    appendToLog("1");
    appendToLog("2");
    long size = log.getLogFileSizeIfBeingWritten(getQueue().peek()).getAsLong();
    AtomicLong fileLength = new AtomicLong(size - 1);
    try (WALEntryStream entryStream = new WALEntryStream(logQueue, CONF, 0,
      p -> OptionalLong.of(fileLength.get()), null, new MetricsSource("1"), fakeWalGroupId)) {
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

  /*
   * Test removal of 0 length log from logQueue if the source is a recovered source and size of
   * logQueue is only 1.
   */
  @Test
  public void testEOFExceptionForRecoveredQueue() throws Exception {
    // Create a 0 length log.
    Path emptyLog = new Path("emptyLog");
    FSDataOutputStream fsdos = fs.create(emptyLog);
    fsdos.close();
    assertEquals(0, fs.getFileStatus(emptyLog).getLen());

    Configuration conf = new Configuration(CONF);
    // Override the max retries multiplier to fail fast.
    conf.setInt("replication.source.maxretriesmultiplier", 1);
    conf.setBoolean("replication.source.eof.autorecovery", true);
    conf.setInt("replication.source.nb.batches", 10);
    // Create a reader thread with source as recovered source.
    ReplicationSource source = mockReplicationSource(true, conf);
    when(source.isPeerEnabled()).thenReturn(true);

    MetricsSource metrics = mock(MetricsSource.class);
    doNothing().when(metrics).incrSizeOfLogQueue();
    doNothing().when(metrics).decrSizeOfLogQueue();
    ReplicationSourceLogQueue localLogQueue = new ReplicationSourceLogQueue(conf, metrics, source);
    localLogQueue.enqueueLog(emptyLog, fakeWalGroupId);
    ReplicationSourceWALReader reader = new ReplicationSourceWALReader(fs, conf, localLogQueue, 0,
      getDummyFilter(), source, fakeWalGroupId);
    reader.run();
    // ReplicationSourceWALReaderThread#handleEofException method will
    // remove empty log from logQueue.
    assertEquals(0, localLogQueue.getQueueSize(fakeWalGroupId));
  }

  @Test
  public void testEOFExceptionForRecoveredQueueWithMultipleLogs() throws Exception {
    Configuration conf = new Configuration(CONF);
    MetricsSource metrics = mock(MetricsSource.class);
    ReplicationSource source = mockReplicationSource(true, conf);
    ReplicationSourceLogQueue localLogQueue = new ReplicationSourceLogQueue(conf, metrics, source);
    // Create a 0 length log.
    Path emptyLog = new Path(fs.getHomeDirectory(), "log.2");
    FSDataOutputStream fsdos = fs.create(emptyLog);
    fsdos.close();
    assertEquals(0, fs.getFileStatus(emptyLog).getLen());
    localLogQueue.enqueueLog(emptyLog, fakeWalGroupId);

    final Path log1 = new Path(fs.getHomeDirectory(), "log.1");
    WALProvider.Writer writer1 = WALFactory.createWALWriter(fs, log1, TEST_UTIL.getConfiguration());
    appendEntries(writer1, 3);
    localLogQueue.enqueueLog(log1, fakeWalGroupId);

    ReplicationSourceManager mockSourceManager = mock(ReplicationSourceManager.class);
    // Make it look like the source is from recovered source.
    when(mockSourceManager.getOldSources())
      .thenReturn(new ArrayList<>(Arrays.asList((ReplicationSourceInterface) source)));
    when(source.isPeerEnabled()).thenReturn(true);
    when(mockSourceManager.getTotalBufferUsed()).thenReturn(new AtomicLong(0));
    // Override the max retries multiplier to fail fast.
    conf.setInt("replication.source.maxretriesmultiplier", 1);
    conf.setBoolean("replication.source.eof.autorecovery", true);
    conf.setInt("replication.source.nb.batches", 10);
    // Create a reader thread.
    ReplicationSourceWALReader reader = new ReplicationSourceWALReader(fs, conf, localLogQueue, 0,
      getDummyFilter(), source, fakeWalGroupId);
    assertEquals("Initial log queue size is not correct", 2,
      localLogQueue.getQueueSize(fakeWalGroupId));
    reader.run();

    // remove empty log from logQueue.
    assertEquals(0, localLogQueue.getQueueSize(fakeWalGroupId));
    assertEquals("Log queue should be empty", 0, localLogQueue.getQueueSize(fakeWalGroupId));
  }

  private PriorityBlockingQueue<Path> getQueue() {
    return logQueue.getQueue(fakeWalGroupId);
  }

  private void appendEntries(WALProvider.Writer writer, int numEntries) throws IOException {
    for (int i = 0; i < numEntries; i++) {
      byte[] b = Bytes.toBytes(Integer.toString(i));
      KeyValue kv = new KeyValue(b, b, b);
      WALEdit edit = new WALEdit();
      edit.add(kv);
      WALKeyImpl key = new WALKeyImpl(b, TableName.valueOf(b), 0, 0, HConstants.DEFAULT_CLUSTER_ID);
      NavigableMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
      scopes.put(b, HConstants.REPLICATION_SCOPE_GLOBAL);
      writer.append(new WAL.Entry(key, edit));
      writer.sync(false);
    }
    writer.close();
  }

  /**
   * Tests size of log queue is incremented and decremented properly.
   */
  @Test
  public void testSizeOfLogQueue() throws Exception {
    // There should be always 1 log which is current wal.
    assertEquals(1, logQueue.getMetrics().getSizeOfLogQueue());
    appendToLogAndSync();

    log.rollWriter();
    // After rolling there will be 2 wals in the queue
    assertEquals(2, logQueue.getMetrics().getSizeOfLogQueue());

    try (WALEntryStream entryStream =
      new WALEntryStream(logQueue, CONF, 0, log, null, logQueue.getMetrics(), fakeWalGroupId)) {
      // There's one edit in the log, read it.
      assertTrue(entryStream.hasNext());
      WAL.Entry entry = entryStream.next();
      assertNotNull(entry);
      assertFalse(entryStream.hasNext());
    }
    // After removing one wal, size of log queue will be 1 again.
    assertEquals(1, logQueue.getMetrics().getSizeOfLogQueue());
  }

  /**
   * Tests that wals are closed cleanly and we read the trailer when we remove wal from
   * WALEntryStream.
   */
  @Test
  public void testCleanClosedWALs() throws Exception {
    try (WALEntryStream entryStream = new WALEntryStreamWithRetries(logQueue, CONF, 0, log, null,
      logQueue.getMetrics(), fakeWalGroupId)) {
      assertEquals(0, logQueue.getMetrics().getUncleanlyClosedWALs());
      appendToLogAndSync();
      assertNotNull(entryStream.next());
      log.rollWriter();
      appendToLogAndSync();
      assertNotNull(entryStream.next());
      assertEquals(0, logQueue.getMetrics().getUncleanlyClosedWALs());
    }
  }

  /**
   * Tests that we handle EOFException properly if the wal has moved to oldWALs directory.
   * @throws Exception exception
   */
  @Test
  public void testEOFExceptionInOldWALsDirectory() throws Exception {
    assertEquals(1, logQueue.getQueueSize(fakeWalGroupId));
    AbstractFSWAL  abstractWAL = (AbstractFSWAL)log;
    Path emptyLogFile = abstractWAL.getCurrentFileName();
    log.rollWriter(true);

    // AsyncFSWAl and FSHLog both moves the log from WALs to oldWALs directory asynchronously.
    // Wait for in flight wal close count to become 0. This makes sure that empty wal is moved to
    // oldWALs directory.
    Waiter.waitFor(CONF, 5000,
      (Waiter.Predicate<Exception>) () -> abstractWAL.getInflightWALCloseCount() == 0);
    // There will 2 logs in the queue.
    assertEquals(2, logQueue.getQueueSize(fakeWalGroupId));

    // Get the archived dir path for the first wal.
    Path archivePath = AbstractFSWALProvider.findArchivedLog(emptyLogFile, CONF);
    // Make sure that the wal path is not the same as archived Dir path.
    assertNotNull(archivePath);
    assertTrue(fs.exists(archivePath));
    fs.truncate(archivePath, 0);
    // make sure the size of the wal file is 0.
    assertEquals(0, fs.getFileStatus(archivePath).getLen());

    ReplicationSourceManager mockSourceManager = Mockito.mock(ReplicationSourceManager.class);
    ReplicationSource source = Mockito.mock(ReplicationSource.class);
    when(source.isPeerEnabled()).thenReturn(true);
    when(mockSourceManager.getTotalBufferUsed()).thenReturn(new AtomicLong(0));

    Configuration localConf = new Configuration(CONF);
    localConf.setInt("replication.source.maxretriesmultiplier", 1);
    localConf.setBoolean("replication.source.eof.autorecovery", true);
    // Start the reader thread.
    createReader(false, localConf);
    // Wait for the replication queue size to be 1. This means that we have handled
    // 0 length wal from oldWALs directory.
    Waiter.waitFor(localConf, 10000,
      (Waiter.Predicate<Exception>) () -> logQueue.getQueueSize(fakeWalGroupId) == 1);
  }
}
