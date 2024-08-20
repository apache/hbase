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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.hbase.regionserver.wal.AbstractProtobufWALReader;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.replication.regionserver.WALEntryStream.HasNext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALEditInternalHelper;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALHeader;

public abstract class TestBasicWALEntryStream extends WALEntryStreamTestBase {

  @Parameter
  public boolean isCompressionEnabled;

  @Parameters(name = "{index}: isCompressionEnabled={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[] { false }, new Object[] { true });
  }

  @Before
  public void setUp() throws Exception {
    CONF.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, isCompressionEnabled);
    initWAL();
  }

  private WAL.Entry next(WALEntryStream entryStream) {
    assertEquals(HasNext.YES, entryStream.hasNext());
    return entryStream.next();
  }

  /**
   * Tests basic reading of log appends
   */
  @Test
  public void testAppendsWithRolls() throws Exception {
    appendToLogAndSync();
    long oldPos;
    try (WALEntryStream entryStream =
      new WALEntryStream(logQueue, fs, CONF, 0, log, new MetricsSource("1"), fakeWalGroupId)) {
      // There's one edit in the log, read it. Reading past it needs to throw exception
      assertEquals(HasNext.YES, entryStream.hasNext());
      WAL.Entry entry = entryStream.peek();
      assertSame(entry, entryStream.next());
      assertNotNull(entry);
      assertEquals(HasNext.RETRY, entryStream.hasNext());
      assertNull(entryStream.peek());
      assertThrows(IllegalStateException.class, () -> entryStream.next());
      oldPos = entryStream.getPosition();
    }

    appendToLogAndSync();

    try (WALEntryStream entryStream = new WALEntryStreamWithRetries(logQueue, fs, CONF, oldPos, log,
      new MetricsSource("1"), fakeWalGroupId)) {
      // Read the newly added entry, make sure we made progress
      WAL.Entry entry = next(entryStream);
      assertNotEquals(oldPos, entryStream.getPosition());
      assertNotNull(entry);
      oldPos = entryStream.getPosition();
    }

    // We rolled but we still should see the end of the first log and get that item
    appendToLogAndSync();
    log.rollWriter();
    appendToLogAndSync();

    try (WALEntryStreamWithRetries entryStream = new WALEntryStreamWithRetries(logQueue, fs, CONF,
      oldPos, log, new MetricsSource("1"), fakeWalGroupId)) {
      WAL.Entry entry = next(entryStream);
      assertNotEquals(oldPos, entryStream.getPosition());
      assertNotNull(entry);

      // next item should come from the new log
      entry = next(entryStream);
      assertNotEquals(oldPos, entryStream.getPosition());
      assertNotNull(entry);

      // no more entries to read, disable retry otherwise we will get a wait too much time error
      entryStream.disableRetry();
      assertEquals(HasNext.RETRY, entryStream.hasNext());
      oldPos = entryStream.getPosition();
    }
  }

  /**
   * Tests that if after a stream is opened, more entries come in and then the log is rolled, we
   * don't mistakenly dequeue the current log thinking we're done with it
   */
  @Test
  public void testLogRollWhileStreaming() throws Exception {
    appendToLog("1");
    // 2
    appendToLog("2");
    try (WALEntryStreamWithRetries entryStream = new WALEntryStreamWithRetries(logQueue, fs, CONF,
      0, log, new MetricsSource("1"), fakeWalGroupId)) {
      assertEquals("1", getRow(next(entryStream)));

      // 3 - comes in after reader opened
      appendToLog("3");
      // log roll happening while we're reading
      log.rollWriter();
      // 4 - this append is in the rolled log
      appendToLog("4");

      assertEquals("2", getRow(next(entryStream)));
      // we should not have dequeued yet since there's still an entry in first log
      assertEquals(2, getQueue().size());
      // if implemented improperly, this would be 4 and 3 would be skipped
      assertEquals("3", getRow(next(entryStream)));
      // 4
      assertEquals("4", getRow(next(entryStream)));
      // now we've dequeued and moved on to next log properly
      assertEquals(1, getQueue().size());

      // disable so we can get the return value immediately, otherwise we will fail with wait too
      // much time...
      entryStream.disableRetry();
      assertEquals(HasNext.RETRY, entryStream.hasNext());
    }
  }

  /**
   * Tests that if writes come in while we have a stream open, we shouldn't miss them
   */

  @Test
  public void testNewEntriesWhileStreaming() throws Exception {
    appendToLog("1");
    try (WALEntryStream entryStream =
      new WALEntryStream(logQueue, fs, CONF, 0, log, new MetricsSource("1"), fakeWalGroupId)) {
      assertNotNull(next(entryStream)); // we've hit the end of the stream at this point

      // some new entries come in while we're streaming
      appendToLog("2");
      appendToLog("3");

      // don't see them
      assertEquals(HasNext.RETRY, entryStream.hasNext());

      // But we do if we retry next time, as the entryStream will reset the reader
      assertEquals("2", getRow(next(entryStream)));
      assertEquals("3", getRow(next(entryStream)));
      // reached the end again
      assertEquals(HasNext.RETRY, entryStream.hasNext());
    }
  }

  @Test
  public void testResumeStreamingFromPosition() throws Exception {
    long lastPosition = 0;
    appendToLog("1");
    try (WALEntryStream entryStream =
      new WALEntryStream(logQueue, fs, CONF, 0, log, new MetricsSource("1"), fakeWalGroupId)) {
      assertNotNull(next(entryStream)); // we've hit the end of the stream at this point
      appendToLog("2");
      appendToLog("3");
      lastPosition = entryStream.getPosition();
    }
    // next stream should picks up where we left off
    try (WALEntryStream entryStream = new WALEntryStream(logQueue, fs, CONF, lastPosition, log,
      new MetricsSource("1"), fakeWalGroupId)) {
      assertEquals("2", getRow(next(entryStream)));
      assertEquals("3", getRow(next(entryStream)));
      assertEquals(HasNext.RETRY, entryStream.hasNext()); // done
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
    try (WALEntryStream entryStream = new WALEntryStream(logQueue, fs, CONF, lastPosition, log,
      new MetricsSource("1"), fakeWalGroupId)) {
      assertNotNull(next(entryStream));
      lastPosition = entryStream.getPosition();
    }
    // there should still be two more entries from where we left off
    try (WALEntryStream entryStream = new WALEntryStream(logQueue, fs, CONF, lastPosition, log,
      new MetricsSource("1"), fakeWalGroupId)) {
      assertNotNull(next(entryStream));
      assertNotNull(next(entryStream));
      assertEquals(HasNext.RETRY, entryStream.hasNext());
    }
  }

  @Test
  public void testEmptyStream() throws Exception {
    try (WALEntryStream entryStream =
      new WALEntryStream(logQueue, fs, CONF, 0, log, new MetricsSource("1"), fakeWalGroupId)) {
      assertEquals(HasNext.RETRY, entryStream.hasNext());
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

  private ReplicationSource mockReplicationSource(boolean recovered, Configuration conf)
    throws IOException {
    ReplicationSourceManager mockSourceManager = new ReplicationSourceManager(null, null, conf,
      null, null, null, null, null, null, null, createMockGlobalMetrics());
    Server mockServer = Mockito.mock(Server.class);
    ReplicationSource source = Mockito.mock(ReplicationSource.class);
    when(source.getSourceManager()).thenReturn(mockSourceManager);
    when(source.getSourceMetrics()).thenReturn(new MetricsSource("1"));
    when(source.getWALFileLengthProvider()).thenReturn(log);
    when(source.getServer()).thenReturn(mockServer);
    when(source.isRecovered()).thenReturn(recovered);
    return source;
  }

  private MetricsReplicationGlobalSourceSource createMockGlobalMetrics() {
    MetricsReplicationGlobalSourceSource globalMetrics =
      Mockito.mock(MetricsReplicationGlobalSourceSource.class);
    final AtomicLong bufferUsedCounter = new AtomicLong(0);
    Mockito.doAnswer((invocationOnMock) -> {
      bufferUsedCounter.set(invocationOnMock.getArgument(0, Long.class));
      return null;
    }).when(globalMetrics).setWALReaderEditsBufferBytes(Mockito.anyLong());
    when(globalMetrics.getWALReaderEditsBufferBytes())
      .then(invocationOnMock -> bufferUsedCounter.get());
    return globalMetrics;
  }

  private ReplicationSourceWALReader createReader(boolean recovered, Configuration conf)
    throws IOException {
    ReplicationSource source = mockReplicationSource(recovered, conf);
    when(source.isPeerEnabled()).thenReturn(true);
    ReplicationSourceWALReader reader = new ReplicationSourceWALReader(fs, conf, logQueue, 0,
      getDummyFilter(), source, fakeWalGroupId);
    reader.start();
    return reader;
  }

  private ReplicationSourceWALReader createReaderWithBadReplicationFilter(int numFailures,
    Configuration conf) throws IOException {
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
      new WALEntryStream(logQueue, fs, CONF, 0, log, new MetricsSource("1"), fakeWalGroupId)) {
      for (int i = 0; i < 3; i++) {
        assertNotNull(next(entryStream));
      }
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
      new WALEntryStream(logQueue, fs, CONF, 0, log, new MetricsSource("1"), fakeWalGroupId)) {
      for (int i = 0; i < 3; i++) {
        assertNotNull(next(entryStream));
      }
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
        return fs.getFileStatus(walPath).getLen() > 0
          && ((AbstractFSWAL<?>) log).getInflightWALCloseCount() == 0;
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
    assertTrue("Position " + entryBatch.getLastWalPosition() + " is out of range, file length is "
      + walLength, entryBatch.getLastWalPosition() <= walLength);
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
      new WALEntryStream(logQueue, fs, CONF, 0, log, new MetricsSource("1"), fakeWalGroupId)) {
      for (int i = 0; i < 3; i++) {
        assertNotNull(next(entryStream));
      }
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
    WALEditInternalHelper.addExtendedCell(edit, new KeyValue(Bytes.toBytes(row), family, qualifier,
      EnvironmentEdgeManager.currentTime(), qualifier));
    return edit;
  }

  private WALEntryFilter getDummyFilter() {
    return new WALEntryFilter() {

      @Override
      public WAL.Entry filter(WAL.Entry entry) {
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
    public WAL.Entry filter(WAL.Entry entry) {
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
    try (WALEntryStream entryStream = new WALEntryStream(logQueue, fs, CONF, 0,
      p -> OptionalLong.of(fileLength.get()), new MetricsSource("1"), fakeWalGroupId)) {
      assertNotNull(next(entryStream));
      // can not get log 2
      assertEquals(HasNext.RETRY, entryStream.hasNext());
      Thread.sleep(1000);
      // still can not get log 2
      assertEquals(HasNext.RETRY, entryStream.hasNext());

      // can get log 2 now
      fileLength.set(size);
      assertNotNull(next(entryStream));

      assertEquals(HasNext.RETRY, entryStream.hasNext());
    }
  }

  /**
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
    reader.start();
    reader.join();
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
    Path emptyLog = new Path(fs.getHomeDirectory(), "log.2." + isCompressionEnabled);
    fs.create(emptyLog).close();
    assertEquals(0, fs.getFileStatus(emptyLog).getLen());
    localLogQueue.enqueueLog(emptyLog, fakeWalGroupId);

    final Path log1 = new Path(fs.getHomeDirectory(), "log.1." + isCompressionEnabled);
    WALProvider.Writer writer1 = WALFactory.createWALWriter(fs, log1, TEST_UTIL.getConfiguration());
    appendEntries(writer1, 3);
    localLogQueue.enqueueLog(log1, fakeWalGroupId);

    when(source.isPeerEnabled()).thenReturn(true);
    // Override the max retries multiplier to fail fast.
    conf.setInt("replication.source.maxretriesmultiplier", 1);
    conf.setBoolean("replication.source.eof.autorecovery", true);
    conf.setInt("replication.source.nb.batches", 10);
    // Create a reader thread.
    ReplicationSourceWALReader reader = new ReplicationSourceWALReader(fs, conf, localLogQueue, 0,
      getDummyFilter(), source, fakeWalGroupId);
    assertEquals("Initial log queue size is not correct", 2,
      localLogQueue.getQueueSize(fakeWalGroupId));
    reader.start();
    reader.join();

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
      WALEditInternalHelper.addExtendedCell(edit, kv);
      WALKeyImpl key = new WALKeyImpl(b, TableName.valueOf(b), 0, 0, HConstants.DEFAULT_CLUSTER_ID);
      NavigableMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
      scopes.put(b, HConstants.REPLICATION_SCOPE_GLOBAL);
      writer.append(new WAL.Entry(key, edit));
      writer.sync(false);
    }
    writer.close();
  }

  /***
   * Tests size of log queue is incremented and decremented properly.
   */
  @Test
  public void testSizeOfLogQueue() throws Exception {
    // There should be always 1 log which is current wal.
    assertEquals(1, logQueue.getMetrics().getSizeOfLogQueue());
    appendToLogAndSync();

    log.rollWriter();
    // wait until the previous WAL file is cleanly closed, so later we can aleays see
    // RETRY_IMMEDIATELY instead of RETRY. The wait here is necessary because the closing of a WAL
    // writer is asynchronouns
    TEST_UTIL.waitFor(30000, () -> fs.getClient().isFileClosed(logQueue.getQueue(fakeWalGroupId)
      .peek().makeQualified(fs.getUri(), fs.getWorkingDirectory()).toUri().getPath()));
    // After rolling there will be 2 wals in the queue
    assertEquals(2, logQueue.getMetrics().getSizeOfLogQueue());

    try (WALEntryStream entryStream =
      new WALEntryStream(logQueue, fs, CONF, 0, log, logQueue.getMetrics(), fakeWalGroupId)) {
      // There's one edit in the log, read it.
      assertNotNull(next(entryStream));
      // we've switched to the next WAL, and the previous WAL file is closed cleanly, so it is
      // RETRY_IMMEDIATELY
      assertEquals(HasNext.RETRY_IMMEDIATELY, entryStream.hasNext());
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
    try (WALEntryStream entryStream = new WALEntryStreamWithRetries(logQueue, fs, CONF, 0, log,
      logQueue.getMetrics(), fakeWalGroupId)) {
      assertEquals(0, logQueue.getMetrics().getUncleanlyClosedWALs());
      appendToLogAndSync();
      assertNotNull(next(entryStream));
      log.rollWriter();
      appendToLogAndSync();
      assertNotNull(next(entryStream));
      assertEquals(0, logQueue.getMetrics().getUncleanlyClosedWALs());
    }
  }

  /**
   * Tests that we handle EOFException properly if the wal has moved to oldWALs directory.
   */
  @Test
  public void testEOFExceptionInOldWALsDirectory() throws Exception {
    assertEquals(1, logQueue.getQueueSize(fakeWalGroupId));
    AbstractFSWAL<?> abstractWAL = (AbstractFSWAL<?>) log;
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

    ReplicationSource source = Mockito.mock(ReplicationSource.class);
    when(source.isPeerEnabled()).thenReturn(true);

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

  /**
   * This test is for HBASE-27778, when {@link WALEntryFilter#filter} throws exception for some
   * entries in {@link WALEntryBatch},{@link ReplicationSourceWALReader#totalBufferUsed} should be
   * decreased because {@link WALEntryBatch} is not put to
   * {@link ReplicationSourceWALReader#entryBatchQueue}.
   */
  @Test
  public void testReplicationSourceWALReaderWithPartialWALEntryFailingFilter() throws Exception {
    appendEntriesToLogAndSync(3);
    // get ending position
    long position;
    try (WALEntryStream entryStream =
      new WALEntryStream(logQueue, fs, CONF, 0, log, new MetricsSource("1"), fakeWalGroupId)) {
      for (int i = 0; i < 3; i++) {
        assertNotNull(next(entryStream));
      }
      position = entryStream.getPosition();
    }

    Path walPath = getQueue().peek();
    int maxThrowExceptionCount = 3;

    ReplicationSource source = mockReplicationSource(false, CONF);
    when(source.isPeerEnabled()).thenReturn(true);
    PartialWALEntryFailingWALEntryFilter walEntryFilter =
      new PartialWALEntryFailingWALEntryFilter(maxThrowExceptionCount, 3);
    ReplicationSourceWALReader reader =
      new ReplicationSourceWALReader(fs, CONF, logQueue, 0, walEntryFilter, source, fakeWalGroupId);
    reader.start();
    WALEntryBatch entryBatch = reader.take();

    assertNotNull(entryBatch);
    assertEquals(3, entryBatch.getWalEntries().size());
    long sum = entryBatch.getWalEntries().stream()
      .mapToLong(WALEntryBatch::getEntrySizeExcludeBulkLoad).sum();
    assertEquals(position, entryBatch.getLastWalPosition());
    assertEquals(walPath, entryBatch.getLastWalPath());
    assertEquals(3, entryBatch.getNbRowKeys());
    assertEquals(sum, source.getSourceManager().getTotalBufferUsed());
    assertEquals(sum, source.getSourceManager().getGlobalMetrics().getWALReaderEditsBufferBytes());
    assertEquals(maxThrowExceptionCount, walEntryFilter.getThrowExceptionCount());
    assertNull(reader.poll(10));
  }

  // testcase for HBASE-28748
  @Test
  public void testWALEntryStreamEOFRightAfterHeader() throws Exception {
    assertEquals(1, logQueue.getQueueSize(fakeWalGroupId));
    AbstractFSWAL<?> abstractWAL = (AbstractFSWAL<?>) log;
    Path emptyLogFile = abstractWAL.getCurrentFileName();
    log.rollWriter(true);

    // AsyncFSWAl and FSHLog both moves the log from WALs to oldWALs directory asynchronously.
    // Wait for in flight wal close count to become 0. This makes sure that empty wal is moved to
    // oldWALs directory.
    Waiter.waitFor(CONF, 5000,
      (Waiter.Predicate<Exception>) () -> abstractWAL.getInflightWALCloseCount() == 0);
    // There will 2 logs in the queue.
    assertEquals(2, logQueue.getQueueSize(fakeWalGroupId));
    appendToLogAndSync();

    Path archivedEmptyLogFile = AbstractFSWALProvider.findArchivedLog(emptyLogFile, CONF);

    // read the wal header
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    bos.write(AbstractProtobufWALReader.PB_WAL_MAGIC);
    try (FSDataInputStream in = fs.open(archivedEmptyLogFile)) {
      IOUtils.skipFully(in, AbstractProtobufWALReader.PB_WAL_MAGIC.length);
      WALHeader header = WALHeader.parseDelimitedFrom(in);
      header.writeDelimitedTo(bos);
    }
    // truncate the first empty log so we have an incomplete header
    try (FSDataOutputStream out = fs.create(archivedEmptyLogFile, true)) {
      bos.writeTo(out);
    }
    try (WALEntryStream entryStream =
      new WALEntryStream(logQueue, fs, CONF, 0, log, new MetricsSource("1"), fakeWalGroupId)) {
      assertEquals(HasNext.RETRY_IMMEDIATELY, entryStream.hasNext());
      assertNotNull(next(entryStream));
    }
  }

  private static class PartialWALEntryFailingWALEntryFilter implements WALEntryFilter {
    private int filteredWALEntryCount = -1;
    private int walEntryCount = 0;
    private int throwExceptionCount = -1;
    private int maxThrowExceptionCount;

    public PartialWALEntryFailingWALEntryFilter(int throwExceptionLimit, int walEntryCount) {
      this.maxThrowExceptionCount = throwExceptionLimit;
      this.walEntryCount = walEntryCount;
    }

    @Override
    public WAL.Entry filter(WAL.Entry entry) {
      filteredWALEntryCount++;
      if (filteredWALEntryCount < walEntryCount - 1) {
        return entry;
      }

      filteredWALEntryCount = -1;
      throwExceptionCount++;
      if (throwExceptionCount <= maxThrowExceptionCount - 1) {
        throw new WALEntryFilterRetryableException("failing filter");
      }
      return entry;
    }

    public int getThrowExceptionCount() {
      return throwExceptionCount;
    }
  }

}
