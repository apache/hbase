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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.shouldRetryCreate;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.Sequencer;

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.SingleThreadEventExecutor;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.io.asyncfs.AsyncFSOutput;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.NameNodeException;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALProvider.AsyncWriter;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.htrace.NullScope;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

/**
 * An asynchronous implementation of FSWAL.
 * <p>
 * Here 'waitingConsumePayloads' acts as the RingBuffer in FSHLog.
 * <p>
 * For append, we process it as follow:
 * <ol>
 * <li>In the caller thread(typically, in the rpc handler thread):
 * <ol>
 * <li>Insert the entry into 'waitingConsumePayloads'. Use ringbuffer sequence as txid.</li>
 * <li>Schedule the consumer task if needed. See {@link #shouldScheduleConsumer()} for more details.
 * </li>
 * </ol>
 * </li>
 * <li>In the consumer task(in the EventLoop thread)
 * <ol>
 * <li>Poll the entry from {@link #waitingConsumePayloads} and insert it into
 * {@link #toWriteAppends}</li>
 * <li>Poll the entry from {@link #toWriteAppends}, append it to the AsyncWriter, and insert it into
 * {@link #unackedAppends}</li>
 * <li>If the buffered size reaches {@link #batchSize}, or there is a sync request, then we call
 * sync on the AsyncWriter.</li>
 * <li>In the callback methods:
 * <ul>
 * <li>If succeeded, poll the entry from {@link #unackedAppends} and drop it.</li>
 * <li>If failed, add all the entries in {@link #unackedAppends} back to {@link #toWriteAppends} and
 * wait for writing them again.</li>
 * </ul>
 * </li>
 * </ol>
 * </li>
 * </ol>
 * For sync, the processing stages are almost same. And different from FSHLog, we will open a new
 * writer and rewrite unacked entries to the new writer and sync again if we hit a sync error.
 * <p>
 * Here we only describe the logic of doReplaceWriter. The main logic of rollWriter is same with
 * FSHLog.<br>
 * For a normal roll request(for example, we have reached the log roll size):
 * <ol>
 * <li>In the log roller thread, we will set {@link #waitingRoll} to true and
 * {@link #readyForRolling} to false, and then wait on {@link #readyForRolling}(see
 * {@link #waitForSafePoint()}).</li>
 * <li>In the consumer thread, we will stop polling entries from {@link #waitingConsumePayloads} if
 * {@link #waitingRoll} is true, and also stop writing the entries in {@link #toWriteAppends}
 * out.</li>
 * <li>If there are unflush data in the writer, sync them.</li>
 * <li>When all out-going sync request is finished, i.e, the {@link #unackedAppends} is empty,
 * signal the {@link #readyForRollingCond}.</li>
 * <li>Back to the log roller thread, now we can confirm that there are no out-going entries, i.e.,
 * we reach a safe point. So it is safe to replace old writer with new writer now.</li>
 * <li>Set {@link #writerBroken} and {@link #waitingRoll} to false, cancel log roller exit checker
 * if any(see the comments in the {@link #syncFailed(Throwable)} method to see why we need a checker
 * here).</li>
 * <li>Schedule the consumer task.</li>
 * <li>Schedule a background task to close the old writer.</li>
 * </ol>
 * For a broken writer roll request, the only difference is that we can bypass the wait for safe
 * point stage. See the comments in the {@link #syncFailed(Throwable)} method for more details.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class AsyncFSWAL extends AbstractFSWAL<AsyncWriter> {

  private static final Log LOG = LogFactory.getLog(AsyncFSWAL.class);

  private static final Comparator<SyncFuture> SEQ_COMPARATOR = (o1, o2) -> {
    int c = Long.compare(o1.getTxid(), o2.getTxid());
    return c != 0 ? c : Integer.compare(System.identityHashCode(o1), System.identityHashCode(o2));
  };

  public static final String WAL_BATCH_SIZE = "hbase.wal.batch.size";
  public static final long DEFAULT_WAL_BATCH_SIZE = 64L * 1024;

  public static final String ASYNC_WAL_CREATE_MAX_RETRIES = "hbase.wal.async.create.retries";
  public static final int DEFAULT_ASYNC_WAL_CREATE_MAX_RETRIES = 10;

  private final EventLoop eventLoop;

  private final Lock consumeLock = new ReentrantLock();

  private final Runnable consumer = this::consume;

  // check if there is already a consumer task in the event loop's task queue
  private final Supplier<Boolean> hasConsumerTask;

  // new writer is created and we are waiting for old writer to be closed.
  private volatile boolean waitingRoll;

  private boolean readyForRolling;

  private final Condition readyForRollingCond = consumeLock.newCondition();

  private final RingBuffer<RingBufferTruck> waitingConsumePayloads;

  private final Sequence waitingConsumePayloadsGatingSequence;

  private final AtomicBoolean consumerScheduled = new AtomicBoolean(false);

  // writer is broken and rollWriter is needed.
  private volatile boolean writerBroken;

  private final long batchSize;

  private final int createMaxRetries;

  private final ExecutorService closeExecutor = Executors.newCachedThreadPool(
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Close-WAL-Writer-%d").build());

  private volatile AsyncFSOutput fsOut;

  private final Deque<FSWALEntry> toWriteAppends = new ArrayDeque<>();

  private final Deque<FSWALEntry> unackedAppends = new ArrayDeque<>();

  private final PriorityQueue<SyncFuture> syncFutures =
      new PriorityQueue<SyncFuture>(11, SEQ_COMPARATOR);

  // the highest txid of WAL entries being processed
  private long highestProcessedAppendTxid;

  // file length when we issue last sync request on the writer
  private long fileLengthAtLastSync;

  public AsyncFSWAL(FileSystem fs, Path rootDir, String logDir, String archiveDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
      String prefix, String suffix, EventLoop eventLoop)
      throws FailedLogCloseException, IOException {
    super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix);
    this.eventLoop = eventLoop;
    Supplier<Boolean> hasConsumerTask;
    if (eventLoop instanceof SingleThreadEventExecutor) {

      try {
        Field field = SingleThreadEventExecutor.class.getDeclaredField("taskQueue");
        field.setAccessible(true);
        Queue<?> queue = (Queue<?>) field.get(eventLoop);
        hasConsumerTask = () -> queue.peek() == consumer;
      } catch (Exception e) {
        LOG.warn("Can not get task queue of " + eventLoop + ", this is not necessary, just give up",
          e);
        hasConsumerTask = () -> false;
      }
    } else {
      hasConsumerTask = () -> false;
    }
    this.hasConsumerTask = hasConsumerTask;
    int preallocatedEventCount =
        this.conf.getInt("hbase.regionserver.wal.disruptor.event.count", 1024 * 16);
    waitingConsumePayloads =
        RingBuffer.createMultiProducer(RingBufferTruck::new, preallocatedEventCount);
    waitingConsumePayloadsGatingSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    waitingConsumePayloads.addGatingSequences(waitingConsumePayloadsGatingSequence);

    // inrease the ringbuffer sequence so our txid is start from 1
    waitingConsumePayloads.publish(waitingConsumePayloads.next());
    waitingConsumePayloadsGatingSequence.set(waitingConsumePayloads.getCursor());

    batchSize = conf.getLong(WAL_BATCH_SIZE, DEFAULT_WAL_BATCH_SIZE);
    createMaxRetries =
        conf.getInt(ASYNC_WAL_CREATE_MAX_RETRIES, DEFAULT_ASYNC_WAL_CREATE_MAX_RETRIES);
    rollWriter();
  }

  // return whether we have successfully set readyForRolling to true.
  private boolean trySetReadyForRolling() {
    // Check without holding lock first. Usually we will just return here.
    // waitingRoll is volatile and unacedEntries is only accessed inside event loop so it is safe to
    // check them outside the consumeLock.
    if (!waitingRoll || !unackedAppends.isEmpty()) {
      return false;
    }
    consumeLock.lock();
    try {
      // 1. a roll is requested
      // 2. all out-going entries have been acked(we have confirmed above).
      if (waitingRoll) {
        readyForRolling = true;
        readyForRollingCond.signalAll();
        return true;
      } else {
        return false;
      }
    } finally {
      consumeLock.unlock();
    }
  }

  private void syncFailed(Throwable error) {
    LOG.warn("sync failed", error);
    // Here we depends on the implementation of FanOutOneBlockAsyncDFSOutput and netty.
    // When error occur, FanOutOneBlockAsyncDFSOutput will fail all pending flush requests. It
    // is execute inside EventLoop. And in DefaultPromise in netty, it will notifyListener
    // directly if it is already in the EventLoop thread. And in the listener method, it will
    // call us. So here we know that all failed flush request will call us continuously, and
    // before the last one finish, no other task can be executed in EventLoop. So here we are
    // safe to use writerBroken as a guard.
    // Do not forget to revisit this if we change the implementation of
    // FanOutOneBlockAsyncDFSOutput!
    consumeLock.lock();
    try {
      if (writerBroken) {
        return;
      }
      writerBroken = true;
      if (waitingRoll) {
        readyForRolling = true;
        readyForRollingCond.signalAll();
      }
    } finally {
      consumeLock.unlock();
    }
    for (Iterator<FSWALEntry> iter = unackedAppends.descendingIterator(); iter.hasNext();) {
      toWriteAppends.addFirst(iter.next());
    }
    highestUnsyncedTxid = highestSyncedTxid.get();
    // request a roll.
    requestLogRoll();
  }

  private void syncCompleted(AsyncWriter writer, long processedTxid, long startTimeNs) {
    highestSyncedTxid.set(processedTxid);
    int syncCount = finishSync(true);
    for (Iterator<FSWALEntry> iter = unackedAppends.iterator(); iter.hasNext();) {
      if (iter.next().getTxid() <= processedTxid) {
        iter.remove();
      } else {
        break;
      }
    }
    postSync(System.nanoTime() - startTimeNs, syncCount);
    // Ideally, we should set a flag to indicate that the log roll has already been requested for
    // the current writer and give up here, and reset the flag when roll is finished. But we
    // finish roll in the log roller thread so the flag need to be set by different thread which
    // typically means we need to use a lock to protect it and do fencing. As the log roller will
    // aggregate the roll requests of the same WAL, so it is safe to call requestLogRoll multiple
    // times before the roll actual happens. But we need to stop if we set readyForRolling to true
    // and wake up the log roller thread waiting in waitForSafePoint as the rollWriter call may
    // return firstly and then we run the code below and request a roll on the new writer.
    if (trySetReadyForRolling()) {
      // we have just finished a roll, then do not need to check for log rolling, the writer will be
      // closed soon.
      return;
    }
    if (writer.getLength() < logrollsize) {
      return;
    }
    if (!rollWriterLock.tryLock()) {
      return;
    }
    try {
      requestLogRoll();
    } finally {
      rollWriterLock.unlock();
    }
  }

  private void sync(final AsyncWriter writer, final long processedTxid) {
    fileLengthAtLastSync = writer.getLength();
    final long startTimeNs = System.nanoTime();
    writer.sync().whenComplete((result, error) -> {
      if (error != null) {
        syncFailed(error);
      } else {
        syncCompleted(writer, processedTxid, startTimeNs);
      }
    });
  }

  private void addTimeAnnotation(SyncFuture future, String annotation) {
    TraceScope scope = Trace.continueSpan(future.getSpan());
    Trace.addTimelineAnnotation(annotation);
    future.setSpan(scope.detach());
  }

  private int finishSyncLowerThanTxid(long txid, boolean addSyncTrace) {
    int finished = 0;
    for (SyncFuture sync; (sync = syncFutures.peek()) != null;) {
      if (sync.getTxid() <= txid) {
        sync.done(txid, null);
        syncFutures.remove();
        finished++;
        if (addSyncTrace) {
          addTimeAnnotation(sync, "writer synced");
        }
      } else {
        break;
      }
    }
    return finished;
  }

  private int finishSync(boolean addSyncTrace) {
    long doneTxid = highestSyncedTxid.get();
    if (doneTxid >= highestProcessedAppendTxid) {
      if (toWriteAppends.isEmpty()) {
        // all outstanding appends have been acked, just finish all syncs.
        long maxSyncTxid = doneTxid;
        for (SyncFuture sync : syncFutures) {
          maxSyncTxid = Math.max(maxSyncTxid, sync.getTxid());
          sync.done(maxSyncTxid, null);
          if (addSyncTrace) {
            addTimeAnnotation(sync, "writer synced");
          }
        }
        highestSyncedTxid.set(maxSyncTxid);
        int finished = syncFutures.size();
        syncFutures.clear();
        return finished;
      } else {
        // There is no append between highestProcessedAppendTxid and lowestUnprocessedAppendTxid, so
        // if highestSyncedTxid >= highestProcessedAppendTxid, then all syncs whose txid are between
        // highestProcessedAppendTxid and lowestUnprocessedAppendTxid can be finished.
        long lowestUnprocessedAppendTxid = toWriteAppends.peek().getTxid();
        assert lowestUnprocessedAppendTxid > highestProcessedAppendTxid;
        highestSyncedTxid.set(lowestUnprocessedAppendTxid - 1);
        return finishSyncLowerThanTxid(lowestUnprocessedAppendTxid - 1, addSyncTrace);
      }
    } else {
      return finishSyncLowerThanTxid(doneTxid, addSyncTrace);
    }
  }

  private void appendAndSync() {
    final AsyncWriter writer = this.writer;
    // maybe a sync request is not queued when we issue a sync, so check here to see if we could
    // finish some.
    finishSync(false);
    long newHighestProcessedTxid = -1L;
    for (Iterator<FSWALEntry> iter = toWriteAppends.iterator(); iter.hasNext();) {
      FSWALEntry entry = iter.next();
      boolean appended;
      Span span = entry.detachSpan();
      // the span maybe null if this is a retry after rolling.
      if (span != null) {
        TraceScope scope = Trace.continueSpan(span);
        try {
          appended = append(writer, entry);
        } catch (IOException e) {
          throw new AssertionError("should not happen", e);
        } finally {
          assert scope == NullScope.INSTANCE || !scope.isDetached();
          scope.close(); // append scope is complete
        }
      } else {
        try {
          appended = append(writer, entry);
        } catch (IOException e) {
          throw new AssertionError("should not happen", e);
        }
      }
      newHighestProcessedTxid = entry.getTxid();
      iter.remove();
      if (appended) {
        unackedAppends.addLast(entry);
        if (writer.getLength() - fileLengthAtLastSync >= batchSize) {
          break;
        }
      }
    }
    // if we have a newer transaction id, update it.
    // otherwise, use the previous transaction id.
    if (newHighestProcessedTxid > 0) {
      highestProcessedAppendTxid = newHighestProcessedTxid;
    } else {
      newHighestProcessedTxid = highestProcessedAppendTxid;
    }

    if (writer.getLength() - fileLengthAtLastSync >= batchSize) {
      // sync because buffer size limit.
      sync(writer, newHighestProcessedTxid);
      return;
    }
    if (writer.getLength() == fileLengthAtLastSync) {
      // we haven't written anything out, just advance the highestSyncedSequence since we may only
      // stamped some region sequence id.
      highestSyncedTxid.set(newHighestProcessedTxid);
      finishSync(false);
      trySetReadyForRolling();
      return;
    }
    // we have some unsynced data but haven't reached the batch size yet
    if (!syncFutures.isEmpty()) {
      // we have at least one sync request
      sync(writer, newHighestProcessedTxid);
      return;
    }
    // usually waitingRoll is false so we check it without lock first.
    if (waitingRoll) {
      consumeLock.lock();
      try {
        if (waitingRoll) {
          // there is a roll request
          sync(writer, newHighestProcessedTxid);
        }
      } finally {
        consumeLock.unlock();
      }
    }
  }

  private void consume() {
    consumeLock.lock();
    try {
      if (writerBroken) {
        return;
      }
      if (waitingRoll) {
        if (writer.getLength() > fileLengthAtLastSync) {
          // issue a sync
          sync(writer, highestProcessedAppendTxid);
        } else {
          if (unackedAppends.isEmpty()) {
            readyForRolling = true;
            readyForRollingCond.signalAll();
          }
        }
        return;
      }
    } finally {
      consumeLock.unlock();
    }
    long nextCursor = waitingConsumePayloadsGatingSequence.get() + 1;
    for (long cursorBound =
        waitingConsumePayloads.getCursor(); nextCursor <= cursorBound; nextCursor++) {
      if (!waitingConsumePayloads.isPublished(nextCursor)) {
        break;
      }
      RingBufferTruck truck = waitingConsumePayloads.get(nextCursor);
      switch (truck.type()) {
        case APPEND:
          toWriteAppends.addLast(truck.unloadAppend());
          break;
        case SYNC:
          syncFutures.add(truck.unloadSync());
          break;
        default:
          LOG.warn("RingBufferTruck with unexpected type: " + truck.type());
          break;
      }
      waitingConsumePayloadsGatingSequence.set(nextCursor);
    }
    appendAndSync();
    if (hasConsumerTask.get()) {
      return;
    }
    if (toWriteAppends.isEmpty()) {
      if (waitingConsumePayloadsGatingSequence.get() == waitingConsumePayloads.getCursor()) {
        consumerScheduled.set(false);
        // recheck here since in append and sync we do not hold the consumeLock. Thing may
        // happen like
        // 1. we check cursor, no new entry
        // 2. someone publishes a new entry to ringbuffer and the consumerScheduled is true and
        // give up scheduling the consumer task.
        // 3. we set consumerScheduled to false and also give up scheduling consumer task.
        if (waitingConsumePayloadsGatingSequence.get() == waitingConsumePayloads.getCursor()) {
          return;
        } else {
          // maybe someone has grabbed this before us
          if (!consumerScheduled.compareAndSet(false, true)) {
            return;
          }
        }
      }
    }
    // reschedule if we still have something to write.
    eventLoop.execute(consumer);
  }

  private boolean shouldScheduleConsumer() {
    if (writerBroken || waitingRoll) {
      return false;
    }
    return consumerScheduled.compareAndSet(false, true);
  }

  @Override
  public long append(HRegionInfo hri, WALKey key, WALEdit edits, boolean inMemstore)
      throws IOException {
    if (closed) {
      throw new IOException("Cannot append; log is closed");
    }
    TraceScope scope = Trace.startSpan("AsyncFSWAL.append");
    long txid = waitingConsumePayloads.next();
    try {
      RingBufferTruck truck = waitingConsumePayloads.get(txid);
      truck.load(new FSWALEntry(txid, key, edits, hri, inMemstore), scope.detach());
    } finally {
      waitingConsumePayloads.publish(txid);
    }
    if (shouldScheduleConsumer()) {
      eventLoop.execute(consumer);
    }
    return txid;
  }

  @Override
  public void sync() throws IOException {
    TraceScope scope = Trace.startSpan("AsyncFSWAL.sync");
    try {
      long txid = waitingConsumePayloads.next();
      SyncFuture future;
      try {
        future = getSyncFuture(txid, scope.detach());
        RingBufferTruck truck = waitingConsumePayloads.get(txid);
        truck.load(future);
      } finally {
        waitingConsumePayloads.publish(txid);
      }
      if (shouldScheduleConsumer()) {
        eventLoop.execute(consumer);
      }
      scope = Trace.continueSpan(blockOnSync(future));
    } finally {
      assert scope == NullScope.INSTANCE || !scope.isDetached();
      scope.close();
    }
  }

  @Override
  public void sync(long txid) throws IOException {
    if (highestSyncedTxid.get() >= txid) {
      return;
    }
    TraceScope scope = Trace.startSpan("AsyncFSWAL.sync");
    try {
      // here we do not use ring buffer sequence as txid
      long sequence = waitingConsumePayloads.next();
      SyncFuture future;
      try {
        future = getSyncFuture(txid, scope.detach());
        RingBufferTruck truck = waitingConsumePayloads.get(sequence);
        truck.load(future);
      } finally {
        waitingConsumePayloads.publish(sequence);
      }
      if (shouldScheduleConsumer()) {
        eventLoop.execute(consumer);
      }
      scope = Trace.continueSpan(blockOnSync(future));
    } finally {
      assert scope == NullScope.INSTANCE || !scope.isDetached();
      scope.close();
    }
  }

  @Override
  protected AsyncWriter createWriterInstance(Path path) throws IOException {
    boolean overwrite = false;
    for (int retry = 0;; retry++) {
      try {
        return AsyncFSWALProvider.createAsyncWriter(conf, fs, path, overwrite, eventLoop);
      } catch (RemoteException e) {
        LOG.warn("create wal log writer " + path + " failed, retry = " + retry, e);
        if (shouldRetryCreate(e)) {
          if (retry >= createMaxRetries) {
            break;
          }
        } else {
          throw e.unwrapRemoteException();
        }
      } catch (NameNodeException e) {
        throw e;
      } catch (IOException e) {
        LOG.warn("create wal log writer " + path + " failed, retry = " + retry, e);
        if (retry >= createMaxRetries) {
          break;
        }
        // overwrite the old broken file.
        overwrite = true;
        try {
          Thread.sleep(ConnectionUtils.getPauseTime(100, retry));
        } catch (InterruptedException ie) {
          throw new InterruptedIOException();
        }
      }
    }
    throw new IOException("Failed to create wal log writer " + path + " after retrying "
        + createMaxRetries + " time(s)");
  }

  private void waitForSafePoint() {
    consumeLock.lock();
    try {
      if (writerBroken || this.writer == null) {
        return;
      }
      consumerScheduled.set(true);
      waitingRoll = true;
      readyForRolling = false;
      eventLoop.execute(consumer);
      while (!readyForRolling) {
        readyForRollingCond.awaitUninterruptibly();
      }
    } finally {
      consumeLock.unlock();
    }
  }

  @Override
  protected long doReplaceWriter(Path oldPath, Path newPath, AsyncWriter nextWriter)
      throws IOException {
    waitForSafePoint();
    final AsyncWriter oldWriter = this.writer;
    this.writer = nextWriter;
    if (nextWriter != null && nextWriter instanceof AsyncProtobufLogWriter) {
      this.fsOut = ((AsyncProtobufLogWriter) nextWriter).getOutput();
    }
    this.fileLengthAtLastSync = 0L;
    consumeLock.lock();
    try {
      consumerScheduled.set(true);
      writerBroken = waitingRoll = false;
      eventLoop.execute(consumer);
    } finally {
      consumeLock.unlock();
    }
    long oldFileLen;
    if (oldWriter != null) {
      oldFileLen = oldWriter.getLength();
      closeExecutor.execute(() -> {
        try {
          oldWriter.close();
        } catch (IOException e) {
          LOG.warn("close old writer failed", e);
        }
      });
    } else {
      oldFileLen = 0L;
    }
    return oldFileLen;
  }

  @Override
  protected void doShutdown() throws IOException {
    waitForSafePoint();
    this.writer.close();
    this.writer = null;
    closeExecutor.shutdown();
  }

  @Override
  protected void doAppend(AsyncWriter writer, FSWALEntry entry) {
    writer.append(entry);
  }

  @Override
  DatanodeInfo[] getPipeline() {
    AsyncFSOutput output = this.fsOut;
    return output != null ? output.getPipeline() : new DatanodeInfo[0];
  }

  @Override
  int getLogReplication() {
    return getPipeline().length;
  }
}
