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
package org.apache.hadoop.hbase.regionserver.wal;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.Sequencer;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.asyncfs.AsyncFSOutput;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALProvider.AsyncWriter;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.htrace.core.TraceScope;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoop;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.SingleThreadEventExecutor;

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
 * <li>In the consumer task(executed in a single threaded thread pool)
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
 * {@link #waitingRoll} is true, and also stop writing the entries in {@link #toWriteAppends} out.
 * </li>
 * <li>If there are unflush data in the writer, sync them.</li>
 * <li>When all out-going sync request is finished, i.e, the {@link #unackedAppends} is empty,
 * signal the {@link #readyForRollingCond}.</li>
 * <li>Back to the log roller thread, now we can confirm that there are no out-going entries, i.e.,
 * we reach a safe point. So it is safe to replace old writer with new writer now.</li>
 * <li>Set {@link #writerBroken} and {@link #waitingRoll} to false.</li>
 * <li>Schedule the consumer task.</li>
 * <li>Schedule a background task to close the old writer.</li>
 * </ol>
 * For a broken writer roll request, the only difference is that we can bypass the wait for safe
 * point stage.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class AsyncFSWAL extends AbstractFSWAL<AsyncWriter> {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncFSWAL.class);

  private static final Comparator<SyncFuture> SEQ_COMPARATOR = (o1, o2) -> {
    int c = Long.compare(o1.getTxid(), o2.getTxid());
    return c != 0 ? c : Integer.compare(System.identityHashCode(o1), System.identityHashCode(o2));
  };

  public static final String WAL_BATCH_SIZE = "hbase.wal.batch.size";
  public static final long DEFAULT_WAL_BATCH_SIZE = 64L * 1024;

  public static final String ASYNC_WAL_USE_SHARED_EVENT_LOOP =
    "hbase.wal.async.use-shared-event-loop";
  public static final boolean DEFAULT_ASYNC_WAL_USE_SHARED_EVENT_LOOP = false;

  public static final String ASYNC_WAL_WAIT_ON_SHUTDOWN_IN_SECONDS =
    "hbase.wal.async.wait.on.shutdown.seconds";
  public static final int DEFAULT_ASYNC_WAL_WAIT_ON_SHUTDOWN_IN_SECONDS = 5;

  private final EventLoopGroup eventLoopGroup;

  private final ExecutorService consumeExecutor;

  private final Class<? extends Channel> channelClass;

  private final Lock consumeLock = new ReentrantLock();

  private final Runnable consumer = this::consume;

  // check if there is already a consumer task in the event loop's task queue
  private final Supplier<Boolean> hasConsumerTask;

  private static final int MAX_EPOCH = 0x3FFFFFFF;
  // the lowest bit is waitingRoll, which means new writer is created and we are waiting for old
  // writer to be closed.
  // the second lowest bit is writerBorken which means the current writer is broken and rollWriter
  // is needed.
  // all other bits are the epoch number of the current writer, this is used to detect whether the
  // writer is still the one when you issue the sync.
  // notice that, modification to this field is only allowed under the protection of consumeLock.
  private volatile int epochAndState;

  // used to guard the log roll request when we exceed the log roll size.
  private boolean rollRequested;

  private boolean readyForRolling;

  private final Condition readyForRollingCond = consumeLock.newCondition();

  private final RingBuffer<RingBufferTruck> waitingConsumePayloads;

  private final Sequence waitingConsumePayloadsGatingSequence;

  private final AtomicBoolean consumerScheduled = new AtomicBoolean(false);

  private final long batchSize;

  private final ExecutorService closeExecutor = Executors.newCachedThreadPool(
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Close-WAL-Writer-%d").build());

  private volatile AsyncFSOutput fsOut;

  private final Deque<FSWALEntry> toWriteAppends = new ArrayDeque<>();

  private final Deque<FSWALEntry> unackedAppends = new ArrayDeque<>();

  private final SortedSet<SyncFuture> syncFutures = new TreeSet<>(SEQ_COMPARATOR);

  // the highest txid of WAL entries being processed
  private long highestProcessedAppendTxid;

  // file length when we issue last sync request on the writer
  private long fileLengthAtLastSync;

  private long highestProcessedAppendTxidAtLastSync;

  private final int waitOnShutdownInSeconds;

  public AsyncFSWAL(FileSystem fs, Path rootDir, String logDir, String archiveDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
      String prefix, String suffix, EventLoopGroup eventLoopGroup,
      Class<? extends Channel> channelClass) throws FailedLogCloseException, IOException {
    super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix);
    this.eventLoopGroup = eventLoopGroup;
    this.channelClass = channelClass;
    Supplier<Boolean> hasConsumerTask;
    if (conf.getBoolean(ASYNC_WAL_USE_SHARED_EVENT_LOOP, DEFAULT_ASYNC_WAL_USE_SHARED_EVENT_LOOP)) {
      this.consumeExecutor = eventLoopGroup.next();
      if (consumeExecutor instanceof SingleThreadEventExecutor) {
        try {
          Field field = SingleThreadEventExecutor.class.getDeclaredField("taskQueue");
          field.setAccessible(true);
          Queue<?> queue = (Queue<?>) field.get(consumeExecutor);
          hasConsumerTask = () -> queue.peek() == consumer;
        } catch (Exception e) {
          LOG.warn("Can not get task queue of " + consumeExecutor +
            ", this is not necessary, just give up", e);
          hasConsumerTask = () -> false;
        }
      } else {
        hasConsumerTask = () -> false;
      }
    } else {
      ThreadPoolExecutor threadPool =
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryBuilder().setNameFormat("AsyncFSWAL-%d").setDaemon(true).build());
      hasConsumerTask = () -> threadPool.getQueue().peek() == consumer;
      this.consumeExecutor = threadPool;
    }

    this.hasConsumerTask = hasConsumerTask;
    int preallocatedEventCount =
      conf.getInt("hbase.regionserver.wal.disruptor.event.count", 1024 * 16);
    waitingConsumePayloads =
      RingBuffer.createMultiProducer(RingBufferTruck::new, preallocatedEventCount);
    waitingConsumePayloadsGatingSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    waitingConsumePayloads.addGatingSequences(waitingConsumePayloadsGatingSequence);

    // inrease the ringbuffer sequence so our txid is start from 1
    waitingConsumePayloads.publish(waitingConsumePayloads.next());
    waitingConsumePayloadsGatingSequence.set(waitingConsumePayloads.getCursor());

    batchSize = conf.getLong(WAL_BATCH_SIZE, DEFAULT_WAL_BATCH_SIZE);
    waitOnShutdownInSeconds = conf.getInt(ASYNC_WAL_WAIT_ON_SHUTDOWN_IN_SECONDS,
      DEFAULT_ASYNC_WAL_WAIT_ON_SHUTDOWN_IN_SECONDS);
    rollWriter();
  }

  private static boolean waitingRoll(int epochAndState) {
    return (epochAndState & 1) != 0;
  }

  private static boolean writerBroken(int epochAndState) {
    return ((epochAndState >>> 1) & 1) != 0;
  }

  private static int epoch(int epochAndState) {
    return epochAndState >>> 2;
  }

  // return whether we have successfully set readyForRolling to true.
  private boolean trySetReadyForRolling() {
    // Check without holding lock first. Usually we will just return here.
    // waitingRoll is volatile and unacedEntries is only accessed inside event loop so it is safe to
    // check them outside the consumeLock.
    if (!waitingRoll(epochAndState) || !unackedAppends.isEmpty()) {
      return false;
    }
    consumeLock.lock();
    try {
      // 1. a roll is requested
      // 2. all out-going entries have been acked(we have confirmed above).
      if (waitingRoll(epochAndState)) {
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

  private void syncFailed(long epochWhenSync, Throwable error) {
    LOG.warn("sync failed", error);
    boolean shouldRequestLogRoll = true;
    consumeLock.lock();
    try {
      int currentEpochAndState = epochAndState;
      if (epoch(currentEpochAndState) != epochWhenSync || writerBroken(currentEpochAndState)) {
        // this is not the previous writer which means we have already rolled the writer.
        // or this is still the current writer, but we have already marked it as broken and request
        // a roll.
        return;
      }
      this.epochAndState = currentEpochAndState | 0b10;
      if (waitingRoll(currentEpochAndState)) {
        readyForRolling = true;
        readyForRollingCond.signalAll();
        // this means we have already in the middle of a rollWriter so just tell the roller thread
        // that you can continue without requesting an extra log roll.
        shouldRequestLogRoll = false;
      }
    } finally {
      consumeLock.unlock();
    }
    for (Iterator<FSWALEntry> iter = unackedAppends.descendingIterator(); iter.hasNext();) {
      toWriteAppends.addFirst(iter.next());
    }
    highestUnsyncedTxid = highestSyncedTxid.get();
    if (shouldRequestLogRoll) {
      // request a roll.
      requestLogRoll();
    }
  }

  private void syncCompleted(AsyncWriter writer, long processedTxid, long startTimeNs) {
    highestSyncedTxid.set(processedTxid);
    for (Iterator<FSWALEntry> iter = unackedAppends.iterator(); iter.hasNext();) {
      if (iter.next().getTxid() <= processedTxid) {
        iter.remove();
      } else {
        break;
      }
    }
    postSync(System.nanoTime() - startTimeNs, finishSync(true));
    if (trySetReadyForRolling()) {
      // we have just finished a roll, then do not need to check for log rolling, the writer will be
      // closed soon.
      return;
    }
    if (writer.getLength() < logrollsize || rollRequested) {
      return;
    }
    rollRequested = true;
    requestLogRoll();
  }

  private void sync(AsyncWriter writer) {
    fileLengthAtLastSync = writer.getLength();
    long currentHighestProcessedAppendTxid = highestProcessedAppendTxid;
    highestProcessedAppendTxidAtLastSync = currentHighestProcessedAppendTxid;
    final long startTimeNs = System.nanoTime();
    final long epoch = (long) epochAndState >>> 2L;
    writer.sync().whenCompleteAsync((result, error) -> {
      if (error != null) {
        syncFailed(epoch, error);
      } else {
        syncCompleted(writer, currentHighestProcessedAppendTxid, startTimeNs);
      }
    }, consumeExecutor);
  }

  private void addTimeAnnotation(SyncFuture future, String annotation) {
    TraceUtil.addTimelineAnnotation(annotation);
    // TODO handle htrace API change, see HBASE-18895
    // future.setSpan(scope.getSpan());
  }

  private int finishSyncLowerThanTxid(long txid, boolean addSyncTrace) {
    int finished = 0;
    for (Iterator<SyncFuture> iter = syncFutures.iterator(); iter.hasNext();) {
      SyncFuture sync = iter.next();
      if (sync.getTxid() <= txid) {
        sync.done(txid, null);
        iter.remove();
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

  // try advancing the highestSyncedTxid as much as possible
  private int finishSync(boolean addSyncTrace) {
    if (unackedAppends.isEmpty()) {
      // All outstanding appends have been acked.
      if (toWriteAppends.isEmpty()) {
        // Also no appends that wait to be written out, then just finished all pending syncs.
        long maxSyncTxid = highestSyncedTxid.get();
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
        long doneTxid = lowestUnprocessedAppendTxid - 1;
        highestSyncedTxid.set(doneTxid);
        return finishSyncLowerThanTxid(doneTxid, addSyncTrace);
      }
    } else {
      // There are still unacked appends. So let's move the highestSyncedTxid to the txid of the
      // first unacked append minus 1.
      long lowestUnackedAppendTxid = unackedAppends.peek().getTxid();
      long doneTxid = Math.max(lowestUnackedAppendTxid - 1, highestSyncedTxid.get());
      highestSyncedTxid.set(doneTxid);
      return finishSyncLowerThanTxid(doneTxid, addSyncTrace);
    }
  }

  private void appendAndSync() {
    final AsyncWriter writer = this.writer;
    // maybe a sync request is not queued when we issue a sync, so check here to see if we could
    // finish some.
    finishSync(false);
    long newHighestProcessedAppendTxid = -1L;
    for (Iterator<FSWALEntry> iter = toWriteAppends.iterator(); iter.hasNext();) {
      FSWALEntry entry = iter.next();
      boolean appended;
      try {
        appended = append(writer, entry);
      } catch (IOException e) {
        throw new AssertionError("should not happen", e);
      }
      newHighestProcessedAppendTxid = entry.getTxid();
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
    if (newHighestProcessedAppendTxid > 0) {
      highestProcessedAppendTxid = newHighestProcessedAppendTxid;
    } else {
      newHighestProcessedAppendTxid = highestProcessedAppendTxid;
    }

    if (writer.getLength() - fileLengthAtLastSync >= batchSize) {
      // sync because buffer size limit.
      sync(writer);
      return;
    }
    if (writer.getLength() == fileLengthAtLastSync) {
      // we haven't written anything out, just advance the highestSyncedSequence since we may only
      // stamped some region sequence id.
      if (unackedAppends.isEmpty()) {
        highestSyncedTxid.set(highestProcessedAppendTxid);
        finishSync(false);
        trySetReadyForRolling();
      }
      return;
    }
    // reach here means that we have some unsynced data but haven't reached the batch size yet
    // but we will not issue a sync directly here even if there are sync requests because we may
    // have some new data in the ringbuffer, so let's just return here and delay the decision of
    // whether to issue a sync in the caller method.
  }

  private void consume() {
    consumeLock.lock();
    try {
      int currentEpochAndState = epochAndState;
      if (writerBroken(currentEpochAndState)) {
        return;
      }
      if (waitingRoll(currentEpochAndState)) {
        if (writer.getLength() > fileLengthAtLastSync) {
          // issue a sync
          sync(writer);
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
    for (long cursorBound = waitingConsumePayloads.getCursor(); nextCursor <= cursorBound;
      nextCursor++) {
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
          // we will give up consuming so if there are some unsynced data we need to issue a sync.
          if (writer.getLength() > fileLengthAtLastSync && !syncFutures.isEmpty() &&
            syncFutures.last().getTxid() > highestProcessedAppendTxidAtLastSync) {
            // no new data in the ringbuffer and we have at least one sync request
            sync(writer);
          }
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
    consumeExecutor.execute(consumer);
  }

  private boolean shouldScheduleConsumer() {
    int currentEpochAndState = epochAndState;
    if (writerBroken(currentEpochAndState) || waitingRoll(currentEpochAndState)) {
      return false;
    }
    return consumerScheduled.compareAndSet(false, true);
  }

  @Override
  public long append(RegionInfo hri, WALKeyImpl key, WALEdit edits, boolean inMemstore)
      throws IOException {
    long txid =
      stampSequenceIdAndPublishToRingBuffer(hri, key, edits, inMemstore, waitingConsumePayloads);
    if (shouldScheduleConsumer()) {
      consumeExecutor.execute(consumer);
    }
    return txid;
  }

  @Override
  public void sync() throws IOException {
    try (TraceScope scope = TraceUtil.createTrace("AsyncFSWAL.sync")) {
      long txid = waitingConsumePayloads.next();
      SyncFuture future;
      try {
        future = getSyncFuture(txid);
        RingBufferTruck truck = waitingConsumePayloads.get(txid);
        truck.load(future);
      } finally {
        waitingConsumePayloads.publish(txid);
      }
      if (shouldScheduleConsumer()) {
        consumeExecutor.execute(consumer);
      }
      blockOnSync(future);
    }
  }

  @Override
  public void sync(long txid) throws IOException {
    if (highestSyncedTxid.get() >= txid) {
      return;
    }
    try (TraceScope scope = TraceUtil.createTrace("AsyncFSWAL.sync")) {
      // here we do not use ring buffer sequence as txid
      long sequence = waitingConsumePayloads.next();
      SyncFuture future;
      try {
        future = getSyncFuture(txid);
        RingBufferTruck truck = waitingConsumePayloads.get(sequence);
        truck.load(future);
      } finally {
        waitingConsumePayloads.publish(sequence);
      }
      if (shouldScheduleConsumer()) {
        consumeExecutor.execute(consumer);
      }
      blockOnSync(future);
    }
  }

  protected final AsyncWriter createAsyncWriter(FileSystem fs, Path path) throws IOException {
    return AsyncFSWALProvider.createAsyncWriter(conf, fs, path, false, this.blocksize,
      eventLoopGroup, channelClass);
  }

  @Override
  protected AsyncWriter createWriterInstance(Path path) throws IOException {
    return createAsyncWriter(fs, path);
  }

  private void waitForSafePoint() {
    consumeLock.lock();
    try {
      int currentEpochAndState = epochAndState;
      if (writerBroken(currentEpochAndState) || this.writer == null) {
        return;
      }
      consumerScheduled.set(true);
      epochAndState = currentEpochAndState | 1;
      readyForRolling = false;
      consumeExecutor.execute(consumer);
      while (!readyForRolling) {
        readyForRollingCond.awaitUninterruptibly();
      }
    } finally {
      consumeLock.unlock();
    }
  }

  protected final long closeWriter(AsyncWriter writer) {
    if (writer != null) {
      long fileLength = writer.getLength();
      closeExecutor.execute(() -> {
        try {
          writer.close();
        } catch (IOException e) {
          LOG.warn("close old writer failed", e);
        }
      });
      return fileLength;
    } else {
      return 0L;
    }
  }

  @Override
  protected void doReplaceWriter(Path oldPath, Path newPath, AsyncWriter nextWriter)
      throws IOException {
    Preconditions.checkNotNull(nextWriter);
    waitForSafePoint();
    long oldFileLen = closeWriter(this.writer);
    logRollAndSetupWalProps(oldPath, newPath, oldFileLen);
    this.writer = nextWriter;
    if (nextWriter instanceof AsyncProtobufLogWriter) {
      this.fsOut = ((AsyncProtobufLogWriter) nextWriter).getOutput();
    }
    this.fileLengthAtLastSync = nextWriter.getLength();
    this.rollRequested = false;
    this.highestProcessedAppendTxidAtLastSync = 0L;
    consumeLock.lock();
    try {
      consumerScheduled.set(true);
      int currentEpoch = epochAndState >>> 2;
      int nextEpoch = currentEpoch == MAX_EPOCH ? 0 : currentEpoch + 1;
      // set a new epoch and also clear waitingRoll and writerBroken
      this.epochAndState = nextEpoch << 2;
      consumeExecutor.execute(consumer);
    } finally {
      consumeLock.unlock();
    }
  }

  @Override
  protected void doShutdown() throws IOException {
    waitForSafePoint();
    closeWriter(this.writer);
    closeExecutor.shutdown();
    try {
      if (!closeExecutor.awaitTermination(waitOnShutdownInSeconds, TimeUnit.SECONDS)) {
        LOG.error("We have waited " + waitOnShutdownInSeconds + " seconds but" +
          " the close of async writer doesn't complete." +
          "Please check the status of underlying filesystem" +
          " or increase the wait time by the config \"" + ASYNC_WAL_WAIT_ON_SHUTDOWN_IN_SECONDS +
          "\"");
      }
    } catch (InterruptedException e) {
      LOG.error("The wait for close of async writer is interrupted");
      Thread.currentThread().interrupt();
    }
    IOException error = new IOException("WAL has been closed");
    long nextCursor = waitingConsumePayloadsGatingSequence.get() + 1;
    // drain all the pending sync requests
    for (long cursorBound = waitingConsumePayloads.getCursor(); nextCursor <= cursorBound;
      nextCursor++) {
      if (!waitingConsumePayloads.isPublished(nextCursor)) {
        break;
      }
      RingBufferTruck truck = waitingConsumePayloads.get(nextCursor);
      switch (truck.type()) {
        case SYNC:
          syncFutures.add(truck.unloadSync());
          break;
        default:
          break;
      }
    }
    // and fail them
    syncFutures.forEach(f -> f.done(f.getTxid(), error));
    if (!(consumeExecutor instanceof EventLoop)) {
      consumeExecutor.shutdown();
    }
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

  @Override
  protected boolean doCheckLogLowReplication() {
    // not like FSHLog, AsyncFSOutput will fail immediately if there are errors writing to DNs, so
    // typically there is no 'low replication' state, only a 'broken' state.
    AsyncFSOutput output = this.fsOut;
    return output != null && output.isBroken();
  }
}
