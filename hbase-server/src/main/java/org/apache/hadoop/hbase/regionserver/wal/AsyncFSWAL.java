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

import static org.apache.hadoop.hbase.HConstants.REGION_SERVER_HANDLER_COUNT;
import static org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper.shouldRetryCreate;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

/**
 * An asynchronous implementation of FSWAL.
 * <p>
 * Here 'waitingConsumePayloads' acts as the RingBuffer in FSHLog. We do not use RingBuffer here
 * because RingBuffer need an exclusive thread to consume the entries in it, and here we want to run
 * the append and sync operation inside EventLoop. We can not use EventLoop as the RingBuffer's
 * executor otherwise the EventLoop can not process any other events such as socket read and write.
 * <p>
 * For append, we process it as follow:
 * <ol>
 * <li>In the caller thread(typically, in the rpc handler thread):
 * <ol>
 * <li>Lock 'waitingConsumePayloads', bump nextTxid, and insert the entry to
 * 'waitingConsumePayloads'.</li>
 * <li>Schedule the consumer task if needed. See {@link #shouldScheduleConsumer()} for more details.
 * </li>
 * </ol>
 * </li>
 * <li>In the consumer task(in the EventLoop thread)
 * <ol>
 * <li>Poll the entry from 'waitingConsumePayloads' and insert it into 'waitingAppendEntries'</li>
 * <li>Poll the entry from 'waitingAppendEntries', append it to the AsyncWriter, and insert it into
 * 'unackedEntries'</li>
 * <li>If the buffered size reaches {@link #batchSize}, or there is a sync request, then we call
 * sync on the AsyncWriter.</li>
 * <li>In the callback methods(CompletionHandler):
 * <ul>
 * <li>If succeeded, poll the entry from 'unackedEntries' and drop it.</li>
 * <li>If failed, add all the entries in 'unackedEntries' back to 'waitingAppendEntries' and wait
 * for writing them again.</li>
 * </ul>
 * </li>
 * </ol>
 * </li>
 * </ol>
 * For sync, the processing stages are almost same except that if it is not assigned with a new
 * 'txid', we just assign the previous 'txid' to it without bumping the 'nextTxid'. And different
 * from FSHLog, we will open a new writer and rewrite unacked entries to the new writer and sync
 * again if we hit a sync error.
 * <p>
 * Here we only describe the logic of doReplaceWriter. The main logic of rollWriter is same with
 * FSHLog.<br>
 * For a normal roll request(for example, we have reached the log roll size):
 * <ol>
 * <li>In the log roller thread, we add a roll payload to 'waitingConsumePayloads', and then wait on
 * the rollPromise(see {@link #waitForSafePoint()}).</li>
 * <li>In the consumer thread, we will stop polling entries from 'waitingConsumePayloads' if we hit
 * a Payload which contains a roll request.</li>
 * <li>Append all entries to current writer, issue a sync request if possible.</li>
 * <li>If sync succeeded, check if we could finish a roll request. There 3 conditions:
 * <ul>
 * <li>'rollPromise' is not null which means we have a pending roll request.</li>
 * <li>'waitingAppendEntries' is empty.</li>
 * <li>'unackedEntries' is empty.</li>
 * </ul>
 * </li>
 * <li>Back to the log roller thread, now we can confirm that there are no out-going entries, i.e.,
 * we reach a safe point. So it is safe to replace old writer with new writer now.</li>
 * <li>Acquire 'waitingConsumePayloads' lock, set 'writerBroken' and 'waitingRoll' to false, cancel
 * log roller exit checker if any(see the comments in the 'failed' method of the sync
 * CompletionHandler to see why we need a checker here).</li>
 * <li>Schedule the consumer task if needed.</li>
 * <li>Schedule a background task to close the old writer.</li>
 * </ol>
 * For a broken writer roll request, the only difference is that we can bypass the wait for safe
 * point stage. See the comments in the 'failed' method of the sync CompletionHandler for more
 * details.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class AsyncFSWAL extends AbstractFSWAL<AsyncWriter> {

  private static final Log LOG = LogFactory.getLog(AsyncFSWAL.class);

  public static final String WAL_BATCH_SIZE = "hbase.wal.batch.size";
  public static final long DEFAULT_WAL_BATCH_SIZE = 64L * 1024;

  public static final String ASYNC_WAL_CREATE_MAX_RETRIES = "hbase.wal.async.create.retries";
  public static final int DEFAULT_ASYNC_WAL_CREATE_MAX_RETRIES = 10;

  public static final String ASYNC_WAL_LOG_ROLLER_EXITED_CHECK_INTERVAL_MS =
      "hbase.wal.async.logroller.exited.check.interval.ms";
  public static final long DEFAULT_ASYNC_WAL_LOG_ROLLER_EXITED_CHECK_INTERVAL_MS = 1000;

  /**
   * Carry things that we want to pass to the consume task in event loop. Only one field can be
   * non-null.
   * <p>
   * TODO: need to unify this and {@link RingBufferTruck}. There are mostly the same thing.
   */
  private static final class Payload {

    // a wal entry which need to be appended
    public final FSWALEntry entry;

    // indicate that we need to sync our wal writer.
    public final SyncFuture sync;

    // incidate that we want to roll the writer.
    public final Promise<Void> roll;

    public Payload(FSWALEntry entry) {
      this.entry = entry;
      this.sync = null;
      this.roll = null;
    }

    public Payload(SyncFuture sync) {
      this.entry = null;
      this.sync = sync;
      this.roll = null;
    }

    public Payload(Promise<Void> roll) {
      this.entry = null;
      this.sync = null;
      this.roll = roll;
    }

    @Override
    public String toString() {
      return "Payload [entry=" + entry + ", sync=" + sync + ", roll=" + roll + "]";
    }
  }

  private final EventLoop eventLoop;

  private final Deque<Payload> waitingConsumePayloads;

  // like the ringbuffer sequence. Every FSWALEntry and SyncFuture will be assigned a txid and
  // then added to waitingConsumePayloads.
  private long nextTxid = 1L;

  private boolean consumerScheduled;

  // new writer is created and we are waiting for old writer to be closed.
  private boolean waitingRoll;

  // writer is broken and rollWriter is needed.
  private boolean writerBroken;

  private final long batchSize;

  private final int createMaxRetries;

  private final long logRollerExitedCheckIntervalMs;

  private final ExecutorService closeExecutor = Executors.newCachedThreadPool(
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Close-WAL-Writer-%d").build());

  private volatile AsyncFSOutput fsOut;

  private final Deque<FSWALEntry> waitingAppendEntries = new ArrayDeque<FSWALEntry>();

  private final Deque<FSWALEntry> unackedEntries = new ArrayDeque<FSWALEntry>();

  private final PriorityQueue<SyncFuture> syncFutures =
      new PriorityQueue<SyncFuture>(11, SEQ_COMPARATOR);

  private Promise<Void> rollPromise;

  // the highest txid of WAL entries being processed
  private long highestProcessedTxid;

  // file length when we issue last sync request on the writer
  private long fileLengthAtLastSync;

  private volatile boolean logRollerExited;

  private final class LogRollerExitedChecker implements Runnable {

    private boolean cancelled;

    private ScheduledFuture<?> future;

    public synchronized void setFuture(ScheduledFuture<?> future) {
      this.future = future;
    }

    @Override
    public void run() {
      if (!logRollerExited) {
        return;
      }
      // rollWriter is called in the log roller thread, and logRollerExited will be set just before
      // the log rolled exit. So here we can confirm that no one could cancel us if the 'canceled'
      // check passed. So it is safe to release the lock after checking 'canceled' flag.
      synchronized (this) {
        if (cancelled) {
          return;
        }
      }
      unackedEntries.clear();
      waitingAppendEntries.clear();
      IOException error = new IOException("sync failed but log roller exited");
      for (SyncFuture future; (future = syncFutures.peek()) != null;) {
        future.done(highestProcessedTxid, error);
        syncFutures.remove();
      }
      synchronized (waitingConsumePayloads) {
        for (Payload p : waitingConsumePayloads) {
          if (p.entry != null) {
            try {
              p.entry.stampRegionSequenceId();
            } catch (IOException e) {
              throw new AssertionError("should not happen", e);
            }
          } else if (p.sync != null) {
            p.sync.done(nextTxid, error);
          }
        }
        waitingConsumePayloads.clear();
      }
    }

    public synchronized void cancel() {
      future.cancel(false);
      cancelled = true;
    }
  }

  private LogRollerExitedChecker logRollerExitedChecker;

  public AsyncFSWAL(FileSystem fs, Path rootDir, String logDir, String archiveDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
      String prefix, String suffix, EventLoop eventLoop)
      throws FailedLogCloseException, IOException {
    super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix);
    this.eventLoop = eventLoop;
    int maxHandlersCount = conf.getInt(REGION_SERVER_HANDLER_COUNT, 200);
    waitingConsumePayloads = new ArrayDeque<Payload>(maxHandlersCount * 3);
    batchSize = conf.getLong(WAL_BATCH_SIZE, DEFAULT_WAL_BATCH_SIZE);
    createMaxRetries =
        conf.getInt(ASYNC_WAL_CREATE_MAX_RETRIES, DEFAULT_ASYNC_WAL_CREATE_MAX_RETRIES);
    logRollerExitedCheckIntervalMs = conf.getLong(ASYNC_WAL_LOG_ROLLER_EXITED_CHECK_INTERVAL_MS,
      DEFAULT_ASYNC_WAL_LOG_ROLLER_EXITED_CHECK_INTERVAL_MS);
    rollWriter();
  }

  private void tryFinishRoll() {
    // 1. a roll is requested
    // 2. we have written out all entries before the roll point.
    // 3. all entries have been acked.
    if (rollPromise != null && waitingAppendEntries.isEmpty() && unackedEntries.isEmpty()) {
      rollPromise.trySuccess(null);
      rollPromise = null;
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
    synchronized (waitingConsumePayloads) {
      if (writerBroken) {
        return;
      }
      // schedule a periodical task to check if log roller is exited. Otherwise the the sync
      // request maybe blocked forever since we are still waiting for a new writer to write the
      // pending data and sync it...
      logRollerExitedChecker = new LogRollerExitedChecker();
      // we are currently in the EventLoop thread, so it is safe to set the future after
      // schedule it since the task can not be executed before we release the thread.
      logRollerExitedChecker.setFuture(eventLoop.scheduleAtFixedRate(logRollerExitedChecker,
        logRollerExitedCheckIntervalMs, logRollerExitedCheckIntervalMs, TimeUnit.MILLISECONDS));
      writerBroken = true;
    }
    for (Iterator<FSWALEntry> iter = unackedEntries.descendingIterator(); iter.hasNext();) {
      waitingAppendEntries.addFirst(iter.next());
    }
    highestUnsyncedTxid = highestSyncedTxid.get();
    if (rollPromise != null) {
      rollPromise.trySuccess(null);
      rollPromise = null;
      return;
    }
    // request a roll.
    if (!rollWriterLock.tryLock()) {
      return;
    }
    try {
      requestLogRoll();
    } finally {
      rollWriterLock.unlock();
    }
  }

  private void syncCompleted(AsyncWriter writer, long processedTxid, long startTimeNs) {
    highestSyncedTxid.set(processedTxid);
    int syncCount = finishSync(true);
    for (Iterator<FSWALEntry> iter = unackedEntries.iterator(); iter.hasNext();) {
      if (iter.next().getTxid() <= processedTxid) {
        iter.remove();
      } else {
        break;
      }
    }
    postSync(System.nanoTime() - startTimeNs, syncCount);
    tryFinishRoll();
    if (!rollWriterLock.tryLock()) {
      return;
    }
    try {
      if (writer.getLength() >= logrollsize) {
        requestLogRoll();
      }
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

  private int finishSync(boolean addSyncTrace) {
    long doneTxid = highestSyncedTxid.get();
    int finished = 0;
    for (SyncFuture future; (future = syncFutures.peek()) != null;) {
      if (future.getTxid() <= doneTxid) {
        future.done(doneTxid, null);
        syncFutures.remove();
        finished++;
        addTimeAnnotation(future, "writer synced");
      } else {
        break;
      }
    }
    return finished;
  }

  private void consume() {
    final AsyncWriter writer = this.writer;
    // maybe a sync request is not queued when we issue a sync, so check here to see if we could
    // finish some.
    finishSync(false);
    long newHighestProcessedTxid = -1L;
    for (Iterator<FSWALEntry> iter = waitingAppendEntries.iterator(); iter.hasNext();) {
      FSWALEntry entry = iter.next();
      boolean appended;
      try {
        appended = append(writer, entry);
      } catch (IOException e) {
        throw new AssertionError("should not happen", e);
      }
      newHighestProcessedTxid = entry.getTxid();
      iter.remove();
      if (appended) {
        unackedEntries.addLast(entry);
        if (writer.getLength() - fileLengthAtLastSync >= batchSize) {
          break;
        }
      }
    }
    // if we have a newer transaction id, update it.
    // otherwise, use the previous transaction id.
    if (newHighestProcessedTxid > 0) {
      highestProcessedTxid = newHighestProcessedTxid;
    } else {
      newHighestProcessedTxid = highestProcessedTxid;
    }
    if (writer.getLength() - fileLengthAtLastSync >= batchSize) {
      // sync because buffer size limit.
      sync(writer, newHighestProcessedTxid);
    } else if ((!syncFutures.isEmpty() || rollPromise != null)
        && writer.getLength() > fileLengthAtLastSync) {
      // first we should have at least one sync request or a roll request
      // second we should have some unsynced data.
      sync(writer, newHighestProcessedTxid);
    } else if (writer.getLength() == fileLengthAtLastSync) {
      // we haven't written anything out, just advance the highestSyncedSequence since we may only
      // stamped some region sequence id.
      highestSyncedTxid.set(newHighestProcessedTxid);
      finishSync(false);
      tryFinishRoll();
    }
  }

  private static final Comparator<SyncFuture> SEQ_COMPARATOR = (o1, o2) -> {
    int c = Long.compare(o1.getTxid(), o2.getTxid());
    return c != 0 ? c : Integer.compare(System.identityHashCode(o1), System.identityHashCode(o2));
  };

  private final Runnable consumer = new Runnable() {

    @Override
    public void run() {
      synchronized (waitingConsumePayloads) {
        assert consumerScheduled;
        if (writerBroken) {
          // waiting for reschedule after rollWriter.
          consumerScheduled = false;
          return;
        }
        if (waitingRoll) {
          // we may have toWriteEntries if the consume method does not write all pending entries
          // out, this is usually happen if we have too many toWriteEntries that exceeded the
          // batchSize limit.
          if (waitingAppendEntries.isEmpty()) {
            consumerScheduled = false;
            return;
          }
        } else {
          for (Payload p; (p = waitingConsumePayloads.pollFirst()) != null;) {
            if (p.entry != null) {
              waitingAppendEntries.addLast(p.entry);
            } else if (p.sync != null) {
              syncFutures.add(p.sync);
            } else {
              rollPromise = p.roll;
              waitingRoll = true;
              break;
            }
          }
        }
      }
      consume();
      synchronized (waitingConsumePayloads) {
        if (waitingRoll) {
          if (waitingAppendEntries.isEmpty()) {
            consumerScheduled = false;
            return;
          }
        } else {
          if (waitingConsumePayloads.isEmpty() && waitingAppendEntries.isEmpty()) {
            consumerScheduled = false;
            return;
          }
        }
      }
      // reschedule if we still have something to write.
      eventLoop.execute(this);
    }
  };

  private boolean shouldScheduleConsumer() {
    if (writerBroken || waitingRoll) {
      return false;
    }
    if (consumerScheduled) {
      return false;
    }
    consumerScheduled = true;
    return true;
  }

  @Override
  public long append(HRegionInfo hri, WALKey key, WALEdit edits, boolean inMemstore)
      throws IOException {
    boolean scheduleTask;
    long txid;
    synchronized (waitingConsumePayloads) {
      if (this.closed) {
        throw new IOException("Cannot append; log is closed");
      }
      txid = nextTxid++;
      FSWALEntry entry = new FSWALEntry(txid, key, edits, hri, inMemstore);
      scheduleTask = shouldScheduleConsumer();
      waitingConsumePayloads.add(new Payload(entry));
    }
    if (scheduleTask) {
      eventLoop.execute(consumer);
    }
    return txid;
  }

  @Override
  public void sync() throws IOException {
    TraceScope scope = Trace.startSpan("AsyncFSWAL.sync");
    try {
      SyncFuture future;
      boolean scheduleTask;
      synchronized (waitingConsumePayloads) {
        scheduleTask = shouldScheduleConsumer();
        future = getSyncFuture(nextTxid - 1, scope.detach());
        waitingConsumePayloads.addLast(new Payload(future));
      }
      if (scheduleTask) {
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
      SyncFuture future = getSyncFuture(txid, scope.detach());
      boolean scheduleTask;
      synchronized (waitingConsumePayloads) {
        scheduleTask = shouldScheduleConsumer();
        waitingConsumePayloads.addLast(new Payload(future));
      }
      if (scheduleTask) {
        eventLoop.execute(consumer);
      }
      scope = Trace.continueSpan(blockOnSync(future));
    } finally {
      assert scope == NullScope.INSTANCE || !scope.isDetached();
      scope.close();
    }
  }

  @Override
  public void logRollerExited() {
    logRollerExited = true;
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
    Future<Void> roll;
    boolean scheduleTask;
    synchronized (waitingConsumePayloads) {
      if (!writerBroken && this.writer != null) {
        Promise<Void> promise = eventLoop.newPromise();
        if (consumerScheduled) {
          scheduleTask = false;
        } else {
          scheduleTask = consumerScheduled = true;
        }
        waitingConsumePayloads.addLast(new Payload(promise));
        roll = promise;
      } else {
        roll = eventLoop.newSucceededFuture(null);
        scheduleTask = false;
      }
    }
    if (scheduleTask) {
      eventLoop.execute(consumer);
    }
    roll.awaitUninterruptibly();
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
    boolean scheduleTask;
    synchronized (waitingConsumePayloads) {
      writerBroken = waitingRoll = false;
      if (logRollerExitedChecker != null) {
        logRollerExitedChecker.cancel();
        logRollerExitedChecker = null;
      }
      if (consumerScheduled) {
        scheduleTask = false;
      } else {
        if (waitingConsumePayloads.isEmpty() && waitingAppendEntries.isEmpty()) {
          scheduleTask = false;
        } else {
          scheduleTask = consumerScheduled = true;
        }
      }
    }
    if (scheduleTask) {
      eventLoop.execute(consumer);
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
