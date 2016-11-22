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

import com.google.common.annotations.VisibleForTesting;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.FSHLogProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALPrettyPrinter;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.htrace.NullScope;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

/**
 * The default implementation of FSWAL.
 */
@InterfaceAudience.Private
public class FSHLog extends AbstractFSWAL<Writer> {
  // IMPLEMENTATION NOTES:
  //
  // At the core is a ring buffer. Our ring buffer is the LMAX Disruptor. It tries to
  // minimize synchronizations and volatile writes when multiple contending threads as is the case
  // here appending and syncing on a single WAL. The Disruptor is configured to handle multiple
  // producers but it has one consumer only (the producers in HBase are IPC Handlers calling append
  // and then sync). The single consumer/writer pulls the appends and syncs off the ring buffer.
  // When a handler calls sync, it is given back a future. The producer 'blocks' on the future so
  // it does not return until the sync completes. The future is passed over the ring buffer from
  // the producer/handler to the consumer thread where it does its best to batch up the producer
  // syncs so one WAL sync actually spans multiple producer sync invocations. How well the
  // batching works depends on the write rate; i.e. we tend to batch more in times of
  // high writes/syncs.
  //
  // Calls to append now also wait until the append has been done on the consumer side of the
  // disruptor. We used to not wait but it makes the implementation easier to grok if we have
  // the region edit/sequence id after the append returns.
  //
  // TODO: Handlers need to coordinate appending AND syncing. Can we have the threads contend
  // once only? Probably hard given syncs take way longer than an append.
  //
  // The consumer threads pass the syncs off to multiple syncing threads in a round robin fashion
  // to ensure we keep up back-to-back FS sync calls (FS sync calls are the long poll writing the
  // WAL). The consumer thread passes the futures to the sync threads for it to complete
  // the futures when done.
  //
  // The 'sequence' in the below is the sequence of the append/sync on the ringbuffer. It
  // acts as a sort-of transaction id. It is always incrementing.
  //
  // The RingBufferEventHandler class hosts the ring buffer consuming code. The threads that
  // do the actual FS sync are implementations of SyncRunner. SafePointZigZagLatch is a
  // synchronization class used to halt the consumer at a safe point -- just after all outstanding
  // syncs and appends have completed -- so the log roller can swap the WAL out under it.
  //
  // We use ring buffer sequence as txid of FSWALEntry and SyncFuture.
  private static final Log LOG = LogFactory.getLog(FSHLog.class);

  /**
   * The nexus at which all incoming handlers meet. Does appends and sync with an ordering. Appends
   * and syncs are each put on the ring which means handlers need to smash up against the ring twice
   * (can we make it once only? ... maybe not since time to append is so different from time to sync
   * and sometimes we don't want to sync or we want to async the sync). The ring is where we make
   * sure of our ordering and it is also where we do batching up of handler sync calls.
   */
  private final Disruptor<RingBufferTruck> disruptor;

  /**
   * This fellow is run by the above appendExecutor service but it is all about batching up appends
   * and syncs; it may shutdown without cleaning out the last few appends or syncs. To guard against
   * this, keep a reference to this handler and do explicit close on way out to make sure all
   * flushed out before we exit.
   */
  private final RingBufferEventHandler ringBufferEventHandler;

  /**
   * FSDataOutputStream associated with the current SequenceFile.writer
   */
  private FSDataOutputStream hdfs_out;

  // All about log rolling if not enough replicas outstanding.

  // Minimum tolerable replicas, if the actual value is lower than it, rollWriter will be triggered
  private final int minTolerableReplication;

  // If live datanode count is lower than the default replicas value,
  // RollWriter will be triggered in each sync(So the RollWriter will be
  // triggered one by one in a short time). Using it as a workaround to slow
  // down the roll frequency triggered by checkLowReplication().
  private final AtomicInteger consecutiveLogRolls = new AtomicInteger(0);

  private final int lowReplicationRollLimit;

  // If consecutiveLogRolls is larger than lowReplicationRollLimit,
  // then disable the rolling in checkLowReplication().
  // Enable it if the replications recover.
  private volatile boolean lowReplicationRollEnabled = true;

  /** Number of log close errors tolerated before we abort */
  private final int closeErrorsTolerated;

  private final AtomicInteger closeErrorCount = new AtomicInteger();

  /**
   * Exception handler to pass the disruptor ringbuffer. Same as native implementation only it logs
   * using our logger instead of java native logger.
   */
  static class RingBufferExceptionHandler implements ExceptionHandler<RingBufferTruck> {

    @Override
    public void handleEventException(Throwable ex, long sequence, RingBufferTruck event) {
      LOG.error("Sequence=" + sequence + ", event=" + event, ex);
      throw new RuntimeException(ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
      LOG.error(ex);
      throw new RuntimeException(ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
      LOG.error(ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Constructor.
   * @param fs filesystem handle
   * @param root path for stored and archived wals
   * @param logDir dir where wals are stored
   * @param conf configuration to use
   */
  public FSHLog(final FileSystem fs, final Path root, final String logDir, final Configuration conf)
      throws IOException {
    this(fs, root, logDir, HConstants.HREGION_OLDLOGDIR_NAME, conf, null, true, null, null);
  }

  /**
   * Create an edit log at the given <code>dir</code> location. You should never have to load an
   * existing log. If there is a log at startup, it should have already been processed and deleted
   * by the time the WAL object is started up.
   * @param fs filesystem handle
   * @param rootDir path to where logs and oldlogs
   * @param logDir dir where wals are stored
   * @param archiveDir dir where wals are archived
   * @param conf configuration to use
   * @param listeners Listeners on WAL events. Listeners passed here will be registered before we do
   *          anything else; e.g. the Constructor {@link #rollWriter()}.
   * @param failIfWALExists If true IOException will be thrown if files related to this wal already
   *          exist.
   * @param prefix should always be hostname and port in distributed env and it will be URL encoded
   *          before being used. If prefix is null, "wal" will be used
   * @param suffix will be url encoded. null is treated as empty. non-empty must start with
   *          {@link org.apache.hadoop.hbase.wal.AbstractFSWALProvider#WAL_FILE_NAME_DELIMITER}
   */
  public FSHLog(final FileSystem fs, final Path rootDir, final String logDir,
      final String archiveDir, final Configuration conf, final List<WALActionsListener> listeners,
      final boolean failIfWALExists, final String prefix, final String suffix) throws IOException {
    super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix);
    this.minTolerableReplication = conf.getInt("hbase.regionserver.hlog.tolerable.lowreplication",
      FSUtils.getDefaultReplication(fs, this.walDir));
    this.lowReplicationRollLimit = conf.getInt("hbase.regionserver.hlog.lowreplication.rolllimit",
      5);
    this.closeErrorsTolerated = conf.getInt("hbase.regionserver.logroll.errors.tolerated", 0);

    // rollWriter sets this.hdfs_out if it can.
    rollWriter();

    // This is the 'writer' -- a single threaded executor. This single thread 'consumes' what is
    // put on the ring buffer.
    String hostingThreadName = Thread.currentThread().getName();
    // Using BlockingWaitStrategy. Stuff that is going on here takes so long it makes no sense
    // spinning as other strategies do.
    this.disruptor = new Disruptor<RingBufferTruck>(RingBufferTruck::new,
        getPreallocatedEventCount(), Threads.getNamedThreadFactory(hostingThreadName + ".append"),
        ProducerType.MULTI, new BlockingWaitStrategy());
    // Advance the ring buffer sequence so that it starts from 1 instead of 0,
    // because SyncFuture.NOT_DONE = 0.
    this.disruptor.getRingBuffer().next();
    int maxHandlersCount = conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT, 200);
    this.ringBufferEventHandler = new RingBufferEventHandler(
        conf.getInt("hbase.regionserver.hlog.syncer.count", 5), maxHandlersCount);
    this.disruptor.setDefaultExceptionHandler(new RingBufferExceptionHandler());
    this.disruptor.handleEventsWith(new RingBufferEventHandler[] { this.ringBufferEventHandler });
    // Starting up threads in constructor is a no no; Interface should have an init call.
    this.disruptor.start();
  }

  /**
   * Currently, we need to expose the writer's OutputStream to tests so that they can manipulate the
   * default behavior (such as setting the maxRecoveryErrorCount value for example (see
   * {@link AbstractTestWALReplay#testReplayEditsWrittenIntoWAL()}). This is done using reflection
   * on the underlying HDFS OutputStream. NOTE: This could be removed once Hadoop1 support is
   * removed.
   * @return null if underlying stream is not ready.
   */
  @VisibleForTesting
  OutputStream getOutputStream() {
    FSDataOutputStream fsdos = this.hdfs_out;
    return fsdos != null ? fsdos.getWrappedStream() : null;
  }

  /**
   * Run a sync after opening to set up the pipeline.
   */
  private void preemptiveSync(final ProtobufLogWriter nextWriter) {
    long startTimeNanos = System.nanoTime();
    try {
      nextWriter.sync();
      postSync(System.nanoTime() - startTimeNanos, 0);
    } catch (IOException e) {
      // optimization failed, no need to abort here.
      LOG.warn("pre-sync failed but an optimization so keep going", e);
    }
  }

  /**
   * This method allows subclasses to inject different writers without having to extend other
   * methods like rollWriter().
   * @return Writer instance
   */
  @Override
  protected Writer createWriterInstance(final Path path) throws IOException {
    Writer writer = FSHLogProvider.createWriter(conf, fs, path, false);
    if (writer instanceof ProtobufLogWriter) {
      preemptiveSync((ProtobufLogWriter) writer);
    }
    return writer;
  }

  /**
   * Used to manufacture race condition reliably. For testing only.
   * @see #beforeWaitOnSafePoint()
   */
  @VisibleForTesting
  protected void afterCreatingZigZagLatch() {
  }

  /**
   * @see #afterCreatingZigZagLatch()
   */
  @VisibleForTesting
  protected void beforeWaitOnSafePoint() {
  };

  @Override
  protected void doAppend(Writer writer, FSWALEntry entry) throws IOException {
    writer.append(entry);
  }

  @Override
  protected long doReplaceWriter(Path oldPath, Path newPath, Writer nextWriter) throws IOException {
    // Ask the ring buffer writer to pause at a safe point. Once we do this, the writer
    // thread will eventually pause. An error hereafter needs to release the writer thread
    // regardless -- hence the finally block below. Note, this method is called from the FSHLog
    // constructor BEFORE the ring buffer is set running so it is null on first time through
    // here; allow for that.
    SyncFuture syncFuture = null;
    SafePointZigZagLatch zigzagLatch = null;
    long sequence = -1L;
    if (this.ringBufferEventHandler != null) {
      // Get sequence first to avoid dead lock when ring buffer is full
      // Considering below sequence
      // 1. replaceWriter is called and zigzagLatch is initialized
      // 2. ringBufferEventHandler#onEvent is called and arrives at #attainSafePoint(long) then wait
      // on safePointReleasedLatch
      // 3. Since ring buffer is full, if we get sequence when publish sync, the replaceWriter
      // thread will wait for the ring buffer to be consumed, but the only consumer is waiting
      // replaceWriter thread to release safePointReleasedLatch, which causes a deadlock
      sequence = getSequenceOnRingBuffer();
      zigzagLatch = this.ringBufferEventHandler.attainSafePoint();
    }
    afterCreatingZigZagLatch();
    long oldFileLen = 0L;
    try {
      // Wait on the safe point to be achieved. Send in a sync in case nothing has hit the
      // ring buffer between the above notification of writer that we want it to go to
      // 'safe point' and then here where we are waiting on it to attain safe point. Use
      // 'sendSync' instead of 'sync' because we do not want this thread to block waiting on it
      // to come back. Cleanup this syncFuture down below after we are ready to run again.
      try {
        if (zigzagLatch != null) {
          // use assert to make sure no change breaks the logic that
          // sequence and zigzagLatch will be set together
          assert sequence > 0L : "Failed to get sequence from ring buffer";
          Trace.addTimelineAnnotation("awaiting safepoint");
          syncFuture = zigzagLatch.waitSafePoint(publishSyncOnRingBuffer(sequence));
        }
      } catch (FailedSyncBeforeLogCloseException e) {
        // If unflushed/unsynced entries on close, it is reason to abort.
        if (isUnflushedEntries()) {
          throw e;
        }
        LOG.warn(
          "Failed sync-before-close but no outstanding appends; closing WAL" + e.getMessage());
      }
      // It is at the safe point. Swap out writer from under the blocked writer thread.
      // TODO: This is close is inline with critical section. Should happen in background?
      if (this.writer != null) {
        oldFileLen = this.writer.getLength();
        try {
          Trace.addTimelineAnnotation("closing writer");
          this.writer.close();
          Trace.addTimelineAnnotation("writer closed");
          this.closeErrorCount.set(0);
        } catch (IOException ioe) {
          int errors = closeErrorCount.incrementAndGet();
          if (!isUnflushedEntries() && (errors <= this.closeErrorsTolerated)) {
            LOG.warn("Riding over failed WAL close of " + oldPath + ", cause=\"" + ioe.getMessage()
                + "\", errors=" + errors
                + "; THIS FILE WAS NOT CLOSED BUT ALL EDITS SYNCED SO SHOULD BE OK");
          } else {
            throw ioe;
          }
        }
      }
      this.writer = nextWriter;
      if (nextWriter != null && nextWriter instanceof ProtobufLogWriter) {
        this.hdfs_out = ((ProtobufLogWriter) nextWriter).getStream();
      } else {
        this.hdfs_out = null;
      }
    } catch (InterruptedException ie) {
      // Perpetuate the interrupt
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      long count = getUnflushedEntriesCount();
      LOG.error("Failed close of WAL writer " + oldPath + ", unflushedEntries=" + count, e);
      throw new FailedLogCloseException(oldPath + ", unflushedEntries=" + count, e);
    } finally {
      // Let the writer thread go regardless, whether error or not.
      if (zigzagLatch != null) {
        zigzagLatch.releaseSafePoint();
        // syncFuture will be null if we failed our wait on safe point above. Otherwise, if
        // latch was obtained successfully, the sync we threw in either trigger the latch or it
        // got stamped with an exception because the WAL was damaged and we could not sync. Now
        // the write pipeline has been opened up again by releasing the safe point, process the
        // syncFuture we got above. This is probably a noop but it may be stale exception from
        // when old WAL was in place. Catch it if so.
        if (syncFuture != null) {
          try {
            blockOnSync(syncFuture);
          } catch (IOException ioe) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Stale sync exception", ioe);
            }
          }
        }
      }
    }
    return oldFileLen;
  }

  @Override
  protected void doShutdown() throws IOException {
    // Shutdown the disruptor. Will stop after all entries have been processed. Make sure we
    // have stopped incoming appends before calling this else it will not shutdown. We are
    // conservative below waiting a long time and if not elapsed, then halting.
    if (this.disruptor != null) {
      long timeoutms = conf.getLong("hbase.wal.disruptor.shutdown.timeout.ms", 60000);
      try {
        this.disruptor.shutdown(timeoutms, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        LOG.warn("Timed out bringing down disruptor after " + timeoutms + "ms; forcing halt "
            + "(It is a problem if this is NOT an ABORT! -- DATALOSS!!!!)");
        this.disruptor.halt();
        this.disruptor.shutdown();
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Closing WAL writer in " + FSUtils.getPath(walDir));
    }
    if (this.writer != null) {
      this.writer.close();
      this.writer = null;
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NP_NULL_ON_SOME_PATH_EXCEPTION",
      justification = "Will never be null")
  @Override
  public long append(final HRegionInfo hri,
      final WALKey key, final WALEdit edits, final boolean inMemstore) throws IOException {
    if (this.closed) {
      throw new IOException("Cannot append; log is closed");
    }
    // Make a trace scope for the append. It is closed on other side of the ring buffer by the
    // single consuming thread. Don't have to worry about it.
    TraceScope scope = Trace.startSpan("FSHLog.append");

    // This is crazy how much it takes to make an edit. Do we need all this stuff!!!!???? We need
    // all this to make a key and then below to append the edit, we need to carry htd, info,
    // etc. all over the ring buffer.
    FSWALEntry entry = null;
    long sequence = this.disruptor.getRingBuffer().next();
    try {
      RingBufferTruck truck = this.disruptor.getRingBuffer().get(sequence);
      // Construction of FSWALEntry sets a latch. The latch is thrown just after we stamp the
      // edit with its edit/sequence id.
      // TODO: reuse FSWALEntry as we do SyncFuture rather create per append.
      entry = new FSWALEntry(sequence, key, edits, hri, inMemstore);
      truck.load(entry, scope.detach());
    } finally {
      this.disruptor.getRingBuffer().publish(sequence);
    }
    return sequence;
  }

  /**
   * Thread to runs the hdfs sync call. This call takes a while to complete. This is the longest
   * pole adding edits to the WAL and this must complete to be sure all edits persisted. We run
   * multiple threads sync'ng rather than one that just syncs in series so we have better latencies;
   * otherwise, an edit that arrived just after a sync started, might have to wait almost the length
   * of two sync invocations before it is marked done.
   * <p>
   * When the sync completes, it marks all the passed in futures done. On the other end of the sync
   * future is a blocked thread, usually a regionserver Handler. There may be more than one future
   * passed in the case where a few threads arrive at about the same time and all invoke 'sync'. In
   * this case we'll batch up the invocations and run one filesystem sync only for a batch of
   * Handler sync invocations. Do not confuse these Handler SyncFutures with the futures an
   * ExecutorService returns when you call submit. We have no use for these in this model. These
   * SyncFutures are 'artificial', something to hold the Handler until the filesystem sync
   * completes.
   */
  private class SyncRunner extends HasThread {
    private volatile long sequence;
    // Keep around last exception thrown. Clear on successful sync.
    private final BlockingQueue<SyncFuture> syncFutures;
    private volatile SyncFuture takeSyncFuture = null;

    /**
     * UPDATE!
     * @param syncs the batch of calls to sync that arrived as this thread was starting; when done,
     *          we will put the result of the actual hdfs sync call as the result.
     * @param sequence The sequence number on the ring buffer when this thread was set running. If
     *          this actual writer sync completes then all appends up this point have been
     *          flushed/synced/pushed to datanodes. If we fail, then the passed in
     *          <code>syncs</code> futures will return the exception to their clients; some of the
     *          edits may have made it out to data nodes but we will report all that were part of
     *          this session as failed.
     */
    SyncRunner(final String name, final int maxHandlersCount) {
      super(name);
      // LinkedBlockingQueue because of
      // http://www.javacodegeeks.com/2010/09/java-best-practices-queue-battle-and.html
      // Could use other blockingqueues here or concurrent queues.
      //
      // We could let the capacity be 'open' but bound it so we get alerted in pathological case
      // where we cannot sync and we have a bunch of threads all backed up waiting on their syncs
      // to come in. LinkedBlockingQueue actually shrinks when you remove elements so Q should
      // stay neat and tidy in usual case. Let the max size be three times the maximum handlers.
      // The passed in maxHandlerCount is the user-level handlers which is what we put up most of
      // but HBase has other handlers running too -- opening region handlers which want to write
      // the meta table when succesful (i.e. sync), closing handlers -- etc. These are usually
      // much fewer in number than the user-space handlers so Q-size should be user handlers plus
      // some space for these other handlers. Lets multiply by 3 for good-measure.
      this.syncFutures = new LinkedBlockingQueue<SyncFuture>(maxHandlersCount * 3);
    }

    void offer(final long sequence, final SyncFuture[] syncFutures, final int syncFutureCount) {
      // Set sequence first because the add to the queue will wake the thread if sleeping.
      this.sequence = sequence;
      for (int i = 0; i < syncFutureCount; ++i) {
        this.syncFutures.add(syncFutures[i]);
      }
    }

    /**
     * Release the passed <code>syncFuture</code>
     * @return Returns 1.
     */
    private int releaseSyncFuture(final SyncFuture syncFuture, final long currentSequence,
        final Throwable t) {
      if (!syncFuture.done(currentSequence, t)) {
        throw new IllegalStateException();
      }

      // This function releases one sync future only.
      return 1;
    }

    /**
     * Release all SyncFutures whose sequence is <= <code>currentSequence</code>.
     * @param t May be non-null if we are processing SyncFutures because an exception was thrown.
     * @return Count of SyncFutures we let go.
     */
    private int releaseSyncFutures(final long currentSequence, final Throwable t) {
      int syncCount = 0;
      for (SyncFuture syncFuture; (syncFuture = this.syncFutures.peek()) != null;) {
        if (syncFuture.getTxid() > currentSequence) {
          break;
        }
        releaseSyncFuture(syncFuture, currentSequence, t);
        if (!this.syncFutures.remove(syncFuture)) {
          throw new IllegalStateException(syncFuture.toString());
        }
        syncCount++;
      }
      return syncCount;
    }

    /**
     * @param sequence The sequence we ran the filesystem sync against.
     * @return Current highest synced sequence.
     */
    private long updateHighestSyncedSequence(long sequence) {
      long currentHighestSyncedSequence;
      // Set the highestSyncedSequence IFF our current sequence id is the 'highest'.
      do {
        currentHighestSyncedSequence = highestSyncedTxid.get();
        if (currentHighestSyncedSequence >= sequence) {
          // Set the sync number to current highwater mark; might be able to let go more
          // queued sync futures
          sequence = currentHighestSyncedSequence;
          break;
        }
      } while (!highestSyncedTxid.compareAndSet(currentHighestSyncedSequence, sequence));
      return sequence;
    }

    boolean areSyncFuturesReleased() {
      // check whether there is no sync futures offered, and no in-flight sync futures that is being
      // processed.
      return syncFutures.size() <= 0
          && takeSyncFuture == null;
    }

    @Override
    public void run() {
      long currentSequence;
      while (!isInterrupted()) {
        int syncCount = 0;

        try {
          while (true) {
            takeSyncFuture = null;
            // We have to process what we 'take' from the queue
            takeSyncFuture = this.syncFutures.take();
            currentSequence = this.sequence;
            long syncFutureSequence = takeSyncFuture.getTxid();
            if (syncFutureSequence > currentSequence) {
              throw new IllegalStateException("currentSequence=" + currentSequence
                  + ", syncFutureSequence=" + syncFutureSequence);
            }
            // See if we can process any syncfutures BEFORE we go sync.
            long currentHighestSyncedSequence = highestSyncedTxid.get();
            if (currentSequence < currentHighestSyncedSequence) {
              syncCount += releaseSyncFuture(takeSyncFuture, currentHighestSyncedSequence, null);
              // Done with the 'take'. Go around again and do a new 'take'.
              continue;
            }
            break;
          }
          // I got something. Lets run. Save off current sequence number in case it changes
          // while we run.
          TraceScope scope = Trace.continueSpan(takeSyncFuture.getSpan());
          long start = System.nanoTime();
          Throwable lastException = null;
          try {
            Trace.addTimelineAnnotation("syncing writer");
            writer.sync();
            Trace.addTimelineAnnotation("writer synced");
            currentSequence = updateHighestSyncedSequence(currentSequence);
          } catch (IOException e) {
            LOG.error("Error syncing, request close of WAL", e);
            lastException = e;
          } catch (Exception e) {
            LOG.warn("UNEXPECTED", e);
            lastException = e;
          } finally {
            // reattach the span to the future before releasing.
            takeSyncFuture.setSpan(scope.detach());
            // First release what we 'took' from the queue.
            syncCount += releaseSyncFuture(takeSyncFuture, currentSequence, lastException);
            // Can we release other syncs?
            syncCount += releaseSyncFutures(currentSequence, lastException);
            if (lastException != null) {
              requestLogRoll();
            } else {
              checkLogRoll();
            }
          }
          postSync(System.nanoTime() - start, syncCount);
        } catch (InterruptedException e) {
          // Presume legit interrupt.
          Thread.currentThread().interrupt();
        } catch (Throwable t) {
          LOG.warn("UNEXPECTED, continuing", t);
        }
      }
    }
  }

  /**
   * Schedule a log roll if needed.
   */
  void checkLogRoll() {
    // Will return immediately if we are in the middle of a WAL log roll currently.
    if (!rollWriterLock.tryLock()) {
      return;
    }
    boolean lowReplication;
    try {
      lowReplication = checkLowReplication();
    } finally {
      rollWriterLock.unlock();
    }
    if (lowReplication || writer != null && writer.getLength() > logrollsize) {
      requestLogRoll(lowReplication);
    }
  }

  /**
   * @return true if number of replicas for the WAL is lower than threshold
   */
  private boolean checkLowReplication() {
    boolean logRollNeeded = false;
    // if the number of replicas in HDFS has fallen below the configured
    // value, then roll logs.
    try {
      int numCurrentReplicas = getLogReplication();
      if (numCurrentReplicas != 0 && numCurrentReplicas < this.minTolerableReplication) {
        if (this.lowReplicationRollEnabled) {
          if (this.consecutiveLogRolls.get() < this.lowReplicationRollLimit) {
            LOG.warn("HDFS pipeline error detected. " + "Found " + numCurrentReplicas
                + " replicas but expecting no less than " + this.minTolerableReplication
                + " replicas. " + " Requesting close of WAL. current pipeline: "
                + Arrays.toString(getPipeline()));
            logRollNeeded = true;
            // If rollWriter is requested, increase consecutiveLogRolls. Once it
            // is larger than lowReplicationRollLimit, disable the
            // LowReplication-Roller
            this.consecutiveLogRolls.getAndIncrement();
          } else {
            LOG.warn("Too many consecutive RollWriter requests, it's a sign of "
                + "the total number of live datanodes is lower than the tolerable replicas.");
            this.consecutiveLogRolls.set(0);
            this.lowReplicationRollEnabled = false;
          }
        }
      } else if (numCurrentReplicas >= this.minTolerableReplication) {
        if (!this.lowReplicationRollEnabled) {
          // The new writer's log replicas is always the default value.
          // So we should not enable LowReplication-Roller. If numEntries
          // is lower than or equals 1, we consider it as a new writer.
          if (this.numEntries.get() <= 1) {
            return logRollNeeded;
          }
          // Once the live datanode number and the replicas return to normal,
          // enable the LowReplication-Roller.
          this.lowReplicationRollEnabled = true;
          LOG.info("LowReplication-Roller was enabled.");
        }
      }
    } catch (Exception e) {
      LOG.warn("DFSOutputStream.getNumCurrentReplicas failed because of " + e + ", continuing...");
    }
    return logRollNeeded;
  }

  private SyncFuture publishSyncOnRingBuffer(long sequence) {
    return publishSyncOnRingBuffer(sequence, null);
  }

  private long getSequenceOnRingBuffer() {
    return this.disruptor.getRingBuffer().next();
  }

  private SyncFuture publishSyncOnRingBuffer(Span span) {
    long sequence = this.disruptor.getRingBuffer().next();
    return publishSyncOnRingBuffer(sequence, span);
  }

  private SyncFuture publishSyncOnRingBuffer(long sequence, Span span) {
    // here we use ring buffer sequence as transaction id
    SyncFuture syncFuture = getSyncFuture(sequence, span);
    try {
      RingBufferTruck truck = this.disruptor.getRingBuffer().get(sequence);
      truck.load(syncFuture);
    } finally {
      this.disruptor.getRingBuffer().publish(sequence);
    }
    return syncFuture;
  }

  // Sync all known transactions
  private Span publishSyncThenBlockOnCompletion(Span span) throws IOException {
    return blockOnSync(publishSyncOnRingBuffer(span));
  }

  /**
   * {@inheritDoc}
   * <p>
   * If the pipeline isn't started yet or is empty, you will get the default replication factor.
   * Therefore, if this function returns 0, it means you are not properly running with the HDFS-826
   * patch.
   */
  @Override
  @VisibleForTesting
  int getLogReplication() {
    try {
      // in standalone mode, it will return 0
      if (this.hdfs_out instanceof HdfsDataOutputStream) {
        return ((HdfsDataOutputStream) this.hdfs_out).getCurrentBlockReplication();
      }
    } catch (IOException e) {
      LOG.info("", e);
    }
    return 0;
  }

  @Override
  public void sync() throws IOException {
    TraceScope scope = Trace.startSpan("FSHLog.sync");
    try {
      scope = Trace.continueSpan(publishSyncThenBlockOnCompletion(scope.detach()));
    } finally {
      assert scope == NullScope.INSTANCE || !scope.isDetached();
      scope.close();
    }
  }

  @Override
  public void sync(long txid) throws IOException {
    if (this.highestSyncedTxid.get() >= txid) {
      // Already sync'd.
      return;
    }
    TraceScope scope = Trace.startSpan("FSHLog.sync");
    try {
      scope = Trace.continueSpan(publishSyncThenBlockOnCompletion(scope.detach()));
    } finally {
      assert scope == NullScope.INSTANCE || !scope.isDetached();
      scope.close();
    }
  }

  @VisibleForTesting
  boolean isLowReplicationRollEnabled() {
    return lowReplicationRollEnabled;
  }

  public static final long FIXED_OVERHEAD = ClassSize
      .align(ClassSize.OBJECT + (5 * ClassSize.REFERENCE) + ClassSize.ATOMIC_INTEGER
          + Bytes.SIZEOF_INT + (3 * Bytes.SIZEOF_LONG));

  private static void split(final Configuration conf, final Path p) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    if (!fs.getFileStatus(p).isDirectory()) {
      throw new IOException(p + " is not a directory");
    }

    final Path baseDir = FSUtils.getRootDir(conf);
    final Path archiveDir = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    WALSplitter.split(baseDir, p, archiveDir, fs, conf, WALFactory.getInstance(conf));
  }

  /**
   * This class is used coordinating two threads holding one thread at a 'safe point' while the
   * orchestrating thread does some work that requires the first thread paused: e.g. holding the WAL
   * writer while its WAL is swapped out from under it by another thread.
   * <p>
   * Thread A signals Thread B to hold when it gets to a 'safe point'. Thread A wait until Thread B
   * gets there. When the 'safe point' has been attained, Thread B signals Thread A. Thread B then
   * holds at the 'safe point'. Thread A on notification that Thread B is paused, goes ahead and
   * does the work it needs to do while Thread B is holding. When Thread A is done, it flags B and
   * then Thread A and Thread B continue along on their merry way. Pause and signalling 'zigzags'
   * between the two participating threads. We use two latches -- one the inverse of the other --
   * pausing and signaling when states are achieved.
   * <p>
   * To start up the drama, Thread A creates an instance of this class each time it would do this
   * zigzag dance and passes it to Thread B (these classes use Latches so it is one shot only).
   * Thread B notices the new instance (via reading a volatile reference or how ever) and it starts
   * to work toward the 'safe point'. Thread A calls {@link #waitSafePoint()} when it cannot proceed
   * until the Thread B 'safe point' is attained. Thread A will be held inside in
   * {@link #waitSafePoint()} until Thread B reaches the 'safe point'. Once there, Thread B frees
   * Thread A by calling {@link #safePointAttained()}. Thread A now knows Thread B is at the 'safe
   * point' and that it is holding there (When Thread B calls {@link #safePointAttained()} it blocks
   * here until Thread A calls {@link #releaseSafePoint()}). Thread A proceeds to do what it needs
   * to do while Thread B is paused. When finished, it lets Thread B lose by calling
   * {@link #releaseSafePoint()} and away go both Threads again.
   */
  static class SafePointZigZagLatch {
    /**
     * Count down this latch when safe point attained.
     */
    private volatile CountDownLatch safePointAttainedLatch = new CountDownLatch(1);
    /**
     * Latch to wait on. Will be released when we can proceed.
     */
    private volatile CountDownLatch safePointReleasedLatch = new CountDownLatch(1);

    /**
     * For Thread A to call when it is ready to wait on the 'safe point' to be attained. Thread A
     * will be held in here until Thread B calls {@link #safePointAttained()}
     * @param syncFuture We need this as barometer on outstanding syncs. If it comes home with an
     *          exception, then something is up w/ our syncing.
     * @return The passed <code>syncFuture</code>
     */
    SyncFuture waitSafePoint(final SyncFuture syncFuture) throws InterruptedException,
        FailedSyncBeforeLogCloseException {
      while (true) {
        if (this.safePointAttainedLatch.await(1, TimeUnit.MILLISECONDS)) {
          break;
        }
        if (syncFuture.isThrowable()) {
          throw new FailedSyncBeforeLogCloseException(syncFuture.getThrowable());
        }
      }
      return syncFuture;
    }

    /**
     * Called by Thread B when it attains the 'safe point'. In this method, Thread B signals Thread
     * A it can proceed. Thread B will be held in here until {@link #releaseSafePoint()} is called
     * by Thread A.
     */
    void safePointAttained() throws InterruptedException {
      this.safePointAttainedLatch.countDown();
      this.safePointReleasedLatch.await();
    }

    /**
     * Called by Thread A when it is done with the work it needs to do while Thread B is halted.
     * This will release the Thread B held in a call to {@link #safePointAttained()}
     */
    void releaseSafePoint() {
      this.safePointReleasedLatch.countDown();
    }

    /**
     * @return True is this is a 'cocked', fresh instance, and not one that has already fired.
     */
    boolean isCocked() {
      return this.safePointAttainedLatch.getCount() > 0
          && this.safePointReleasedLatch.getCount() > 0;
    }
  }

  /**
   * Handler that is run by the disruptor ringbuffer consumer. Consumer is a SINGLE
   * 'writer/appender' thread. Appends edits and starts up sync runs. Tries its best to batch up
   * syncs. There is no discernible benefit batching appends so we just append as they come in
   * because it simplifies the below implementation. See metrics for batching effectiveness (In
   * measurement, at 100 concurrent handlers writing 1k, we are batching > 10 appends and 10 handler
   * sync invocations for every actual dfsclient sync call; at 10 concurrent handlers, YMMV).
   * <p>
   * Herein, we have an array into which we store the sync futures as they come in. When we have a
   * 'batch', we'll then pass what we have collected to a SyncRunner thread to do the filesystem
   * sync. When it completes, it will then call {@link SyncFuture#done(long, Throwable)} on each of
   * SyncFutures in the batch to release blocked Handler threads.
   * <p>
   * I've tried various effects to try and make latencies low while keeping throughput high. I've
   * tried keeping a single Queue of SyncFutures in this class appending to its tail as the syncs
   * coming and having sync runner threads poll off the head to 'finish' completed SyncFutures. I've
   * tried linkedlist, and various from concurrent utils whether LinkedBlockingQueue or
   * ArrayBlockingQueue, etc. The more points of synchronization, the more 'work' (according to
   * 'perf stats') that has to be done; small increases in stall percentages seem to have a big
   * impact on throughput/latencies. The below model where we have an array into which we stash the
   * syncs and then hand them off to the sync thread seemed like a decent compromise. See HBASE-8755
   * for more detail.
   */
  class RingBufferEventHandler implements EventHandler<RingBufferTruck>, LifecycleAware {
    private final SyncRunner[] syncRunners;
    private final SyncFuture[] syncFutures;
    // Had 'interesting' issues when this was non-volatile. On occasion, we'd not pass all
    // syncFutures to the next sync'ing thread.
    private volatile int syncFuturesCount = 0;
    private volatile SafePointZigZagLatch zigzagLatch;
    /**
     * Set if we get an exception appending or syncing so that all subsequence appends and syncs on
     * this WAL fail until WAL is replaced.
     */
    private Exception exception = null;
    /**
     * Object to block on while waiting on safe point.
     */
    private final Object safePointWaiter = new Object();
    private volatile boolean shutdown = false;

    /**
     * Which syncrunner to use next.
     */
    private int syncRunnerIndex;

    RingBufferEventHandler(final int syncRunnerCount, final int maxHandlersCount) {
      this.syncFutures = new SyncFuture[maxHandlersCount];
      this.syncRunners = new SyncRunner[syncRunnerCount];
      for (int i = 0; i < syncRunnerCount; i++) {
        this.syncRunners[i] = new SyncRunner("sync." + i, maxHandlersCount);
      }
    }

    private void cleanupOutstandingSyncsOnException(final long sequence, final Exception e) {
      // There could be handler-count syncFutures outstanding.
      for (int i = 0; i < this.syncFuturesCount; i++) {
        this.syncFutures[i].done(sequence, e);
      }
      this.syncFuturesCount = 0;
    }

    /**
     * @return True if outstanding sync futures still
     */
    private boolean isOutstandingSyncs() {
      // Look at SyncFutures in the EventHandler
      for (int i = 0; i < this.syncFuturesCount; i++) {
        if (!this.syncFutures[i].isDone()) {
          return true;
        }
      }

      return false;
    }

    private boolean isOutstandingSyncsFromRunners() {
      // Look at SyncFutures in the SyncRunners
      for (SyncRunner syncRunner: syncRunners) {
        if(syncRunner.isAlive() && !syncRunner.areSyncFuturesReleased()) {
          return true;
        }
      }
      return false;
    }

    @Override
    // We can set endOfBatch in the below method if at end of our this.syncFutures array
    public void onEvent(final RingBufferTruck truck, final long sequence, boolean endOfBatch)
        throws Exception {
      // Appends and syncs are coming in order off the ringbuffer. We depend on this fact. We'll
      // add appends to dfsclient as they come in. Batching appends doesn't give any significant
      // benefit on measurement. Handler sync calls we will batch up. If we get an exception
      // appending an edit, we fail all subsequent appends and syncs with the same exception until
      // the WAL is reset. It is important that we not short-circuit and exit early this method.
      // It is important that we always go through the attainSafePoint on the end. Another thread,
      // the log roller may be waiting on a signal from us here and will just hang without it.

      try {
        if (truck.type() == RingBufferTruck.Type.SYNC) {
          this.syncFutures[this.syncFuturesCount++] = truck.unloadSync();
          // Force flush of syncs if we are carrying a full complement of syncFutures.
          if (this.syncFuturesCount == this.syncFutures.length) {
            endOfBatch = true;
          }
        } else if (truck.type() == RingBufferTruck.Type.APPEND) {
          FSWALEntry entry = truck.unloadAppend();
          TraceScope scope = Trace.continueSpan(entry.detachSpan());
          try {

            if (this.exception != null) {
              // We got an exception on an earlier attempt at append. Do not let this append
              // go through. Fail it but stamp the sequenceid into this append though failed.
              // We need to do this to close the latch held down deep in WALKey...that is waiting
              // on sequenceid assignment otherwise it will just hang out (The #append method
              // called below does this also internally).
              entry.stampRegionSequenceId();
              // Return to keep processing events coming off the ringbuffer
              return;
            }
            append(entry);
          } catch (Exception e) {
            // Failed append. Record the exception.
            this.exception = e;
            // invoking cleanupOutstandingSyncsOnException when append failed with exception,
            // it will cleanup existing sync requests recorded in syncFutures but not offered to SyncRunner yet,
            // so there won't be any sync future left over if no further truck published to disruptor.
            cleanupOutstandingSyncsOnException(sequence,
                this.exception instanceof DamagedWALException ? this.exception
                    : new DamagedWALException("On sync", this.exception));
            // Return to keep processing events coming off the ringbuffer
            return;
          } finally {
            assert scope == NullScope.INSTANCE || !scope.isDetached();
            scope.close(); // append scope is complete
          }
        } else {
          // What is this if not an append or sync. Fail all up to this!!!
          cleanupOutstandingSyncsOnException(sequence,
            new IllegalStateException("Neither append nor sync"));
          // Return to keep processing.
          return;
        }

        // TODO: Check size and if big go ahead and call a sync if we have enough data.
        // This is a sync. If existing exception, fall through. Else look to see if batch.
        if (this.exception == null) {
          // If not a batch, return to consume more events from the ring buffer before proceeding;
          // we want to get up a batch of syncs and appends before we go do a filesystem sync.
          if (!endOfBatch || this.syncFuturesCount <= 0) {
            return;
          }
          // syncRunnerIndex is bound to the range [0, Integer.MAX_INT - 1] as follows:
          //   * The maximum value possible for syncRunners.length is Integer.MAX_INT
          //   * syncRunnerIndex starts at 0 and is incremented only here
          //   * after the increment, the value is bounded by the '%' operator to
          //     [0, syncRunners.length), presuming the value was positive prior to
          //     the '%' operator.
          //   * after being bound to [0, Integer.MAX_INT - 1], the new value is stored in
          //     syncRunnerIndex ensuring that it can't grow without bound and overflow.
          //   * note that the value after the increment must be positive, because the most it
          //     could have been prior was Integer.MAX_INT - 1 and we only increment by 1.
          this.syncRunnerIndex = (this.syncRunnerIndex + 1) % this.syncRunners.length;
          try {
            // Below expects that the offer 'transfers' responsibility for the outstanding syncs to
            // the syncRunner. We should never get an exception in here.
            this.syncRunners[this.syncRunnerIndex].offer(sequence, this.syncFutures,
              this.syncFuturesCount);
          } catch (Exception e) {
            // Should NEVER get here.
            requestLogRoll();
            this.exception = new DamagedWALException("Failed offering sync", e);
          }
        }
        // We may have picked up an exception above trying to offer sync
        if (this.exception != null) {
          cleanupOutstandingSyncsOnException(sequence, this.exception instanceof DamagedWALException
              ? this.exception : new DamagedWALException("On sync", this.exception));
        }
        attainSafePoint(sequence);
        this.syncFuturesCount = 0;
      } catch (Throwable t) {
        LOG.error("UNEXPECTED!!! syncFutures.length=" + this.syncFutures.length, t);
      }
    }

    SafePointZigZagLatch attainSafePoint() {
      this.zigzagLatch = new SafePointZigZagLatch();
      return this.zigzagLatch;
    }

    /**
     * Check if we should attain safe point. If so, go there and then wait till signalled before we
     * proceeding.
     */
    private void attainSafePoint(final long currentSequence) {
      if (this.zigzagLatch == null || !this.zigzagLatch.isCocked()) {
        return;
      }
      // If here, another thread is waiting on us to get to safe point. Don't leave it hanging.
      beforeWaitOnSafePoint();
      try {
        // Wait on outstanding syncers; wait for them to finish syncing (unless we've been
        // shutdown or unless our latch has been thrown because we have been aborted or unless
        // this WAL is broken and we can't get a sync/append to complete).
        while ((!this.shutdown && this.zigzagLatch.isCocked()
            && highestSyncedTxid.get() < currentSequence &&
            // We could be in here and all syncs are failing or failed. Check for this. Otherwise
            // we'll just be stuck here for ever. In other words, ensure there syncs running.
            isOutstandingSyncs())
            // Wait for all SyncRunners to finish their work so that we can replace the writer
            || isOutstandingSyncsFromRunners()) {
          synchronized (this.safePointWaiter) {
            this.safePointWaiter.wait(0, 1);
          }
        }
        // Tell waiting thread we've attained safe point. Can clear this.throwable if set here
        // because we know that next event through the ringbuffer will be going to a new WAL
        // after we do the zigzaglatch dance.
        this.exception = null;
        this.zigzagLatch.safePointAttained();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted ", e);
        Thread.currentThread().interrupt();
      }
    }

    /**
     * Append to the WAL. Does all CP and WAL listener calls.
     */
    void append(final FSWALEntry entry) throws Exception {
      try {
        FSHLog.this.append(writer, entry);
      } catch (Exception e) {
        String msg = "Append sequenceId=" + entry.getKey().getSequenceId()
            + ", requesting roll of WAL";
        LOG.warn(msg, e);
        requestLogRoll();
        throw new DamagedWALException(msg, e);
      }
    }

    @Override
    public void onStart() {
      for (SyncRunner syncRunner : this.syncRunners) {
        syncRunner.start();
      }
    }

    @Override
    public void onShutdown() {
      for (SyncRunner syncRunner : this.syncRunners) {
        syncRunner.interrupt();
      }
    }
  }

  private static void usage() {
    System.err.println("Usage: FSHLog <ARGS>");
    System.err.println("Arguments:");
    System.err.println(" --dump  Dump textual representation of passed one or more files");
    System.err.println("         For example: "
        + "FSHLog --dump hdfs://example.com:9000/hbase/.logs/MACHINE/LOGFILE");
    System.err.println(" --split Split the passed directory of WAL logs");
    System.err.println(
      "         For example: " + "FSHLog --split hdfs://example.com:9000/hbase/.logs/DIR");
  }

  /**
   * Pass one or more log file names and it will either dump out a text version on
   * <code>stdout</code> or split the specified log files.
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      usage();
      System.exit(-1);
    }
    // either dump using the WALPrettyPrinter or split, depending on args
    if (args[0].compareTo("--dump") == 0) {
      WALPrettyPrinter.run(Arrays.copyOfRange(args, 1, args.length));
    } else if (args[0].compareTo("--perf") == 0) {
      LOG.fatal("Please use the WALPerformanceEvaluation tool instead. i.e.:");
      LOG.fatal(
        "\thbase org.apache.hadoop.hbase.wal.WALPerformanceEvaluation --iterations " + args[1]);
      System.exit(-1);
    } else if (args[0].compareTo("--split") == 0) {
      Configuration conf = HBaseConfiguration.create();
      for (int i = 1; i < args.length; i++) {
        try {
          Path logPath = new Path(args[i]);
          FSUtils.setFsDefault(conf, logPath);
          split(conf, logPath);
        } catch (IOException t) {
          t.printStackTrace(System.err);
          System.exit(-1);
        }
      }
    } else {
      usage();
      System.exit(-1);
    }
  }

  /**
   * This method gets the pipeline for the current WAL.
   */
  @Override
  DatanodeInfo[] getPipeline() {
    if (this.hdfs_out != null) {
      if (this.hdfs_out.getWrappedStream() instanceof DFSOutputStream) {
        return ((DFSOutputStream) this.hdfs_out.getWrappedStream()).getPipeline();
      }
    }
    return new DatanodeInfo[0];
  }
}
