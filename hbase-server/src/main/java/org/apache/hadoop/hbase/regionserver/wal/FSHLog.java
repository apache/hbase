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

import static org.apache.hadoop.hbase.regionserver.wal.WALActionsListener.RollRequestReason.SLOW_SYNC;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.FSHLogProvider;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The original implementation of FSWAL.
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
  private static final Logger LOG = LoggerFactory.getLogger(FSHLog.class);

  private static final String TOLERABLE_LOW_REPLICATION =
    "hbase.regionserver.hlog.tolerable.lowreplication";
  private static final String LOW_REPLICATION_ROLL_LIMIT =
    "hbase.regionserver.hlog.lowreplication.rolllimit";
  private static final int DEFAULT_LOW_REPLICATION_ROLL_LIMIT = 5;
  private static final String SYNCER_COUNT = "hbase.regionserver.hlog.syncer.count";
  private static final int DEFAULT_SYNCER_COUNT = 5;
  private static final String MAX_BATCH_COUNT = "hbase.regionserver.wal.sync.batch.count";
  private static final int DEFAULT_MAX_BATCH_COUNT = 200;

  private static final String FSHLOG_WAIT_ON_SHUTDOWN_IN_SECONDS =
    "hbase.wal.fshlog.wait.on.shutdown.seconds";
  private static final int DEFAULT_FSHLOG_WAIT_ON_SHUTDOWN_IN_SECONDS = 5;

  private static final IOException WITER_REPLACED_EXCEPTION =
    new IOException("Writer was replaced!");
  private static final IOException WITER_BROKEN_EXCEPTION = new IOException("Wirter was broken!");
  private static final IOException WAL_CLOSE_EXCEPTION = new IOException("WAL was closed!");

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

  private final int syncerCount;
  private final int maxSyncRequestCount;

  /**
   * Which syncrunner to use next.
   */
  private int syncRunnerIndex = 0;

  private SyncRunner[] syncRunners = null;

  /**
   * Constructor.
   * @param fs     filesystem handle
   * @param root   path for stored and archived wals
   * @param logDir dir where wals are stored
   * @param conf   configuration to use
   */
  public FSHLog(final FileSystem fs, final Path root, final String logDir, final Configuration conf)
    throws IOException {
    this(fs, root, logDir, HConstants.HREGION_OLDLOGDIR_NAME, conf, null, true, null, null);
  }

  public FSHLog(final FileSystem fs, Abortable abortable, final Path root, final String logDir,
    final Configuration conf) throws IOException {
    this(fs, abortable, root, logDir, HConstants.HREGION_OLDLOGDIR_NAME, conf, null, true, null,
      null, null, null);
  }

  public FSHLog(final FileSystem fs, final Path rootDir, final String logDir,
    final String archiveDir, final Configuration conf, final List<WALActionsListener> listeners,
    final boolean failIfWALExists, final String prefix, final String suffix) throws IOException {
    this(fs, null, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix,
      null, null);
  }

  /**
   * Create an edit log at the given <code>dir</code> location. You should never have to load an
   * existing log. If there is a log at startup, it should have already been processed and deleted
   * by the time the WAL object is started up.
   * @param fs              filesystem handle
   * @param abortable       Abortable - the server here
   * @param rootDir         path to where logs and oldlogs
   * @param logDir          dir where wals are stored
   * @param archiveDir      dir where wals are archived
   * @param conf            configuration to use
   * @param listeners       Listeners on WAL events. Listeners passed here will be registered before
   *                        we do anything else; e.g. the Constructor {@link #rollWriter()}.
   * @param failIfWALExists If true IOException will be thrown if files related to this wal already
   *                        exist.
   * @param prefix          should always be hostname and port in distributed env and it will be URL
   *                        encoded before being used. If prefix is null, "wal" will be used
   * @param suffix          will be url encoded. null is treated as empty. non-empty must start with
   *                        {@link org.apache.hadoop.hbase.wal.AbstractFSWALProvider#WAL_FILE_NAME_DELIMITER}
   */
  public FSHLog(final FileSystem fs, final Abortable abortable, final Path rootDir,
    final String logDir, final String archiveDir, final Configuration conf,
    final List<WALActionsListener> listeners, final boolean failIfWALExists, final String prefix,
    final String suffix, FileSystem remoteFs, Path remoteWALDir) throws IOException {
    super(fs, abortable, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix,
      suffix, remoteFs, remoteWALDir);
    this.minTolerableReplication =
      conf.getInt(TOLERABLE_LOW_REPLICATION, CommonFSUtils.getDefaultReplication(fs, this.walDir));
    this.lowReplicationRollLimit =
      conf.getInt(LOW_REPLICATION_ROLL_LIMIT, DEFAULT_LOW_REPLICATION_ROLL_LIMIT);

    // Advance the ring buffer sequence so that it starts from 1 instead of 0,
    // because SyncFuture.NOT_DONE = 0.

    this.syncerCount = conf.getInt(SYNCER_COUNT, DEFAULT_SYNCER_COUNT);
    this.maxSyncRequestCount = conf.getInt(MAX_BATCH_COUNT,
      conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT, DEFAULT_MAX_BATCH_COUNT));

    this.createSingleThreadPoolConsumeExecutor("FSHLog", rootDir, prefix);

    this.setWaitOnShutdownInSeconds(
      conf.getInt(FSHLOG_WAIT_ON_SHUTDOWN_IN_SECONDS, DEFAULT_FSHLOG_WAIT_ON_SHUTDOWN_IN_SECONDS),
      FSHLOG_WAIT_ON_SHUTDOWN_IN_SECONDS);
  }

  @Override
  public void init() throws IOException {
    super.init();
    this.createSyncRunnersAndStart();
  }

  private void createSyncRunnersAndStart() {
    this.syncRunnerIndex = 0;
    this.syncRunners = new SyncRunner[syncerCount];
    for (int i = 0; i < syncerCount; i++) {
      this.syncRunners[i] = new SyncRunner("sync." + i, maxSyncRequestCount);
      this.syncRunners[i].start();
    }
  }

  /**
   * Currently, we need to expose the writer's OutputStream to tests so that they can manipulate the
   * default behavior (such as setting the maxRecoveryErrorCount value). This is done using
   * reflection on the underlying HDFS OutputStream. NOTE: This could be removed once Hadoop1
   * support is removed.
   * @return null if underlying stream is not ready.
   */
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
      nextWriter.sync(useHsync);
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
  protected Writer createWriterInstance(FileSystem fs, Path path) throws IOException {
    Writer writer = FSHLogProvider.createWriter(conf, fs, path, false, this.blocksize);
    if (writer instanceof ProtobufLogWriter) {
      preemptiveSync((ProtobufLogWriter) writer);
    }
    return writer;
  }

  @Override
  protected void doAppend(Writer writer, FSWALEntry entry) throws IOException {
    writer.append(entry);
  }

  @Override
  protected void onWriterReplaced(Writer nextWriter) {
    if (nextWriter != null && nextWriter instanceof ProtobufLogWriter) {
      this.hdfs_out = ((ProtobufLogWriter) nextWriter).getStream();
    } else {
      this.hdfs_out = null;
    }
    this.createSyncRunnersAndStart();
  }

  @Override
  protected void doCleanUpResources() {
    this.shutDownSyncRunners();
  };

  private void shutDownSyncRunners() {
    SyncRunner[] syncRunnersToUse = this.syncRunners;
    if (syncRunnersToUse != null) {
      for (SyncRunner syncRunner : syncRunnersToUse) {
        syncRunner.shutDown();
      }
    }
    this.syncRunners = null;
  }

  @Override
  protected CompletableFuture<Long> doWriterSync(Writer writer, boolean shouldUseHSync,
    long txidWhenSync) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    SyncRequest syncRequest = new SyncRequest(writer, shouldUseHSync, txidWhenSync, future);
    this.offerSyncRequest(syncRequest);
    return future;
  }

  private void offerSyncRequest(SyncRequest syncRequest) {
    for (int i = 0; i < this.syncRunners.length; i++) {
      this.syncRunnerIndex = (this.syncRunnerIndex + 1) % this.syncRunners.length;
      if (this.syncRunners[this.syncRunnerIndex].offer(syncRequest)) {
        return;
      }
    }
    syncRequest.completableFuture
      .completeExceptionally(new IOException("There is no available syncRunner."));
  }

  static class SyncRequest {
    private final Writer writer;
    private final boolean shouldUseHSync;
    private final long sequenceWhenSync;
    private final CompletableFuture<Long> completableFuture;

    public SyncRequest(Writer writer, boolean shouldUseHSync, long txidWhenSync,
      CompletableFuture<Long> completableFuture) {
      this.writer = writer;
      this.shouldUseHSync = shouldUseHSync;
      this.sequenceWhenSync = txidWhenSync;
      this.completableFuture = completableFuture;
    }

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
  private class SyncRunner extends Thread {
    // Keep around last exception thrown. Clear on successful sync.
    private final BlockingQueue<SyncRequest> syncRequests;
    private volatile boolean shutDown = false;

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
      this.syncRequests = new LinkedBlockingQueue<>(maxHandlersCount * 3);
    }

    boolean offer(SyncRequest syncRequest) {
      if (this.shutDown) {
        return false;
      }

      if (!this.syncRequests.offer(syncRequest)) {
        return false;
      }

      // recheck
      if (this.shutDown) {
        if (this.syncRequests.remove(syncRequest)) {
          return false;
        }
      }
      return true;
    }

    private void completeSyncRequests(SyncRequest syncRequest, long syncedSequenceId) {
      if (syncRequest != null) {
        syncRequest.completableFuture.complete(syncedSequenceId);
      }
      while (true) {
        SyncRequest head = this.syncRequests.peek();
        if (head == null) {
          break;
        }
        if (head.sequenceWhenSync > syncedSequenceId) {
          break;
        }
        head.completableFuture.complete(syncedSequenceId);
        this.syncRequests.poll();
      }
    }

    private void completeExceptionallySyncRequests(SyncRequest syncRequest, Exception exception) {
      if (syncRequest != null) {
        syncRequest.completableFuture.completeExceptionally(exception);
      }
      while (true) {
        SyncRequest head = this.syncRequests.peek();
        if (head == null) {
          break;
        }
        if (head.writer != syncRequest.writer) {
          break;
        }
        head.completableFuture.completeExceptionally(exception);
        this.syncRequests.poll();
      }
    }

    private SyncRequest takeSyncRequest() throws InterruptedException {
      while (true) {
        // We have to process what we 'take' from the queue
        SyncRequest syncRequest = this.syncRequests.take();
        // See if we can process any syncfutures BEFORE we go sync.
        long currentHighestSyncedSequence = highestSyncedTxid.get();
        if (syncRequest.sequenceWhenSync < currentHighestSyncedSequence) {
          syncRequest.completableFuture.complete(currentHighestSyncedSequence);
          continue;
        }
        return syncRequest;
      }
    }

    @Override
    public void run() {
      while (!this.shutDown) {
        try {
          SyncRequest syncRequest = this.takeSyncRequest();
          // I got something. Lets run. Save off current sequence number in case it changes
          // while we run.
          long currentSequenceToUse = syncRequest.sequenceWhenSync;
          boolean writerBroken = isWriterBroken();
          long currentHighestProcessedAppendTxid = highestProcessedAppendTxid;
          Writer currentWriter = writer;
          if (currentWriter != syncRequest.writer) {
            syncRequest.completableFuture.completeExceptionally(WITER_REPLACED_EXCEPTION);
            continue;
          }
          if (writerBroken) {
            syncRequest.completableFuture.completeExceptionally(WITER_BROKEN_EXCEPTION);
            continue;
          }
          if (currentHighestProcessedAppendTxid > currentSequenceToUse) {
            currentSequenceToUse = currentHighestProcessedAppendTxid;
          }
          Exception lastException = null;
          try {
            writer.sync(syncRequest.shouldUseHSync);
          } catch (IOException e) {
            LOG.error("Error syncing", e);
            lastException = e;
          } catch (Exception e) {
            LOG.warn("UNEXPECTED", e);
            lastException = e;
          } finally {
            if (lastException != null) {
              this.completeExceptionallySyncRequests(syncRequest, lastException);
            } else {
              this.completeSyncRequests(syncRequest, currentSequenceToUse);
            }
          }
        } catch (InterruptedException e) {
          // Presume legit interrupt.
          LOG.info("interrupted");
        } catch (Throwable t) {
          LOG.warn("UNEXPECTED, continuing", t);
        }
      }
      this.clearSyncRequestsWhenShutDown();
    }

    private void clearSyncRequestsWhenShutDown() {
      while (true) {
        SyncRequest syncRequest = this.syncRequests.poll();
        if (syncRequest == null) {
          break;
        }
        syncRequest.completableFuture.completeExceptionally(WAL_CLOSE_EXCEPTION);
      }
    }

    void shutDown() {
      try {
        this.shutDown = true;
        this.interrupt();
        this.join();
      } catch (InterruptedException e) {
        LOG.warn("interrupted", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  protected void checkSlowSyncCount() {
    if (isLogRollRequested()) {
      return;
    }
    if (doCheckSlowSync()) {
      // We log this already in checkSlowSync
      requestLogRoll(SLOW_SYNC);
    }
  }

  /** Returns true if number of replicas for the WAL is lower than threshold */
  @Override
  protected boolean doCheckLogLowReplication() {
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

  /**
   * {@inheritDoc}
   * <p>
   * If the pipeline isn't started yet or is empty, you will get the default replication factor.
   * Therefore, if this function returns 0, it means you are not properly running with the HDFS-826
   * patch.
   */
  @Override
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

  boolean isLowReplicationRollEnabled() {
    return lowReplicationRollEnabled;
  }

  public static final long FIXED_OVERHEAD =
    ClassSize.align(ClassSize.OBJECT + (5 * ClassSize.REFERENCE) + (2 * ClassSize.ATOMIC_INTEGER)
      + (3 * Bytes.SIZEOF_INT) + (4 * Bytes.SIZEOF_LONG));

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

  @Override
  protected Writer createCombinedWriter(Writer localWriter, Writer remoteWriter) {
    // put remote writer first as usually it will cost more time to finish, so we write to it first
    return CombinedWriter.create(remoteWriter, localWriter);
  }
}
