package org.apache.hadoop.hbase.consensus.quorum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.NoLeaderForRegionException;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.consensus.exceptions.CommitQueueOverloadedException;
import org.apache.hadoop.hbase.consensus.exceptions.NewLeaderException;
import org.apache.hadoop.hbase.consensus.metrics.ConsensusMetrics;
import org.apache.hadoop.hbase.consensus.protocol.ConsensusHost;
import org.apache.hadoop.hbase.consensus.raft.events.ReplicateEntriesEvent;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.metrics.TimeStat;
import org.apache.hadoop.hbase.regionserver.wal.AbstractWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * This is an agent that runs on the RaftQuorumContext side, and it is
 * responsible for getting entries committed within a particular time window.
 *
 * There is a continuously running 'WAL Syncer' task, which will take a list
 * of WALEdits to sync on the quorum.
 */
public class QuorumAgent implements ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(QuorumAgent.class);

  private volatile RaftQuorumContext context;

  /** The period of time the agent will wait for a commit to replicate. */
  private volatile long commitTimeoutInMillis;

  /** The period of time after which the syncCommit method is to return. */
  private volatile long commitDeadlineInMillis;

  /**
   * The maximum number of entries allowed in the commit queue. Appends beyond
   * this number will be fast failed until the queue is drained.
   */
  private volatile long commitQueueEntriesLimit;

  /**
   * The maximum size of the commit queue in KB. Appends will be fast failed
   * once the queue reaches this limit until it is drained.
   */
  private volatile long commitQueueSizeLimit;

  /** The interval between retries */
  private volatile long sleepIntervalInMillis;

  private final Compression.Algorithm compressionCodec;

  // Lock to guarantee the ordering of log entries in WAL
  private final ReentrantLock appendLock = new ReentrantLock(true);
  private final Condition groupCommitBuffer = appendLock.newCondition();

  private LinkedList<WALEdit> currentList = new LinkedList<>();
  private LinkedList<WALEdit> syncList = new LinkedList<>();

  private SettableFuture<Long> currentResult;
  private SettableFuture<Long> futureResult = SettableFuture.create();

  private final ExecutorService executor;
  private volatile boolean isSyncStopped = false;

  private volatile long lastSequenceID = -1;

  private Random random = new Random();

  public QuorumAgent(RaftQuorumContext context) {
    this.context = context;
    Configuration conf = context.getConf();

    commitQueueEntriesLimit = conf.getLong(
      HConstants.QUORUM_AGENT_COMMIT_QUEUE_ENTRIES_LIMIT_KEY,
      HConstants.QUORUM_AGENT_COMMIT_QUEUE_ENTRIES_LIMIT_DEFAULT);
    commitQueueSizeLimit = conf.getLong(
      HConstants.QUORUM_AGENT_COMMIT_QUEUE_SIZE_LIMIT_KEY,
      HConstants.QUORUM_AGENT_COMMIT_QUEUE_SIZE_LIMIT_DEFAULT);
    commitTimeoutInMillis = conf.getLong(
      HConstants.QUORUM_AGENT_COMMIT_TIMEOUT_KEY,
      HConstants.QUORUM_AGENT_COMMIT_TIMEOUT_DEFAULT);
    commitDeadlineInMillis = conf.getLong(
      HConstants.QUORUM_AGENT_COMMIT_DEADLINE_KEY,
      HConstants.QUORUM_AGENT_COMMIT_DEADLINE_DEFAULT);
    sleepIntervalInMillis = conf.getLong(
      HConstants.QUORUM_CLIENT_SLEEP_INTERVAL_KEY,
      HConstants.QUORUM_CLIENT_SLEEP_INTERVAL_DEFAULT);

    compressionCodec = Compression.getCompressionAlgorithmByName(conf.get(
      HConstants.CONSENSUS_TRANSACTION_LOG_COMPRESSION_CODEC_KEY,
      HConstants.CONSENSUS_TRANSACTION_LOG_COMPRESSION_CODEC_DEFAULT));

    executor = Executors.newSingleThreadExecutor(
      new DaemonThreadFactory("Quorum-Syncer-"+ context.getQuorumName() + "-"));
    submitWALSyncerTask();
  }

  private void setCommitQueueLimits(final Configuration conf) {
    long newCommitQueueEntriesLimit = conf.getLong(
      HConstants.QUORUM_AGENT_COMMIT_QUEUE_ENTRIES_LIMIT_KEY,
      HConstants.QUORUM_AGENT_COMMIT_QUEUE_ENTRIES_LIMIT_DEFAULT);
    long newCommitQueueSizeLimit = conf.getLong(
      HConstants.QUORUM_AGENT_COMMIT_QUEUE_SIZE_LIMIT_KEY,
      HConstants.QUORUM_AGENT_COMMIT_QUEUE_SIZE_LIMIT_DEFAULT);

    if (commitQueueEntriesLimit != newCommitQueueEntriesLimit) {
      commitQueueEntriesLimit = newCommitQueueEntriesLimit;
      LOG.debug("Set commit queue entries limit for region: %s, entries: %d",
        getRaftQuorumContext().getQuorumName(), commitQueueEntriesLimit);
    }
    if (commitQueueSizeLimit != newCommitQueueSizeLimit) {
      commitQueueSizeLimit = newCommitQueueSizeLimit;
      LOG.debug("Set commit queue size limit for region: %s, size: %d",
        getRaftQuorumContext().getQuorumName(), commitQueueSizeLimit);
    }
  }

  private void setCommitQueueTimings(final Configuration conf) {
    long newCommitTimeoutInMillis = conf.getLong(
      HConstants.QUORUM_AGENT_COMMIT_TIMEOUT_KEY,
      HConstants.QUORUM_AGENT_COMMIT_TIMEOUT_DEFAULT);
    long newCommitDeadlineInMillis = conf.getLong(
      HConstants.QUORUM_AGENT_COMMIT_DEADLINE_KEY,
      HConstants.QUORUM_AGENT_COMMIT_DEADLINE_DEFAULT);
    long newSleepIntervalInMillis = conf.getLong(
      HConstants.QUORUM_CLIENT_SLEEP_INTERVAL_KEY,
      HConstants.QUORUM_CLIENT_SLEEP_INTERVAL_DEFAULT);

    if (commitTimeoutInMillis != newCommitTimeoutInMillis) {
      commitTimeoutInMillis = newCommitTimeoutInMillis;
      LOG.debug("Set commit timeout for region: %s, %d ms",
        getRaftQuorumContext().getQuorumName(), commitTimeoutInMillis);
    }
    if (commitDeadlineInMillis != newCommitDeadlineInMillis) {
      commitDeadlineInMillis = newCommitDeadlineInMillis;
      LOG.debug("Set commit deadline for region: %s, %d ms",
        getRaftQuorumContext().getQuorumName(), commitDeadlineInMillis);
    }
    if (sleepIntervalInMillis != newSleepIntervalInMillis) {
      sleepIntervalInMillis = newSleepIntervalInMillis;
      LOG.debug("Set commit sleep interval for region: %s, %d ms",
        getRaftQuorumContext().getQuorumName(), sleepIntervalInMillis);
    }
  }

  public RaftQuorumContext getRaftQuorumContext() {
    return this.context;
  }

  @Override
  public void notifyOnChange(Configuration conf) {
    setCommitQueueLimits(conf);
    setCommitQueueTimings(conf);
  }

  private long getBackoffTimeMillis() {
    double p50 = getRaftQuorumContext().getConsensusMetrics().
      getAppendEntriesLatency().getP50();
    // Get a random number of microseconds, up to half the p50.
    int randomMicros = random.nextInt() % (int) (p50 / 2.0);
    return (long) Math.max(1.0, (p50 + randomMicros) / 1000.0);
  }

  public boolean isLeader() {
    return context.isLeader();
  }

  public String getPath() {
    return context.getLogManager().getPath();
  }

  private void checkBeforeCommit() throws IOException {
    // check whether the current peer is the leader
    if (!isLeader()) {
      throw new NoLeaderForRegionException("Current region server " +
        context.getMyAddress() +
        " is not the leader for the region " + context.getQuorumName());
    }

    if (this.isSyncStopped) {
      throw new IOException("QuorumWAL syncer thread for " + context.getQuorumName() +
        " has been stopped !");
    }
  }

  private ListenableFuture<Long> internalCommit(List<WALEdit> edits)
    throws IOException {
    SettableFuture<Long> future = null;

    // Add the transaction into the group commit queue
    this.appendLock.lock();
    try {
      if (isSyncStopped) {
        throw new IOException("QuorumWAL syncer thread for " +
          context.getQuorumName() + " has been stopped!");
      }
      if (currentList.size() > commitQueueEntriesLimit) {
        getRaftQuorumContext().getConsensusMetrics()
          .incCommitQueueEntriesLimitExceeded();
        throw new CommitQueueOverloadedException(String.format(
          "Exceeded entries limit for region: %s, limit: %d, entries: %d",
          context.getQuorumName(), commitQueueEntriesLimit,
          currentList.size()), getBackoffTimeMillis());
      }

      currentList.addAll(edits);

      this.groupCommitBuffer.signal();
      future = futureResult;
    } finally {
      this.appendLock.unlock();
    }
    return future;
  }

  private ListenableFuture<Long> internalCommit(WALEdit edits)
          throws IOException {
    return internalCommit(Arrays.asList(edits));
  }

  /**
   * Append to the log synchronously.
   * @param edits WALEdit to append.
   * @return The commit index of the committed edit.
   * @throws IOException
   */
  public long syncAppend(WALEdit edits) throws IOException {
    checkBeforeCommit();

    // increase the write size
    AbstractWAL.getWriteSizeHistogram().addValue(edits.getTotalKeyValueLength());

    long start = System.nanoTime();
    ListenableFuture<Long> future = internalCommit(edits);
    // Wait for the group commit finish;
    try {
      // Wait for the transaction to complete
      long seq = future.get();
      // increase the sync time
      double syncMicros = (System.nanoTime() - start) / 1000.0;
      getRaftQuorumContext().getConsensusMetrics().getFsSyncLatency()
        .add((long)syncMicros, TimeUnit.MICROSECONDS);
      AbstractWAL.getSyncTimeHistogram().addValue(syncMicros);
      return seq;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Append to the log asynchronously.
   * @param edits
   * @return Future containing the commit index.
   * @throws IOException
   */
  public ListenableFuture<Long> asyncAppend(WALEdit edits) throws IOException {
    checkBeforeCommit();
    return internalCommit(edits);
  }

  /**
   * Same as asyncAppend(WALEdit), but for a list of WALEdit.
   * @param edits
   * @return The commit index for the list of WALEdits
   * @throws IOException
   */
  public ListenableFuture<Long> asyncAppend(List<WALEdit> edits) throws IOException {
    checkBeforeCommit();
    return internalCommit(edits);
  }

  public long getLastSequenceID() {
    return lastSequenceID;
  }

  /**
   * Stop the RaftQuorumContext. Called by an external RegionClosing thread pool
   */
  public void close() {
    this.isSyncStopped = true;
    this.executor.shutdown();
    context.stop(true);

    appendLock.lock();
    try {
      if (futureResult != null) {
        futureResult.setException(new IOException("WAL already closed"));
      }
      if (currentResult != null) {
        currentResult.setException(new IOException("WAL already closed"));
      }
    } finally {
      appendLock.unlock();
    }
  }

  /**
   * A blocking API to replicate the entries to the quorum. It waits until the
   * commit is succeeded or failed. This method is guaranteed to allow a
   * 'commitTimeoutInMillis' period for the commit to complete while maintaining
   * a strict deadline of 'commitDeadlineInMillis' on when this method
   * completes.
   *
   * @param entries The list of the @WALEdit to replicate
   * @return the commit index of the replicated entries.
   * @throws IOException if the quorum threw an exception during the replication
   */
  private long syncCommit(List<WALEdit> entries,
                          final SettableFuture<Long> result) throws Exception {
    ByteBuffer serializedEntries;
    ConsensusMetrics metrics = getRaftQuorumContext().getConsensusMetrics();

    try (TimeStat.BlockTimer latency =
                 metrics.getLogSerializationLatency().time()) {
      serializedEntries = WALEdit.serializeToByteBuffer(entries,
              System.currentTimeMillis(), compressionCodec);
    }
    int appendEntriesSize = WALEdit.getWALEditsSize(entries);
    metrics.getAppendEntriesSize().add(appendEntriesSize);
    metrics.getAppendEntriesBatchSize().add(entries.size());
    if (!compressionCodec.equals(Compression.Algorithm.NONE)) {
      int compressedSize = serializedEntries.remaining() -
              WALEdit.PAYLOAD_HEADER_SIZE;
      metrics.getAppendEntriesCompressedSize().add(compressedSize);
    } else {
      // We don't use any compression, so the compressed size would be the
      // same as the original size.
      metrics.getAppendEntriesCompressedSize().add(appendEntriesSize);
    }

      if (!context.isLeader()) {
        ConsensusHost leader = context.getLeader();
        throw new NewLeaderException(
                leader == null ? "No leader" : leader.getHostId());
      }

      ReplicateEntriesEvent event = new ReplicateEntriesEvent(false,
              serializedEntries, result);
      if (!context.offerEvent(event)) {
        ConsensusHost leader = context.getLeader();
        throw new NewLeaderException(
                leader == null ? "No leader" : leader.getHostId());
      }
      try {
        return result.get(commitDeadlineInMillis, TimeUnit.MILLISECONDS);
      } catch (Throwable e) {
        if (e instanceof TimeoutException) {
          metrics.incAppendEntriesMissedDeadline();
          LOG.warn(String.format(
                  "%s Failed to commit within the deadline of %dms", context,
                  commitDeadlineInMillis));
          throw e;
        } else {
          LOG.error(context + " Quorum commit failed", e);
          throw new Exception("Quorum commit failed because " + e);
        }
      }
  }

  private void submitWALSyncerTask() {
    executor.submit(new Runnable() {
      @Override
      public void run() {
        while (!isSyncStopped) {
          try {
            SettableFuture<Long> nextResult = SettableFuture.create();
            // Switch the current list under the appendLock
            appendLock.lock();
            try {
              if (isSyncStopped) {
                throw new IOException("QuorumWAL syncer thread for " +
                  context.getQuorumName() + " has been stopped !");
              }

              if (currentList.isEmpty()) {
                // wake up every 100ms to check if sync thread has to shut down
                groupCommitBuffer.await(100, TimeUnit.MILLISECONDS);
              }

              if (!currentList.isEmpty()) {
                // switch the buffer
                assert syncList.isEmpty();
                LinkedList<WALEdit> tmp = syncList;
                syncList = currentList;
                currentList = tmp;

                // Create a new futureResult for the next queue
                currentResult = futureResult;
                futureResult = nextResult;
              } else {
                continue;
              }

            } catch (Exception e) {
              // unexpected exception
            } finally {
              appendLock.unlock();
            }

            // Group commit to the quorum
            long groupCommitID;
            long start = System.nanoTime();
            try {
              groupCommitID = syncCommit(syncList, currentResult);

              // Set the last commitID
              assert groupCommitID > lastSequenceID;
              lastSequenceID = groupCommitID;

            } catch (Throwable e) {
              // Signal all the rpc threads with the exception
              currentResult.setException(e);
            } finally {
              // Clear the sync buffer
              syncList.clear();
            }
            // Add the group sync time
            double gsyncMicros = (System.nanoTime() - start) / 1000.0;
            getRaftQuorumContext().getConsensusMetrics().getFsGSyncLatency()
              .add((long) gsyncMicros, TimeUnit.MICROSECONDS);
            AbstractWAL.getGSyncTimeHistogram().addValue(gsyncMicros);
          } catch (Throwable e) {
            LOG.error("Unexpected exception: ", e);
          }
        }
      }
    });
  }

  public void setLastSequenceID(long seqid)
    throws IOException, ExecutionException, InterruptedException {
    lastSequenceID = seqid;
    context.reseedIndex(seqid);
  }

  public long getLastCommittedIndex() {
    return context.getLogManager().getLastValidTransactionId().getIndex();
  }

  public Compression.Algorithm getCompressionCodec() {
    return compressionCodec;
  }
}
