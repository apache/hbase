package org.apache.hadoop.hbase.consensus.metrics;

import io.airlift.stats.CounterStat;
import io.airlift.stats.Distribution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.hbase.consensus.rpc.PeerStatus;
import org.apache.hadoop.hbase.metrics.MetricsBase;
import org.apache.hadoop.hbase.metrics.TimeStat;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import com.google.common.collect.ImmutableList;

/**
 * A class used to expose various metrics around the consensus, such as the
 * number of leader election, number of stepping downs, append latencies, etc.
 */
@ThreadSafe
public class ConsensusMetrics extends MetricsBase {
  /**
   * Type string used when exporting an MBean for these metrics
   */
  public static final String TYPE = "ConsensusMetrics";
  /**
   * Domain string used when exporting an MBean for these metrics
   */
  public static final String DOMAIN = "org.apache.hadoop.hbase.consensus";

  private final String name;
  private final String procId;
  private final List<PeerMetrics> peerMetrics = new ArrayList<>();

  /**
   * Keep track of the Raft state. This is used to make a decision on which
   * metrics to publish and how to publish them.
   */
  private volatile PeerStatus.RAFT_STATE raftState =
          PeerStatus.RAFT_STATE.INVALID;

  /**
   * Leader election metrics
   */
  private final CounterStat leaderElectionAttempts = new CounterStat();
  private final CounterStat leaderElectionFailures = new CounterStat();
  private final TimeStat leaderElectionLatency =
          new TimeStat(TimeUnit.MILLISECONDS);
  // This is the metric indicating the number of times a higher rank peer
  // caught up with a lower rank leader and caused it to step down.
  private final CounterStat higherRankCaughtUpStepDown = new CounterStat();
  private final CounterStat appendEntriesStepDown = new CounterStat();

  /**
   * Timer metrics
   */
  private final CounterStat progressTimeouts = new CounterStat();
  private final CounterStat heartbeatTimeouts = new CounterStat();
  private final CounterStat heartbeatCanceled = new CounterStat();

  /**
   * Log metrics
   */
  private final TimeStat logWriteLatency = new TimeStat(TimeUnit.MICROSECONDS);
  private final TimeStat logTruncateLatency =
          new TimeStat(TimeUnit.MICROSECONDS);
  private final TimeStat logSerializationLatency =
          new TimeStat(TimeUnit.NANOSECONDS);
  private final TimeStat logDeserializationLatency =
          new TimeStat(TimeUnit.NANOSECONDS);
  private final TimeStat logAppendGreaterThanSecond =
      new TimeStat(TimeUnit.MILLISECONDS);

  /**
   * AppendEntries metrics
   */
  private final TimeStat appendEntriesLatency =
          new TimeStat(TimeUnit.MICROSECONDS);
  private final Distribution appendEntriesSize = new Distribution();
  private final Distribution appendEntriesCompressedSize = new Distribution();
  private final Distribution appendEntriesBatchSize = new Distribution();
  private final CounterStat appendEntriesRetries = new CounterStat();
  private final CounterStat appendEntriesDuplicates = new CounterStat();
  private final CounterStat appendEntriesMissedDeadline = new CounterStat();

  /** Sync Latencies */
  private final TimeStat fsSyncLatency = new TimeStat(TimeUnit.MICROSECONDS);
  private final TimeStat fsGSyncLatency = new TimeStat(TimeUnit.MICROSECONDS);

  /**
   * Commit queue metrics
   */
  private final CounterStat commitQueueEntriesLimitExceeded = new CounterStat();
  private final CounterStat commitQueueSizeLimitExceeded = new CounterStat();

  public ConsensusMetrics(final String name, final String procId,
                          final MBeanExporter exporter) {
    super(DOMAIN, TYPE, name, procId, Collections.<String, String>emptyMap(),
            exporter);
    this.name = name;
    this.procId = procId;
  }

  public ConsensusMetrics(final String name, final String hostId) {
    this(name, hostId, null);
  }

  public String getName() {
    return name;
  }

  public String getProcId() {
    return procId;
  }

  public List<PeerMetrics> getPeerMetrics() {
    synchronized (peerMetrics) {
      return ImmutableList.copyOf(peerMetrics);
    }
  }

  public PeerMetrics createPeerMetrics(final String peerId) {
    return new PeerMetrics(name, procId, peerId, getMBeanExporter());
  }

  public void exportPeerMetrics(final PeerMetrics metrics)
          throws JmxException {
    MBeanExporter exporter = getMBeanExporter();
    synchronized (peerMetrics) {
      peerMetrics.add(metrics);
    }
    if (exporter != null && !metrics.isExported()) {
      metrics.export(exporter);
    }
  }

  public void unexportPeerMetrics(final PeerMetrics metrics)
          throws JmxException {
    synchronized (peerMetrics) {
      peerMetrics.remove(metrics);
    }
    if (metrics.isExported()) {
      metrics.unexport();
    }
  }

  @Override
  public void setMBeanExporter(final MBeanExporter exporter) {
    super.setMBeanExporter(exporter);
    for (PeerMetrics metrics : getPeerMetrics()) {
      metrics.setMBeanExporter(exporter);
    }
  }

  /**
   * Convenience method which will set the given {@link MBeanExporter} for this
   * and the associated {@link LeaderMetrics} and {@link PeerMetrics} objects.
   * Upon setting the exporter this object and the {@link PeerMetrics} objects
   * will be exported as MBeans.
   *
   * @param exporter exporter to use when exporting the metrics
   * @throws JmxException if the object could not be exported
   */
  @Override
  public void export(final MBeanExporter exporter) {
    super.export(exporter);
    for (PeerMetrics metrics : getPeerMetrics()) {
      metrics.export(exporter);
    }
  }

  @Managed
  public PeerStatus.RAFT_STATE getRaftState() {
    return raftState;
  }

  public void setRaftState(PeerStatus.RAFT_STATE state) {
    raftState = state;
  }

  @Managed
  @Nested
  public CounterStat getLeaderElectionAttempts() {
    return leaderElectionAttempts;
  }

  public void incLeaderElectionAttempts() {
    leaderElectionAttempts.update(1);
  }

  @Managed
  @Nested
  public CounterStat getLeaderElectionFailures() {
    return leaderElectionFailures;
  }

  public void incLeaderElectionFailures() {
    leaderElectionFailures.update(1);
  }

  @Managed
  @Nested
  public TimeStat getLeaderElectionLatency() {
    return leaderElectionLatency;
  }

  @Managed
  @Nested
  public CounterStat getHigherRankCaughtUpStepDown() {
    return higherRankCaughtUpStepDown;
  }

  public void incHigherRankCaughtUpStepDown() {
    higherRankCaughtUpStepDown.update(1);
  }

  @Managed
  @Nested
  public CounterStat getAppendEntriesStepDown() {
    return appendEntriesStepDown;
  }

  public void incAppendEntriesStepDown() {
    appendEntriesStepDown.update(1);
  }

  @Managed
  @Nested
  public CounterStat getProgressTimeouts() {
    return progressTimeouts;
  }

  public void incProgressTimeouts() {
    progressTimeouts.update(1);
  }

  @Managed
  @Nested
  public CounterStat getHeartbeatTimeouts() {
    return heartbeatTimeouts;
  }

  public void incHeartBeatTimeouts() {
    heartbeatTimeouts.update(1);
  }

  @Managed
  @Nested
  public CounterStat getHeartbeatCanceled() {
    return heartbeatCanceled;
  }

  public void incHeartBeatCanceled() {
    heartbeatCanceled.update(1);
  }

  @Managed
  @Nested
  public TimeStat getLogWriteLatency() {
    return logWriteLatency;
  }

  @Managed
  @Nested
  public TimeStat getLogTruncateLatency() {
    return logTruncateLatency;
  }

  @Managed
  @Nested
  public TimeStat getLogSerializationLatency() {
    return logSerializationLatency;
  }

  @Managed
  @Nested
  public TimeStat getLogDeserializationLatency() {
    return logDeserializationLatency;
  }

  @Managed
  @Nested
  public TimeStat getAppendEntriesLatency() {
    return appendEntriesLatency;
  }

  @Managed
  @Nested
  public Distribution getAppendEntriesSize() {
    return appendEntriesSize;
  }

  @Managed
  @Nested
  public Distribution getAppendEntriesCompressedSize() {
    return appendEntriesCompressedSize;
  }

  @Managed
  @Nested
  public Distribution getAppendEntriesBatchSize() {
    return appendEntriesBatchSize;
  }

  @Managed
  @Nested
  public CounterStat getAppendEntriesRetries() {
    return appendEntriesRetries;
  }

  public void incAppendEntriesRetries() {
    appendEntriesRetries.update(1);
  }

  @Managed
  @Nested
  public CounterStat getAppendEntriesDuplicates() {
    return appendEntriesDuplicates;
  }

  public void incAppendEntriesDuplicates() {
    appendEntriesDuplicates.update(1);
  }

  @Managed
  @Nested
  public CounterStat getAppendEntriesMissedDeadline() {
    return appendEntriesMissedDeadline;
  }

  public void incAppendEntriesMissedDeadline() {
    appendEntriesMissedDeadline.update(1);
  }

  @Managed
  @Nested
  public TimeStat getFsSyncLatency() {
    return fsSyncLatency;
  }

  @Managed
  @Nested
  public TimeStat getFsGSyncLatency() {
    return fsGSyncLatency;
  }

  @Managed
  @Nested
  public CounterStat getCommitQueueEntriesLimitExceeded() {
    return commitQueueEntriesLimitExceeded;
  }

  public void incCommitQueueEntriesLimitExceeded() {
    commitQueueEntriesLimitExceeded.update(1);
  }

  @Managed
  @Nested
  public CounterStat getCommitQueueSizeLimitExceeded() {
    return commitQueueSizeLimitExceeded;
  }

  public void incCommitQueueSizeLimitExceeded() {
    commitQueueSizeLimitExceeded.update(1);
  }

  @Managed
  @Nested
  public TimeStat getLogAppendGreaterThanSecond() {
    return logAppendGreaterThanSecond;
  }
}
