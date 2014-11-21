package org.apache.hadoop.hbase.consensus.metrics;

import io.airlift.stats.CounterStat;
import io.airlift.stats.Distribution;
import org.apache.hadoop.hbase.metrics.MetricsBase;
import org.apache.hadoop.hbase.metrics.TimeStat;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class PeerMetrics extends MetricsBase {
  public static final String TYPE = "PeerMetrics";

  /** General metrics */
  private final CounterStat RPCErrorEvents = new CounterStat();

  /** Leader election metrics */
  private final CounterStat voteRequestFailures = new CounterStat();

  /** AppendEntries metrics */
  private final CounterStat appendEntriesFailures = new CounterStat();
  private final TimeStat appendEntriesLatency =
          new TimeStat(TimeUnit.MICROSECONDS);
  private final AtomicLong appendEntriesLag = new AtomicLong(0);

  /** Batch recovery metrics */
  private final Distribution batchRecoverySize = new Distribution();
  private final TimeStat batchRecoveryLatency =
          new TimeStat(TimeUnit.MICROSECONDS);

  public PeerMetrics(final String name, final String procId,
                     final String peerId, final MBeanExporter exporter) {
    super(ConsensusMetrics.DOMAIN, TYPE, name, procId,
            getExtendedAttributes(peerId), exporter);
  }

  @Managed
  @Nested
  public CounterStat getRPCErrorEvents() {
    return RPCErrorEvents;
  }

  public void incRPCErrorEvents() {
    RPCErrorEvents.update(1);
  }

  @Managed
  @Nested
  public CounterStat getVoteRequestFailures() {
    return voteRequestFailures;
  }

  public void incVoteRequestFailures() {
    voteRequestFailures.update(1);
  }

  @Managed
  @Nested
  public CounterStat getAppendEntriesFailures() {
    return appendEntriesFailures;
  }

  public void incAppendEntriesFailures() {
    appendEntriesFailures.update(1);
  }

  @Managed
  @Nested
  public TimeStat getAppendEntriesLatency() {
    return appendEntriesLatency;
  }

  @Managed
  @Nested
  public Distribution getBatchRecoverySize() {
    return batchRecoverySize;
  }

  @Managed
  @Nested
  public TimeStat getBatchRecoveryLatency() {
    return batchRecoveryLatency;
  }

  @Managed
  public long getAppendEntriesLag() {
    return appendEntriesLag.get();
  }

  public void setAppendEntriesLag(long lag) {
    appendEntriesLag.set(lag < 0 ? 0 : lag);
  }

  protected static Map<String, String> getExtendedAttributes(
          final String peerId) {
    Map<String, String> extendedAttributes = new TreeMap<>();
    extendedAttributes.put("peer", peerId);
    return extendedAttributes;
  }

  public static String getMBeanName(final String name, final String procId,
                                    final String peerId) {
    return getMBeanName(ConsensusMetrics.DOMAIN, TYPE, name, procId,
            getExtendedAttributes(peerId));
  }
}
