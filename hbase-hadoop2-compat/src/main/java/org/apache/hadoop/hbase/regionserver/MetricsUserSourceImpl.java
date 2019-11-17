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

package org.apache.hadoop.hbase.regionserver;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MetricsUserSourceImpl implements MetricsUserSource {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsUserSourceImpl.class);

  private final String userNamePrefix;

  private final String user;

  private final String userGetKey;
  private final String userScanTimeKey;
  private final String userPutKey;
  private final String userDeleteKey;
  private final String userIncrementKey;
  private final String userAppendKey;
  private final String userReplayKey;

  private MetricHistogram getHisto;
  private MetricHistogram scanTimeHisto;
  private MetricHistogram putHisto;
  private MetricHistogram deleteHisto;
  private MetricHistogram incrementHisto;
  private MetricHistogram appendHisto;
  private MetricHistogram replayHisto;

  private final int hashCode;

  private AtomicBoolean closed = new AtomicBoolean(false);
  private final MetricsUserAggregateSourceImpl agg;
  private final DynamicMetricsRegistry registry;

  private ConcurrentHashMap<String, ClientMetrics> clientMetricsMap;

  static class ClientMetricsImpl implements ClientMetrics {
    private final String hostName;
    final LongAdder readRequestsCount = new LongAdder();
    final LongAdder writeRequestsCount = new LongAdder();
    final LongAdder filteredRequestsCount = new LongAdder();

    public ClientMetricsImpl(String hostName) {
      this.hostName = hostName;
    }

    @Override public void incrementReadRequest() {
      readRequestsCount.increment();
    }

    @Override public void incrementWriteRequest() {
      writeRequestsCount.increment();
    }

    @Override public String getHostName() {
      return hostName;
    }

    @Override public long getReadRequestsCount() {
      return readRequestsCount.sum();
    }

    @Override public long getWriteRequestsCount() {
      return writeRequestsCount.sum();
    }

    @Override public void incrementFilteredReadRequests() {
      filteredRequestsCount.increment();

    }

    @Override public long getFilteredReadRequests() {
      return filteredRequestsCount.sum();
    }
  }

  public MetricsUserSourceImpl(String user, MetricsUserAggregateSourceImpl agg) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating new MetricsUserSourceImpl for user " + user);
    }

    this.user = user;
    this.agg = agg;
    this.registry = agg.getMetricsRegistry();

    this.userNamePrefix = "user_" + user + "_metric_";

    hashCode = userNamePrefix.hashCode();

    userGetKey = userNamePrefix + MetricsRegionServerSource.GET_KEY;
    userScanTimeKey = userNamePrefix + MetricsRegionServerSource.SCAN_TIME_KEY;
    userPutKey = userNamePrefix + MetricsRegionServerSource.PUT_KEY;
    userDeleteKey = userNamePrefix + MetricsRegionServerSource.DELETE_KEY;
    userIncrementKey = userNamePrefix + MetricsRegionServerSource.INCREMENT_KEY;
    userAppendKey = userNamePrefix + MetricsRegionServerSource.APPEND_KEY;
    userReplayKey = userNamePrefix + MetricsRegionServerSource.REPLAY_KEY;
    clientMetricsMap = new ConcurrentHashMap<>();
    agg.register(this);
  }

  @Override
  public void register() {
    synchronized (this) {
      getHisto = registry.newTimeHistogram(userGetKey);
      scanTimeHisto = registry.newTimeHistogram(userScanTimeKey);
      putHisto = registry.newTimeHistogram(userPutKey);
      deleteHisto = registry.newTimeHistogram(userDeleteKey);
      incrementHisto = registry.newTimeHistogram(userIncrementKey);
      appendHisto = registry.newTimeHistogram(userAppendKey);
      replayHisto = registry.newTimeHistogram(userReplayKey);
    }
  }

  @Override
  public void deregister() {
    boolean wasClosed = closed.getAndSet(true);

    // Has someone else already closed this for us?
    if (wasClosed) {
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing user Metrics for user: " + user);
    }

    synchronized (this) {
      registry.removeMetric(userGetKey);
      registry.removeMetric(userScanTimeKey);
      registry.removeMetric(userPutKey);
      registry.removeMetric(userDeleteKey);
      registry.removeMetric(userIncrementKey);
      registry.removeMetric(userAppendKey);
      registry.removeMetric(userReplayKey);
    }
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public int compareTo(MetricsUserSource source) {
    if (source == null) {
      return -1;
    }
    if (!(source instanceof MetricsUserSourceImpl)) {
      return -1;
    }

    MetricsUserSourceImpl impl = (MetricsUserSourceImpl) source;

    return Long.compare(hashCode, impl.hashCode);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this ||
        (obj instanceof MetricsUserSourceImpl && compareTo((MetricsUserSourceImpl) obj) == 0);
  }

  void snapshot(MetricsRecordBuilder mrb, boolean ignored) {
    // If there is a close that started be double extra sure
    // that we're not getting any locks and not putting data
    // into the metrics that should be removed. So early out
    // before even getting the lock.
    if (closed.get()) {
      return;
    }

    // Grab the read
    // This ensures that removes of the metrics
    // can't happen while we are putting them back in.
    synchronized (this) {

      // It's possible that a close happened between checking
      // the closed variable and getting the lock.
      if (closed.get()) {
        return;
      }
    }
  }

  @Override
  public void updatePut(long t) {
    putHisto.add(t);
  }

  @Override
  public void updateDelete(long t) {
    deleteHisto.add(t);
  }

  @Override
  public void updateGet(long t) {
    getHisto.add(t);
  }

  @Override
  public void updateIncrement(long t) {
    incrementHisto.add(t);
  }

  @Override
  public void updateAppend(long t) {
    appendHisto.add(t);
  }

  @Override
  public void updateReplay(long t) {
    replayHisto.add(t);
  }

  @Override
  public void updateScanTime(long t) {
    scanTimeHisto.add(t);
  }

  @Override public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder mrb = metricsCollector.addRecord(this.userNamePrefix);
    registry.snapshot(mrb, all);
  }

  @Override public Map<String, ClientMetrics> getClientMetrics() {
    return Collections.unmodifiableMap(clientMetricsMap);
  }

  @Override public ClientMetrics getOrCreateMetricsClient(String client) {
    ClientMetrics source = clientMetricsMap.get(client);
    if (source != null) {
      return source;
    }
    source = new ClientMetricsImpl(client);
    ClientMetrics prev = clientMetricsMap.putIfAbsent(client, source);
    if (prev != null) {
      return prev;
    }
    return source;
  }

}
