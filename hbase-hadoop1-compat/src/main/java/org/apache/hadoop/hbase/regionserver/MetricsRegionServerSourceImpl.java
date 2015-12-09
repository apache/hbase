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

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;

/**
 * Hadoop1 implementation of MetricsRegionServerSource.
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern
 */
public class MetricsRegionServerSourceImpl
    extends BaseSourceImpl implements MetricsRegionServerSource {

  final MetricsRegionServerWrapper rsWrap;
  private final MetricHistogram putHisto;
  private final MetricHistogram deleteHisto;
  private final MetricHistogram getHisto;
  private final MetricHistogram incrementHisto;
  private final MetricHistogram appendHisto;
  private final MetricHistogram replayHisto;
  private final MetricHistogram scanNextHisto;

  private final MetricMutableCounterLong slowPut;
  private final MetricMutableCounterLong slowDelete;
  private final MetricMutableCounterLong slowGet;
  private final MetricMutableCounterLong slowIncrement;
  private final MetricMutableCounterLong slowAppend;
  private final MetricMutableCounterLong splitRequest;
  private final MetricMutableCounterLong splitSuccess;

  private final MetricHistogram splitTimeHisto;
  private final MetricHistogram flushTimeHisto;

  public MetricsRegionServerSourceImpl(MetricsRegionServerWrapper rsWrap) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT, rsWrap);
  }

  public MetricsRegionServerSourceImpl(String metricsName,
                                       String metricsDescription,
                                       String metricsContext,
                                       String metricsJmxContext,
                                       MetricsRegionServerWrapper rsWrap) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.rsWrap = rsWrap;

    putHisto = getMetricsRegistry().newTimeHistogram(MUTATE_KEY);
    slowPut = getMetricsRegistry().newCounter(SLOW_MUTATE_KEY, SLOW_MUTATE_DESC, 0l);

    deleteHisto = getMetricsRegistry().newTimeHistogram(DELETE_KEY);
    slowDelete = getMetricsRegistry().newCounter(SLOW_DELETE_KEY, SLOW_DELETE_DESC, 0l);

    getHisto = getMetricsRegistry().newTimeHistogram(GET_KEY);
    slowGet = getMetricsRegistry().newCounter(SLOW_GET_KEY, SLOW_GET_DESC, 0l);

    incrementHisto = getMetricsRegistry().newTimeHistogram(INCREMENT_KEY);
    slowIncrement = getMetricsRegistry().newCounter(SLOW_INCREMENT_KEY, SLOW_INCREMENT_DESC, 0l);

    appendHisto = getMetricsRegistry().newTimeHistogram(APPEND_KEY);
    slowAppend = getMetricsRegistry().newCounter(SLOW_APPEND_KEY, SLOW_APPEND_DESC, 0l);

    replayHisto = getMetricsRegistry().newTimeHistogram(REPLAY_KEY);
    scanNextHisto = getMetricsRegistry().newTimeHistogram(SCAN_NEXT_KEY);

    splitTimeHisto = getMetricsRegistry().newTimeHistogram(SPLIT_KEY);
    flushTimeHisto = getMetricsRegistry().newTimeHistogram(FLUSH_KEY);

    splitRequest = getMetricsRegistry().newCounter(SPLIT_REQUEST_KEY, SPLIT_REQUEST_DESC, 0l);
    splitSuccess = getMetricsRegistry().newCounter(SPLIT_SUCCESS_KEY, SPLIT_SUCCESS_DESC, 0l);
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
  public void updateScannerNext(long scanSize) {
    scanNextHisto.add(scanSize);
  }

  @Override
  public void incrSlowPut() {
    slowPut.incr();
  }

  @Override
  public void incrSlowDelete() {
    slowDelete.incr();
  }

  @Override
  public void incrSlowGet() {
    slowGet.incr();
  }

  @Override
  public void incrSlowIncrement() {
    slowIncrement.incr();
  }

  @Override
  public void incrSlowAppend() {
    slowAppend.incr();
  }

  @Override
  public void incrSplitRequest() {
    splitRequest.incr();
  }

  @Override
  public void incrSplitSuccess() {
    splitSuccess.incr();
  }

  @Override
  public void updateSplitTime(long t) {
    splitTimeHisto.add(t);
  }

  @Override
  public void updateFlushTime(long t) {
    flushTimeHisto.add(t);
  }

  /**
   * Yes this is a get function that doesn't return anything.  Thanks Hadoop for breaking all
   * expectations of java programmers.  Instead of returning anything Hadoop metrics expects
   * getMetrics to push the metrics into the metricsBuilder.
   *
   * @param metricsBuilder Builder to accept metrics
   * @param all            push all or only changed?
   */
  @Override
  public void getMetrics(MetricsBuilder metricsBuilder, boolean all) {

    MetricsRecordBuilder mrb = metricsBuilder.addRecord(metricsName);

    // rsWrap can be null because this function is called inside of init.
    if (rsWrap != null) {
      mrb.addGauge(REGION_COUNT, REGION_COUNT_DESC, rsWrap.getNumOnlineRegions())
          .addGauge(STORE_COUNT, STORE_COUNT_DESC, rsWrap.getNumStores())
          .addGauge(HLOGFILE_COUNT, HLOGFILE_COUNT_DESC, rsWrap.getNumHLogFiles())
          .addGauge(HLOGFILE_SIZE, HLOGFILE_SIZE_DESC, rsWrap.getHLogFileSize())
          .addGauge(STOREFILE_COUNT, STOREFILE_COUNT_DESC, rsWrap.getNumStoreFiles())
          .addGauge(MEMSTORE_SIZE, MEMSTORE_SIZE_DESC, rsWrap.getMemstoreSize())
          .addGauge(STOREFILE_SIZE, STOREFILE_SIZE_DESC, rsWrap.getStoreFileSize())
          .addGauge(RS_START_TIME_NAME, RS_START_TIME_DESC, rsWrap.getStartCode())
          .addCounter(TOTAL_REQUEST_COUNT, TOTAL_REQUEST_COUNT_DESC, rsWrap.getTotalRequestCount())
          .addCounter(READ_REQUEST_COUNT, READ_REQUEST_COUNT_DESC, rsWrap.getReadRequestsCount())
          .addCounter(WRITE_REQUEST_COUNT, WRITE_REQUEST_COUNT_DESC, rsWrap.getWriteRequestsCount())
          .addCounter(CHECK_MUTATE_FAILED_COUNT,
              CHECK_MUTATE_FAILED_COUNT_DESC,
              rsWrap.getCheckAndMutateChecksFailed())
          .addCounter(CHECK_MUTATE_PASSED_COUNT,
              CHECK_MUTATE_PASSED_COUNT_DESC,
              rsWrap.getCheckAndMutateChecksPassed())
          .addGauge(STOREFILE_INDEX_SIZE, STOREFILE_INDEX_SIZE_DESC, rsWrap.getStoreFileIndexSize())
          .addGauge(STATIC_INDEX_SIZE, STATIC_INDEX_SIZE_DESC, rsWrap.getTotalStaticIndexSize())
          .addGauge(STATIC_BLOOM_SIZE, STATIC_BLOOM_SIZE_DESC, rsWrap.getTotalStaticBloomSize())
          .addGauge(NUMBER_OF_MUTATIONS_WITHOUT_WAL,
              NUMBER_OF_MUTATIONS_WITHOUT_WAL_DESC,
              rsWrap.getNumMutationsWithoutWAL())
          .addGauge(DATA_SIZE_WITHOUT_WAL,
              DATA_SIZE_WITHOUT_WAL_DESC,
              rsWrap.getDataInMemoryWithoutWAL())
          .addGauge(PERCENT_FILES_LOCAL, PERCENT_FILES_LOCAL_DESC, rsWrap.getPercentFileLocal())
          .addGauge(COMPACTION_QUEUE_LENGTH,
              COMPACTION_QUEUE_LENGTH_DESC,
              rsWrap.getCompactionQueueSize())
          .addGauge(LARGE_COMPACTION_QUEUE_LENGTH,
              COMPACTION_QUEUE_LENGTH_DESC,
              rsWrap.getLargeCompactionQueueSize())
          .addGauge(SMALL_COMPACTION_QUEUE_LENGTH,
              COMPACTION_QUEUE_LENGTH_DESC,
              rsWrap.getSmallCompactionQueueSize())
          .addGauge(FLUSH_QUEUE_LENGTH, FLUSH_QUEUE_LENGTH_DESC, rsWrap.getFlushQueueSize())
          .addGauge(BLOCK_CACHE_FREE_SIZE, BLOCK_CACHE_FREE_DESC, rsWrap.getBlockCacheFreeSize())
          .addGauge(BLOCK_CACHE_COUNT, BLOCK_CACHE_COUNT_DESC, rsWrap.getBlockCacheCount())
          .addGauge(BLOCK_CACHE_SIZE, BLOCK_CACHE_SIZE_DESC, rsWrap.getBlockCacheSize())
          .addCounter(BLOCK_CACHE_HIT_COUNT,
              BLOCK_CACHE_HIT_COUNT_DESC,
              rsWrap.getBlockCacheHitCount())
          .addCounter(BLOCK_CACHE_MISS_COUNT,
              BLOCK_COUNT_MISS_COUNT_DESC,
              rsWrap.getBlockCacheMissCount())
          .addCounter(BLOCK_CACHE_EVICTION_COUNT,
              BLOCK_CACHE_EVICTION_COUNT_DESC,
              rsWrap.getBlockCacheEvictedCount())
          .addGauge(BLOCK_CACHE_HIT_PERCENT,
              BLOCK_CACHE_HIT_PERCENT_DESC,
              rsWrap.getBlockCacheHitPercent())
          .addGauge(BLOCK_CACHE_EXPRESS_HIT_PERCENT,
              BLOCK_CACHE_EXPRESS_HIT_PERCENT_DESC,
              rsWrap.getBlockCacheHitCachingPercent())
          .addCounter(BLOCK_CACHE_FAILED_INSERTION_COUNT,
              BLOCK_CACHE_FAILED_INSERTION_COUNT_DESC,rsWrap.getBlockCacheFailedInsertions())
          .addCounter(UPDATES_BLOCKED_TIME, UPDATES_BLOCKED_DESC, rsWrap.getUpdatesBlockedTime())
          .addCounter(FLUSHED_CELLS, FLUSHED_CELLS_DESC, rsWrap.getFlushedCellsCount())
          .addCounter(COMPACTED_CELLS, COMPACTED_CELLS_DESC, rsWrap.getCompactedCellsCount())
          .addCounter(MAJOR_COMPACTED_CELLS, MAJOR_COMPACTED_CELLS_DESC,
              rsWrap.getMajorCompactedCellsCount())
          .addCounter(FLUSHED_CELLS_SIZE, FLUSHED_CELLS_SIZE_DESC, rsWrap.getFlushedCellsSize())
          .addCounter(COMPACTED_CELLS_SIZE, COMPACTED_CELLS_SIZE_DESC,
              rsWrap.getCompactedCellsSize())
          .addCounter(MAJOR_COMPACTED_CELLS_SIZE, MAJOR_COMPACTED_CELLS_SIZE_DESC,
              rsWrap.getMajorCompactedCellsSize())

          .addCounter(BLOCKED_REQUESTS_COUNT, BLOCKED_REQUESTS_COUNT_DESC,
              rsWrap.getBlockedRequestsCount())

          .tag(ZOOKEEPER_QUORUM_NAME, ZOOKEEPER_QUORUM_DESC, rsWrap.getZookeeperQuorum())
          .tag(SERVER_NAME_NAME, SERVER_NAME_DESC, rsWrap.getServerName())
          .tag(CLUSTER_ID_NAME, CLUSTER_ID_DESC, rsWrap.getClusterId());
    }

    metricsRegistry.snapshot(mrb, all);
  }


}
