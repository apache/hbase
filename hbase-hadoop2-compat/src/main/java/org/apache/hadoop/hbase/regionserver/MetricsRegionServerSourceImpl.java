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


import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MetricsFactory;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.yetus.audience.InterfaceAudience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hadoop2 implementation of MetricsRegionServerSource.
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern
 */
@InterfaceAudience.Private
public class MetricsRegionServerSourceImpl
    extends BaseSourceImpl implements MetricsRegionServerSource {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsRegionServerSourceImpl.class);


  final MetricsRegionServerWrapper rsWrap;
  private final MetricHistogram putHisto;
  private final MetricHistogram putBatchHisto;
  private final MetricHistogram deleteHisto;
  private final MetricHistogram deleteBatchHisto;
  private final MetricHistogram checkAndDeleteHisto;
  private final MetricHistogram checkAndPutHisto;
  private final MetricHistogram getHisto;
  private final MetricHistogram incrementHisto;
  private final MetricHistogram appendHisto;
  private final MetricHistogram replayHisto;
  private final MetricHistogram scanSizeHisto;
  private final MetricHistogram scanTimeHisto;

  private final MutableFastCounter slowPut;
  private final MutableFastCounter slowDelete;
  private final MutableFastCounter slowGet;
  private final MutableFastCounter slowIncrement;
  private final MutableFastCounter slowAppend;

  // split related metrics
  private final MutableFastCounter splitRequest;
  private final MutableFastCounter splitSuccess;
  private final MetricHistogram splitTimeHisto;

  // flush related metrics
  private final MetricHistogram flushTimeHisto;
  private final MetricHistogram flushMemstoreSizeHisto;
  private final MetricHistogram flushOutputSizeHisto;
  private final MutableFastCounter flushedMemstoreBytes;
  private final MutableFastCounter flushedOutputBytes;

  // compaction related metrics
  private final MetricHistogram compactionTimeHisto;
  private final MetricHistogram compactionInputFileCountHisto;
  private final MetricHistogram compactionInputSizeHisto;
  private final MetricHistogram compactionOutputFileCountHisto;
  private final MetricHistogram compactionOutputSizeHisto;
  private final MutableFastCounter compactedInputBytes;
  private final MutableFastCounter compactedOutputBytes;

  private final MetricHistogram majorCompactionTimeHisto;
  private final MetricHistogram majorCompactionInputFileCountHisto;
  private final MetricHistogram majorCompactionInputSizeHisto;
  private final MetricHistogram majorCompactionOutputFileCountHisto;
  private final MetricHistogram majorCompactionOutputSizeHisto;
  private final MutableFastCounter majorCompactedInputBytes;
  private final MutableFastCounter majorCompactedOutputBytes;

  // pause monitor metrics
  private final MutableFastCounter infoPauseThresholdExceeded;
  private final MutableFastCounter warnPauseThresholdExceeded;
  private final MetricHistogram pausesWithGc;
  private final MetricHistogram pausesWithoutGc;

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

    // See the comment in getMetrics. This is similar but for registry-based metrics;
    // if tables are via tags, don't output the same metrics as MetricsTableSourceImpl.
    boolean areTablesViaTags = MetricsTableAggregateSourceImpl.areTablesViaTags(rsWrap);
    boolean areLatenciesViaTags = areTablesViaTags && Boolean.parseBoolean(rsWrap.getConfVar(
      MetricsTableSourceImpl.RS_ENABLE_TABLE_METRICS_KEY,
      Boolean.toString(MetricsTableSourceImpl.RS_ENABLE_TABLE_METRICS_DEFAULT)));
    MetricsFactory tableFactory = areTablesViaTags? new MetricsFactory() : getMetricsRegistry();
    MetricsFactory tableLatencyFactory = areLatenciesViaTags ? tableFactory : getMetricsRegistry();

    putHisto = tableLatencyFactory.newTimeHistogram(PUT_KEY, "");
    putBatchHisto = tableLatencyFactory.newTimeHistogram(PUT_BATCH_KEY, "");
    deleteHisto = tableLatencyFactory.newTimeHistogram(DELETE_KEY, "");
    deleteBatchHisto = tableLatencyFactory.newTimeHistogram(DELETE_BATCH_KEY, "");
    scanSizeHisto = tableLatencyFactory.newSizeHistogram(SCAN_SIZE_KEY, "");
    scanTimeHisto = tableLatencyFactory.newTimeHistogram(SCAN_TIME_KEY, "");
    getHisto = tableLatencyFactory.newTimeHistogram(GET_KEY, "");
    incrementHisto = tableLatencyFactory.newTimeHistogram(INCREMENT_KEY, "");
    appendHisto = tableLatencyFactory.newTimeHistogram(APPEND_KEY, "");

    flushTimeHisto = tableFactory.newTimeHistogram(FLUSH_TIME, FLUSH_TIME_DESC);
    flushMemstoreSizeHisto = tableFactory
        .newSizeHistogram(FLUSH_MEMSTORE_SIZE, FLUSH_MEMSTORE_SIZE_DESC);
    flushOutputSizeHisto = tableFactory.newSizeHistogram(FLUSH_OUTPUT_SIZE,
        FLUSH_OUTPUT_SIZE_DESC);
    flushedOutputBytes = tableFactory.newCounter(FLUSHED_OUTPUT_BYTES,
        FLUSHED_OUTPUT_BYTES_DESC, 0L);
    flushedMemstoreBytes = tableFactory.newCounter(FLUSHED_MEMSTORE_BYTES,
        FLUSHED_MEMSTORE_BYTES_DESC, 0L);

    compactionTimeHisto = tableFactory
        .newTimeHistogram(COMPACTION_TIME, COMPACTION_TIME_DESC);
    compactionInputFileCountHisto = tableFactory
      .newHistogram(COMPACTION_INPUT_FILE_COUNT, COMPACTION_INPUT_FILE_COUNT_DESC);
    compactionInputSizeHisto = tableFactory
        .newSizeHistogram(COMPACTION_INPUT_SIZE, COMPACTION_INPUT_SIZE_DESC);
    compactionOutputFileCountHisto = tableFactory
        .newHistogram(COMPACTION_OUTPUT_FILE_COUNT, COMPACTION_OUTPUT_FILE_COUNT_DESC);
    compactionOutputSizeHisto = tableFactory
      .newSizeHistogram(COMPACTION_OUTPUT_SIZE, COMPACTION_OUTPUT_SIZE_DESC);
    compactedInputBytes = tableFactory
        .newCounter(COMPACTED_INPUT_BYTES, COMPACTED_INPUT_BYTES_DESC, 0L);
    compactedOutputBytes = tableFactory
        .newCounter(COMPACTED_OUTPUT_BYTES, COMPACTED_OUTPUT_BYTES_DESC, 0L);

    majorCompactionTimeHisto = tableFactory
        .newTimeHistogram(MAJOR_COMPACTION_TIME, MAJOR_COMPACTION_TIME_DESC);
    majorCompactionInputFileCountHisto = tableFactory
      .newHistogram(MAJOR_COMPACTION_INPUT_FILE_COUNT, MAJOR_COMPACTION_INPUT_FILE_COUNT_DESC);
    majorCompactionInputSizeHisto = tableFactory
        .newSizeHistogram(MAJOR_COMPACTION_INPUT_SIZE, MAJOR_COMPACTION_INPUT_SIZE_DESC);
    majorCompactionOutputFileCountHisto = tableFactory
        .newHistogram(MAJOR_COMPACTION_OUTPUT_FILE_COUNT, MAJOR_COMPACTION_OUTPUT_FILE_COUNT_DESC);
    majorCompactionOutputSizeHisto = tableFactory
      .newSizeHistogram(MAJOR_COMPACTION_OUTPUT_SIZE, MAJOR_COMPACTION_OUTPUT_SIZE_DESC);
    majorCompactedInputBytes = tableFactory
        .newCounter(MAJOR_COMPACTED_INPUT_BYTES, MAJOR_COMPACTED_INPUT_BYTES_DESC, 0L);
    majorCompactedOutputBytes = tableFactory
        .newCounter(MAJOR_COMPACTED_OUTPUT_BYTES, MAJOR_COMPACTED_OUTPUT_BYTES_DESC, 0L);

    splitTimeHisto = tableFactory.newTimeHistogram(SPLIT_KEY, "");
    splitRequest = tableFactory.newCounter(SPLIT_REQUEST_KEY, SPLIT_REQUEST_DESC, 0L);
    splitSuccess = tableFactory.newCounter(SPLIT_SUCCESS_KEY, SPLIT_SUCCESS_DESC, 0L);

    // End metrics we currently output per table. The rest are always output by server.

    slowPut = getMetricsRegistry().newCounter(SLOW_PUT_KEY, SLOW_PUT_DESC, 0L);
    slowDelete = getMetricsRegistry().newCounter(SLOW_DELETE_KEY, SLOW_DELETE_DESC, 0L);
    checkAndDeleteHisto = getMetricsRegistry().newTimeHistogram(CHECK_AND_DELETE_KEY);
    checkAndPutHisto = getMetricsRegistry().newTimeHistogram(CHECK_AND_PUT_KEY);
    slowGet = getMetricsRegistry().newCounter(SLOW_GET_KEY, SLOW_GET_DESC, 0L);
    slowIncrement = getMetricsRegistry().newCounter(SLOW_INCREMENT_KEY, SLOW_INCREMENT_DESC, 0L);
    slowAppend = getMetricsRegistry().newCounter(SLOW_APPEND_KEY, SLOW_APPEND_DESC, 0L);

    replayHisto = getMetricsRegistry().newTimeHistogram(REPLAY_KEY);

    // pause monitor metrics
    infoPauseThresholdExceeded = getMetricsRegistry().newCounter(INFO_THRESHOLD_COUNT_KEY,
      INFO_THRESHOLD_COUNT_DESC, 0L);
    warnPauseThresholdExceeded = getMetricsRegistry().newCounter(WARN_THRESHOLD_COUNT_KEY,
      WARN_THRESHOLD_COUNT_DESC, 0L);
    pausesWithGc = getMetricsRegistry().newTimeHistogram(PAUSE_TIME_WITH_GC_KEY);
    pausesWithoutGc = getMetricsRegistry().newTimeHistogram(PAUSE_TIME_WITHOUT_GC_KEY);
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
  public void updateScanSize(long scanSize) {
    scanSizeHisto.add(scanSize);
  }

  @Override
  public void updateScanTime(long t) {
    scanTimeHisto.add(t);
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

  @Override
  public void updateFlushMemStoreSize(long bytes) {
    flushMemstoreSizeHisto.add(bytes);
    flushedMemstoreBytes.incr(bytes);
  }

  @Override
  public void updateFlushOutputSize(long bytes) {
    flushOutputSizeHisto.add(bytes);
    flushedOutputBytes.incr(bytes);
  }

  @Override
  public void updateCompactionTime(boolean isMajor, long t) {
    compactionTimeHisto.add(t);
    if (isMajor) {
      majorCompactionTimeHisto.add(t);
    }
  }

  @Override
  public void updateCompactionInputFileCount(boolean isMajor, long c) {
    compactionInputFileCountHisto.add(c);
    if (isMajor) {
      majorCompactionInputFileCountHisto.add(c);
    }
  }

  @Override
  public void updateCompactionInputSize(boolean isMajor, long bytes) {
    compactionInputSizeHisto.add(bytes);
    compactedInputBytes.incr(bytes);
    if (isMajor) {
      majorCompactionInputSizeHisto.add(bytes);
      majorCompactedInputBytes.incr(bytes);
    }
  }

  @Override
  public void updateCompactionOutputFileCount(boolean isMajor, long c) {
    compactionOutputFileCountHisto.add(c);
    if (isMajor) {
      majorCompactionOutputFileCountHisto.add(c);
    }
  }

  @Override
  public void updateCompactionOutputSize(boolean isMajor, long bytes) {
    compactionOutputSizeHisto.add(bytes);
    compactedOutputBytes.incr(bytes);
    if (isMajor) {
      majorCompactionOutputSizeHisto.add(bytes);
      majorCompactedOutputBytes.incr(bytes);
    }
  }

  /**
   * Yes this is a get function that doesn't return anything.  Thanks Hadoop for breaking all
   * expectations of java programmers.  Instead of returning anything Hadoop metrics expects
   * getMetrics to push the metrics into the collector.
   *
   * @param metricsCollector Collector to accept metrics
   * @param all              push all or only changed?
   */
  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder mrb = metricsCollector.addRecord(metricsName);

    // rsWrap can be null because this function is called inside of init.
    if (rsWrap != null) {
      // TODO: this is rather ugly... there are too many ways to handle metrics in HBase.
      //       There needs to be refactoring where ALL the metrics are in one place,
      //       handled the same, and the e.g. same request count is handled in exactly one place.
      //       For now here we'd make an assumption of how the independent table-metrics thingie
      //       handles a subset of its metrics (others that go thru registry are handled this way
      //       via "TableMetrics" and "MetricsTable"). Sigh..
      boolean skipTableMetrics = MetricsTableAggregateSourceImpl.areTablesViaTags(rsWrap);
      mrb = addGaugesToMetricsRecordBuilder(mrb, skipTableMetrics)
              .addCounter(Interns.info(TOTAL_ROW_ACTION_REQUEST_COUNT,
                      TOTAL_ROW_ACTION_REQUEST_COUNT_DESC), rsWrap.getTotalRowActionRequestCount())
              .addCounter(Interns.info(RPC_GET_REQUEST_COUNT, RPC_GET_REQUEST_COUNT_DESC),
                      rsWrap.getRpcGetRequestsCount())
              .addCounter(Interns.info(RPC_SCAN_REQUEST_COUNT, RPC_SCAN_REQUEST_COUNT_DESC),
                      rsWrap.getRpcScanRequestsCount())
              .addCounter(Interns.info(RPC_MULTI_REQUEST_COUNT, RPC_MULTI_REQUEST_COUNT_DESC),
                      rsWrap.getRpcMultiRequestsCount())
              .addCounter(Interns.info(RPC_MUTATE_REQUEST_COUNT, RPC_MUTATE_REQUEST_COUNT_DESC),
                      rsWrap.getRpcMutateRequestsCount())
              .addCounter(Interns.info(CHECK_MUTATE_FAILED_COUNT, CHECK_MUTATE_FAILED_COUNT_DESC),
                      rsWrap.getCheckAndMutateChecksFailed())
              .addCounter(Interns.info(CHECK_MUTATE_PASSED_COUNT, CHECK_MUTATE_PASSED_COUNT_DESC),
                      rsWrap.getCheckAndMutateChecksPassed())
              .addCounter(Interns.info(BLOCK_CACHE_HIT_COUNT, BLOCK_CACHE_HIT_COUNT_DESC),
                      rsWrap.getBlockCacheHitCount())
              .addCounter(Interns.info(BLOCK_CACHE_PRIMARY_HIT_COUNT,
                      BLOCK_CACHE_PRIMARY_HIT_COUNT_DESC), rsWrap.getBlockCachePrimaryHitCount())
              .addCounter(Interns.info(BLOCK_CACHE_MISS_COUNT, BLOCK_COUNT_MISS_COUNT_DESC),
                      rsWrap.getBlockCacheMissCount())
              .addCounter(Interns.info(BLOCK_CACHE_PRIMARY_MISS_COUNT,
                      BLOCK_COUNT_PRIMARY_MISS_COUNT_DESC), rsWrap.getBlockCachePrimaryMissCount())
              .addCounter(Interns.info(BLOCK_CACHE_EVICTION_COUNT, BLOCK_CACHE_EVICTION_COUNT_DESC),
                      rsWrap.getBlockCacheEvictedCount())
              .addCounter(Interns.info(BLOCK_CACHE_PRIMARY_EVICTION_COUNT,
                      BLOCK_CACHE_PRIMARY_EVICTION_COUNT_DESC),
                      rsWrap.getBlockCachePrimaryEvictedCount())
              .addCounter(Interns.info(BLOCK_CACHE_FAILED_INSERTION_COUNT,
                      BLOCK_CACHE_FAILED_INSERTION_COUNT_DESC),
                      rsWrap.getBlockCacheFailedInsertions())
              .addCounter(Interns.info(BLOCK_CACHE_DATA_MISS_COUNT, ""),
                      rsWrap.getDataMissCount())
              .addCounter(Interns.info(BLOCK_CACHE_LEAF_INDEX_MISS_COUNT, ""),
                      rsWrap.getLeafIndexMissCount())
              .addCounter(Interns.info(BLOCK_CACHE_BLOOM_CHUNK_MISS_COUNT, ""),
                      rsWrap.getBloomChunkMissCount())
              .addCounter(Interns.info(BLOCK_CACHE_META_MISS_COUNT, ""),
                      rsWrap.getMetaMissCount())
              .addCounter(Interns.info(BLOCK_CACHE_ROOT_INDEX_MISS_COUNT, ""),
                      rsWrap.getRootIndexMissCount())
              .addCounter(Interns.info(BLOCK_CACHE_INTERMEDIATE_INDEX_MISS_COUNT, ""),
                      rsWrap.getIntermediateIndexMissCount())
              .addCounter(Interns.info(BLOCK_CACHE_FILE_INFO_MISS_COUNT, ""),
                      rsWrap.getFileInfoMissCount())
              .addCounter(Interns.info(BLOCK_CACHE_GENERAL_BLOOM_META_MISS_COUNT, ""),
                      rsWrap.getGeneralBloomMetaMissCount())
              .addCounter(Interns.info(BLOCK_CACHE_DELETE_FAMILY_BLOOM_MISS_COUNT, ""),
                      rsWrap.getDeleteFamilyBloomMissCount())
              .addCounter(Interns.info(BLOCK_CACHE_TRAILER_MISS_COUNT, ""),
                      rsWrap.getTrailerMissCount())
              .addCounter(Interns.info(BLOCK_CACHE_DATA_HIT_COUNT, ""),
                      rsWrap.getDataHitCount())
              .addCounter(Interns.info(BLOCK_CACHE_LEAF_INDEX_HIT_COUNT, ""),
                      rsWrap.getLeafIndexHitCount())
              .addCounter(Interns.info(BLOCK_CACHE_BLOOM_CHUNK_HIT_COUNT, ""),
                      rsWrap.getBloomChunkHitCount())
              .addCounter(Interns.info(BLOCK_CACHE_META_HIT_COUNT, ""),
                      rsWrap.getMetaHitCount())
              .addCounter(Interns.info(BLOCK_CACHE_ROOT_INDEX_HIT_COUNT, ""),
                      rsWrap.getRootIndexHitCount())
              .addCounter(Interns.info(BLOCK_CACHE_INTERMEDIATE_INDEX_HIT_COUNT, ""),
                      rsWrap.getIntermediateIndexHitCount())
              .addCounter(Interns.info(BLOCK_CACHE_FILE_INFO_HIT_COUNT, ""),
                      rsWrap.getFileInfoHitCount())
              .addCounter(Interns.info(BLOCK_CACHE_GENERAL_BLOOM_META_HIT_COUNT, ""),
                      rsWrap.getGeneralBloomMetaHitCount())
              .addCounter(Interns.info(BLOCK_CACHE_DELETE_FAMILY_BLOOM_HIT_COUNT, ""),
                      rsWrap.getDeleteFamilyBloomHitCount())
              .addCounter(Interns.info(BLOCK_CACHE_TRAILER_HIT_COUNT, ""),
                      rsWrap.getTrailerHitCount())
              .addCounter(Interns.info(UPDATES_BLOCKED_TIME, UPDATES_BLOCKED_DESC),
                      rsWrap.getUpdatesBlockedTime())
              .addCounter(Interns.info(FLUSHED_CELLS, FLUSHED_CELLS_DESC),
                      rsWrap.getFlushedCellsCount())
              .addCounter(Interns.info(COMPACTED_CELLS, COMPACTED_CELLS_DESC),
                      rsWrap.getCompactedCellsCount())
              .addCounter(Interns.info(MAJOR_COMPACTED_CELLS, MAJOR_COMPACTED_CELLS_DESC),
                      rsWrap.getMajorCompactedCellsCount())
              .addCounter(Interns.info(FLUSHED_CELLS_SIZE, FLUSHED_CELLS_SIZE_DESC),
                      rsWrap.getFlushedCellsSize())
              .addCounter(Interns.info(COMPACTED_CELLS_SIZE, COMPACTED_CELLS_SIZE_DESC),
                      rsWrap.getCompactedCellsSize())
              .addCounter(Interns.info(MAJOR_COMPACTED_CELLS_SIZE, MAJOR_COMPACTED_CELLS_SIZE_DESC),
                      rsWrap.getMajorCompactedCellsSize())
              .addCounter(Interns.info(CELLS_COUNT_COMPACTED_FROM_MOB,
                      CELLS_COUNT_COMPACTED_FROM_MOB_DESC), rsWrap.getCellsCountCompactedFromMob())
              .addCounter(Interns.info(CELLS_COUNT_COMPACTED_TO_MOB,
                      CELLS_COUNT_COMPACTED_TO_MOB_DESC), rsWrap.getCellsCountCompactedToMob())
              .addCounter(Interns.info(CELLS_SIZE_COMPACTED_FROM_MOB,
                      CELLS_SIZE_COMPACTED_FROM_MOB_DESC), rsWrap.getCellsSizeCompactedFromMob())
              .addCounter(Interns.info(CELLS_SIZE_COMPACTED_TO_MOB,
                      CELLS_SIZE_COMPACTED_TO_MOB_DESC), rsWrap.getCellsSizeCompactedToMob())
              .addCounter(Interns.info(MOB_FLUSH_COUNT, MOB_FLUSH_COUNT_DESC),
                      rsWrap.getMobFlushCount())
              .addCounter(Interns.info(MOB_FLUSHED_CELLS_COUNT, MOB_FLUSHED_CELLS_COUNT_DESC),
                      rsWrap.getMobFlushedCellsCount())
              .addCounter(Interns.info(MOB_FLUSHED_CELLS_SIZE, MOB_FLUSHED_CELLS_SIZE_DESC),
                      rsWrap.getMobFlushedCellsSize())
              .addCounter(Interns.info(MOB_SCAN_CELLS_COUNT, MOB_SCAN_CELLS_COUNT_DESC),
                      rsWrap.getMobScanCellsCount())
              .addCounter(Interns.info(MOB_SCAN_CELLS_SIZE, MOB_SCAN_CELLS_SIZE_DESC),
                      rsWrap.getMobScanCellsSize())
              .addCounter(Interns.info(MOB_FILE_CACHE_ACCESS_COUNT,
                      MOB_FILE_CACHE_ACCESS_COUNT_DESC), rsWrap.getMobFileCacheAccessCount())
              .addCounter(Interns.info(MOB_FILE_CACHE_MISS_COUNT, MOB_FILE_CACHE_MISS_COUNT_DESC),
                      rsWrap.getMobFileCacheMissCount())
              .addCounter(Interns.info(MOB_FILE_CACHE_EVICTED_COUNT,
                      MOB_FILE_CACHE_EVICTED_COUNT_DESC), rsWrap.getMobFileCacheEvictedCount())
              .addCounter(Interns.info(HEDGED_READS, HEDGED_READS_DESC), rsWrap.getHedgedReadOps())
              .addCounter(Interns.info(HEDGED_READ_WINS, HEDGED_READ_WINS_DESC),
                      rsWrap.getHedgedReadWins())
              .addCounter(Interns.info(BLOCKED_REQUESTS_COUNT, BLOCKED_REQUESTS_COUNT_DESC),
                      rsWrap.getBlockedRequestsCount())
              .tag(Interns.info(ZOOKEEPER_QUORUM_NAME, ZOOKEEPER_QUORUM_DESC),
                      rsWrap.getZookeeperQuorum())
              .tag(Interns.info(SERVER_NAME_NAME, SERVER_NAME_DESC), rsWrap.getServerName())
              .tag(Interns.info(CLUSTER_ID_NAME, CLUSTER_ID_DESC), rsWrap.getClusterId());

      if (!skipTableMetrics) {
        // See MetricsTableSourceImpl.snapshot for the list, and the TO-DO above
        mrb.addCounter(Interns.info(TOTAL_REQUEST_COUNT, TOTAL_REQUEST_COUNT_DESC),
            rsWrap.getTotalRequestCount())
          .addCounter(Interns.info(READ_REQUEST_COUNT, READ_REQUEST_COUNT_DESC),
            rsWrap.getReadRequestsCount())
          .addCounter(Interns.info(CP_REQUEST_COUNT, CP_REQUEST_COUNT_DESC),
            rsWrap.getCpRequestsCount())
          .addCounter(Interns.info(FILTERED_READ_REQUEST_COUNT,
            FILTERED_READ_REQUEST_COUNT_DESC), rsWrap.getFilteredReadRequestsCount())
          .addCounter(Interns.info(WRITE_REQUEST_COUNT, WRITE_REQUEST_COUNT_DESC),
            rsWrap.getWriteRequestsCount());
      }
    }

    metricsRegistry.snapshot(mrb, all);

    // source is registered in supers constructor, sometimes called before the whole initialization.
    if (metricsAdapter != null) {
      // snapshot MetricRegistry as well
      metricsAdapter.snapshotAllMetrics(registry, mrb);
    }
  }

  private MetricsRecordBuilder addGaugesToMetricsRecordBuilder(
       MetricsRecordBuilder mrb, boolean skipTableMetrics) {
    mrb = mrb.addGauge(Interns.info(WALFILE_COUNT, WALFILE_COUNT_DESC), rsWrap.getNumWALFiles())
            .addGauge(Interns.info(WALFILE_SIZE, WALFILE_SIZE_DESC), rsWrap.getWALFileSize())
            .addGauge(Interns.info(STOREFILE_SIZE_GROWTH_RATE, STOREFILE_SIZE_GROWTH_RATE_DESC),
              rsWrap.getStoreFileSizeGrowthRate())
            .addGauge(Interns.info(RS_START_TIME_NAME, RS_START_TIME_DESC), rsWrap.getStartCode())
            .addGauge(Interns.info(STOREFILE_INDEX_SIZE, STOREFILE_INDEX_SIZE_DESC),
                    rsWrap.getStoreFileIndexSize())
            .addGauge(Interns.info(STATIC_INDEX_SIZE, STATIC_INDEX_SIZE_DESC),
                    rsWrap.getTotalStaticIndexSize())
            .addGauge(Interns.info(STATIC_BLOOM_SIZE, STATIC_BLOOM_SIZE_DESC),
                    rsWrap.getTotalStaticBloomSize())
            .addGauge(Interns.info(NUMBER_OF_MUTATIONS_WITHOUT_WAL,
                    NUMBER_OF_MUTATIONS_WITHOUT_WAL_DESC), rsWrap.getNumMutationsWithoutWAL())
            .addGauge(Interns.info(DATA_SIZE_WITHOUT_WAL, DATA_SIZE_WITHOUT_WAL_DESC),
                    rsWrap.getDataInMemoryWithoutWAL())
            .addGauge(Interns.info(PERCENT_FILES_LOCAL, PERCENT_FILES_LOCAL_DESC),
                    rsWrap.getPercentFileLocal())
            .addGauge(Interns.info(PERCENT_FILES_LOCAL_SECONDARY_REGIONS,
                    PERCENT_FILES_LOCAL_SECONDARY_REGIONS_DESC),
                    rsWrap.getPercentFileLocalSecondaryRegions())
            .addGauge(Interns.info(SPLIT_QUEUE_LENGTH, SPLIT_QUEUE_LENGTH_DESC),
                    rsWrap.getSplitQueueSize())
            .addGauge(Interns.info(COMPACTION_QUEUE_LENGTH, COMPACTION_QUEUE_LENGTH_DESC),
                    rsWrap.getCompactionQueueSize())
            .addGauge(Interns.info(SMALL_COMPACTION_QUEUE_LENGTH,
                    SMALL_COMPACTION_QUEUE_LENGTH_DESC), rsWrap.getSmallCompactionQueueSize())
            .addGauge(Interns.info(LARGE_COMPACTION_QUEUE_LENGTH,
                    LARGE_COMPACTION_QUEUE_LENGTH_DESC), rsWrap.getLargeCompactionQueueSize())
            .addGauge(Interns.info(FLUSH_QUEUE_LENGTH, FLUSH_QUEUE_LENGTH_DESC),
                    rsWrap.getFlushQueueSize())
            .addGauge(Interns.info(BLOCK_CACHE_FREE_SIZE, BLOCK_CACHE_FREE_DESC),
                    rsWrap.getBlockCacheFreeSize())
            .addGauge(Interns.info(BLOCK_CACHE_COUNT, BLOCK_CACHE_COUNT_DESC),
                    rsWrap.getBlockCacheCount())
            .addGauge(Interns.info(BLOCK_CACHE_SIZE, BLOCK_CACHE_SIZE_DESC),
                    rsWrap.getBlockCacheSize())
            .addGauge(Interns.info(BLOCK_CACHE_HIT_PERCENT, BLOCK_CACHE_HIT_PERCENT_DESC),
                    rsWrap.getBlockCacheHitPercent())
            .addGauge(Interns.info(BLOCK_CACHE_EXPRESS_HIT_PERCENT,
                    BLOCK_CACHE_EXPRESS_HIT_PERCENT_DESC), rsWrap.getBlockCacheHitCachingPercent())
            .addGauge(Interns.info(L1_CACHE_HIT_COUNT, L1_CACHE_HIT_COUNT_DESC),
                    rsWrap.getL1CacheHitCount())
            .addGauge(Interns.info(L1_CACHE_MISS_COUNT, L1_CACHE_MISS_COUNT_DESC),
                    rsWrap.getL1CacheMissCount())
            .addGauge(Interns.info(L1_CACHE_HIT_RATIO, L1_CACHE_HIT_RATIO_DESC),
                    rsWrap.getL1CacheHitRatio())
            .addGauge(Interns.info(L1_CACHE_MISS_RATIO, L1_CACHE_MISS_RATIO_DESC),
                    rsWrap.getL1CacheMissRatio())
            .addGauge(Interns.info(L2_CACHE_HIT_COUNT, L2_CACHE_HIT_COUNT_DESC),
                    rsWrap.getL2CacheHitCount())
            .addGauge(Interns.info(L2_CACHE_MISS_COUNT, L2_CACHE_MISS_COUNT_DESC),
                    rsWrap.getL2CacheMissCount())
            .addGauge(Interns.info(L2_CACHE_HIT_RATIO, L2_CACHE_HIT_RATIO_DESC),
                    rsWrap.getL2CacheHitRatio())
            .addGauge(Interns.info(L2_CACHE_MISS_RATIO, L2_CACHE_MISS_RATIO_DESC),
                    rsWrap.getL2CacheMissRatio())
            .addGauge(Interns.info(MOB_FILE_CACHE_COUNT, MOB_FILE_CACHE_COUNT_DESC),
                    rsWrap.getMobFileCacheCount())
            .addGauge(Interns.info(MOB_FILE_CACHE_HIT_PERCENT, MOB_FILE_CACHE_HIT_PERCENT_DESC),
                    rsWrap.getMobFileCacheHitPercent())
            .addGauge(Interns.info(READ_REQUEST_RATE_PER_SECOND, READ_REQUEST_RATE_DESC),
                    rsWrap.getReadRequestsRatePerSecond())
            .addGauge(Interns.info(WRITE_REQUEST_RATE_PER_SECOND, WRITE_REQUEST_RATE_DESC),
                    rsWrap.getWriteRequestsRatePerSecond());
    // See MetricsTableSourceImpl.snapshot for the list, and the TO-DO in the caller
    if (!skipTableMetrics) {
      mrb.addGauge(Interns.info(REGION_COUNT, REGION_COUNT_DESC), rsWrap.getNumOnlineRegions())
        .addGauge(Interns.info(STORE_COUNT, STORE_COUNT_DESC), rsWrap.getNumStores())
        .addGauge(Interns.info(STOREFILE_COUNT, STOREFILE_COUNT_DESC), rsWrap.getNumStoreFiles())
        .addGauge(Interns.info(MEMSTORE_SIZE, MEMSTORE_SIZE_DESC), rsWrap.getMemStoreSize())
        .addGauge(Interns.info(STOREFILE_SIZE, STOREFILE_SIZE_DESC), rsWrap.getStoreFileSize())
        .addGauge(Interns.info(MAX_STORE_FILE_AGE, MAX_STORE_FILE_AGE_DESC),
          rsWrap.getMaxStoreFileAge())
        .addGauge(Interns.info(MIN_STORE_FILE_AGE, MIN_STORE_FILE_AGE_DESC),
          rsWrap.getMinStoreFileAge())
        .addGauge(Interns.info(AVG_STORE_FILE_AGE, AVG_STORE_FILE_AGE_DESC),
          rsWrap.getAvgStoreFileAge())
        .addGauge(Interns.info(AVERAGE_REGION_SIZE, AVERAGE_REGION_SIZE_DESC),
          rsWrap.getAverageRegionSize())
        .addGauge(Interns.info(NUM_REFERENCE_FILES, NUM_REFERENCE_FILES_DESC),
          rsWrap.getNumReferenceFiles());
    }
    return mrb;
  }

  @Override
  public void incInfoThresholdExceeded(int count) {
    infoPauseThresholdExceeded.incr(count);
  }

  @Override
  public void incWarnThresholdExceeded(int count) {
    warnPauseThresholdExceeded.incr(count);
  }

  @Override
  public void updatePauseTimeWithGc(long t) {
    pausesWithGc.add(t);
  }

  @Override
  public void updatePauseTimeWithoutGc(long t) {
    pausesWithoutGc.add(t);
  }

  @Override
  public void updateDeleteBatch(long t) {
    deleteBatchHisto.add(t);
  }

  @Override
  public void updateCheckAndDelete(long t) {
    checkAndDeleteHisto.add(t);
  }

  @Override
  public void updateCheckAndPut(long t) {
    checkAndPutHisto.add(t);
  }

  @Override
  public void updatePutBatch(long t) {
    putBatchHisto.add(t);
  }
}
