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
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hadoop2 implementation of MetricsRegionServerSource.
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern
 */
@InterfaceAudience.Private
public class MetricsRegionServerSourceImpl
    extends BaseSourceImpl implements MetricsRegionServerSource {

  final MetricsRegionServerWrapper rsWrap;
  private final MetricHistogram putHisto;
  private final MetricHistogram putBatchHisto;
  private final MetricHistogram deleteHisto;
  private final MetricHistogram deleteBatchHisto;
  private final MetricHistogram checkAndDeleteHisto;
  private final MetricHistogram checkAndPutHisto;
  private final MetricHistogram checkAndMutateHisto;
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

    putHisto = getMetricsRegistry().newTimeHistogram(PUT_KEY);
    putBatchHisto = getMetricsRegistry().newTimeHistogram(PUT_BATCH_KEY);
    slowPut = getMetricsRegistry().newCounter(SLOW_PUT_KEY, SLOW_PUT_DESC, 0L);

    deleteHisto = getMetricsRegistry().newTimeHistogram(DELETE_KEY);
    slowDelete = getMetricsRegistry().newCounter(SLOW_DELETE_KEY, SLOW_DELETE_DESC, 0L);

    deleteBatchHisto = getMetricsRegistry().newTimeHistogram(DELETE_BATCH_KEY);
    checkAndDeleteHisto = getMetricsRegistry().newTimeHistogram(CHECK_AND_DELETE_KEY);
    checkAndPutHisto = getMetricsRegistry().newTimeHistogram(CHECK_AND_PUT_KEY);
    checkAndMutateHisto = getMetricsRegistry().newTimeHistogram(CHECK_AND_MUTATE_KEY);

    getHisto = getMetricsRegistry().newTimeHistogram(GET_KEY);
    slowGet = getMetricsRegistry().newCounter(SLOW_GET_KEY, SLOW_GET_DESC, 0L);

    incrementHisto = getMetricsRegistry().newTimeHistogram(INCREMENT_KEY);
    slowIncrement = getMetricsRegistry().newCounter(SLOW_INCREMENT_KEY, SLOW_INCREMENT_DESC, 0L);

    appendHisto = getMetricsRegistry().newTimeHistogram(APPEND_KEY);
    slowAppend = getMetricsRegistry().newCounter(SLOW_APPEND_KEY, SLOW_APPEND_DESC, 0L);

    replayHisto = getMetricsRegistry().newTimeHistogram(REPLAY_KEY);
    scanSizeHisto = getMetricsRegistry().newSizeHistogram(SCAN_SIZE_KEY);
    scanTimeHisto = getMetricsRegistry().newTimeHistogram(SCAN_TIME_KEY);

    flushTimeHisto = getMetricsRegistry().newTimeHistogram(FLUSH_TIME, FLUSH_TIME_DESC);
    flushMemstoreSizeHisto = getMetricsRegistry()
        .newSizeHistogram(FLUSH_MEMSTORE_SIZE, FLUSH_MEMSTORE_SIZE_DESC);
    flushOutputSizeHisto = getMetricsRegistry().newSizeHistogram(FLUSH_OUTPUT_SIZE,
      FLUSH_OUTPUT_SIZE_DESC);
    flushedOutputBytes = getMetricsRegistry().newCounter(FLUSHED_OUTPUT_BYTES,
      FLUSHED_OUTPUT_BYTES_DESC, 0L);
    flushedMemstoreBytes = getMetricsRegistry().newCounter(FLUSHED_MEMSTORE_BYTES,
      FLUSHED_MEMSTORE_BYTES_DESC, 0L);

    compactionTimeHisto = getMetricsRegistry()
        .newTimeHistogram(COMPACTION_TIME, COMPACTION_TIME_DESC);
    compactionInputFileCountHisto = getMetricsRegistry()
      .newHistogram(COMPACTION_INPUT_FILE_COUNT, COMPACTION_INPUT_FILE_COUNT_DESC);
    compactionInputSizeHisto = getMetricsRegistry()
        .newSizeHistogram(COMPACTION_INPUT_SIZE, COMPACTION_INPUT_SIZE_DESC);
    compactionOutputFileCountHisto = getMetricsRegistry()
        .newHistogram(COMPACTION_OUTPUT_FILE_COUNT, COMPACTION_OUTPUT_FILE_COUNT_DESC);
    compactionOutputSizeHisto = getMetricsRegistry()
      .newSizeHistogram(COMPACTION_OUTPUT_SIZE, COMPACTION_OUTPUT_SIZE_DESC);
    compactedInputBytes = getMetricsRegistry()
        .newCounter(COMPACTED_INPUT_BYTES, COMPACTED_INPUT_BYTES_DESC, 0L);
    compactedOutputBytes = getMetricsRegistry()
        .newCounter(COMPACTED_OUTPUT_BYTES, COMPACTED_OUTPUT_BYTES_DESC, 0L);

    majorCompactionTimeHisto = getMetricsRegistry()
        .newTimeHistogram(MAJOR_COMPACTION_TIME, MAJOR_COMPACTION_TIME_DESC);
    majorCompactionInputFileCountHisto = getMetricsRegistry()
      .newHistogram(MAJOR_COMPACTION_INPUT_FILE_COUNT, MAJOR_COMPACTION_INPUT_FILE_COUNT_DESC);
    majorCompactionInputSizeHisto = getMetricsRegistry()
        .newSizeHistogram(MAJOR_COMPACTION_INPUT_SIZE, MAJOR_COMPACTION_INPUT_SIZE_DESC);
    majorCompactionOutputFileCountHisto = getMetricsRegistry()
        .newHistogram(MAJOR_COMPACTION_OUTPUT_FILE_COUNT, MAJOR_COMPACTION_OUTPUT_FILE_COUNT_DESC);
    majorCompactionOutputSizeHisto = getMetricsRegistry()
      .newSizeHistogram(MAJOR_COMPACTION_OUTPUT_SIZE, MAJOR_COMPACTION_OUTPUT_SIZE_DESC);
    majorCompactedInputBytes = getMetricsRegistry()
        .newCounter(MAJOR_COMPACTED_INPUT_BYTES, MAJOR_COMPACTED_INPUT_BYTES_DESC, 0L);
    majorCompactedOutputBytes = getMetricsRegistry()
        .newCounter(MAJOR_COMPACTED_OUTPUT_BYTES, MAJOR_COMPACTED_OUTPUT_BYTES_DESC, 0L);

    splitTimeHisto = getMetricsRegistry().newTimeHistogram(SPLIT_KEY);
    splitRequest = getMetricsRegistry().newCounter(SPLIT_REQUEST_KEY, SPLIT_REQUEST_DESC, 0L);
    splitSuccess = getMetricsRegistry().newCounter(SPLIT_SUCCESS_KEY, SPLIT_SUCCESS_DESC, 0L);

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
      addGaugesToMetricsRecordBuilder(mrb)
              .addCounter(Interns.info(TOTAL_REQUEST_COUNT, TOTAL_REQUEST_COUNT_DESC),
                      rsWrap.getTotalRequestCount())
              .addCounter(Interns.info(TOTAL_ROW_ACTION_REQUEST_COUNT,
                      TOTAL_ROW_ACTION_REQUEST_COUNT_DESC), rsWrap.getTotalRowActionRequestCount())
              .addCounter(Interns.info(READ_REQUEST_COUNT, READ_REQUEST_COUNT_DESC),
                      rsWrap.getReadRequestsCount())
              .addCounter(Interns.info(FILTERED_READ_REQUEST_COUNT,
                      FILTERED_READ_REQUEST_COUNT_DESC), rsWrap.getFilteredReadRequestsCount())
              .addCounter(Interns.info(WRITE_REQUEST_COUNT, WRITE_REQUEST_COUNT_DESC),
                      rsWrap.getWriteRequestsCount())
              .addCounter(Interns.info(RPC_GET_REQUEST_COUNT, RPC_GET_REQUEST_COUNT_DESC),
                      rsWrap.getRpcGetRequestsCount())
              .addCounter(Interns.info(RPC_FULL_SCAN_REQUEST_COUNT, RPC_FULL_SCAN_REQUEST_COUNT_DESC),
                      rsWrap.getRpcFullScanRequestsCount())
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
              .addCounter(Interns.info(HEDGED_READ_IN_CUR_THREAD, HEDGED_READ_IN_CUR_THREAD_DESC),
                      rsWrap.getHedgedReadOpsInCurThread())
              .addCounter(Interns.info(BLOCKED_REQUESTS_COUNT, BLOCKED_REQUESTS_COUNT_DESC),
                      rsWrap.getBlockedRequestsCount())
              .tag(Interns.info(ZOOKEEPER_QUORUM_NAME, ZOOKEEPER_QUORUM_DESC),
                      rsWrap.getZookeeperQuorum())
              .tag(Interns.info(SERVER_NAME_NAME, SERVER_NAME_DESC), rsWrap.getServerName())
              .tag(Interns.info(CLUSTER_ID_NAME, CLUSTER_ID_DESC), rsWrap.getClusterId());
    }

    metricsRegistry.snapshot(mrb, all);

    // source is registered in supers constructor, sometimes called before the whole initialization.
    if (metricsAdapter != null) {
      // snapshot MetricRegistry as well
      metricsAdapter.snapshotAllMetrics(registry, mrb);
    }
  }

  private MetricsRecordBuilder addGaugesToMetricsRecordBuilder(MetricsRecordBuilder mrb) {
    return mrb.addGauge(Interns.info(REGION_COUNT, REGION_COUNT_DESC), rsWrap.getNumOnlineRegions())
            .addGauge(Interns.info(STORE_COUNT, STORE_COUNT_DESC), rsWrap.getNumStores())
            .addGauge(Interns.info(WALFILE_COUNT, WALFILE_COUNT_DESC), rsWrap.getNumWALFiles())
            .addGauge(Interns.info(WALFILE_SIZE, WALFILE_SIZE_DESC), rsWrap.getWALFileSize())
            .addGauge(Interns.info(STOREFILE_COUNT, STOREFILE_COUNT_DESC),
                    rsWrap.getNumStoreFiles())
            .addGauge(Interns.info(MEMSTORE_SIZE, MEMSTORE_SIZE_DESC), rsWrap.getMemStoreSize())
            .addGauge(Interns.info(STOREFILE_SIZE, STOREFILE_SIZE_DESC), rsWrap.getStoreFileSize())
            .addGauge(Interns.info(MAX_STORE_FILE_AGE, MAX_STORE_FILE_AGE_DESC),
                    rsWrap.getMaxStoreFileAge())
            .addGauge(Interns.info(MIN_STORE_FILE_AGE, MIN_STORE_FILE_AGE_DESC),
                    rsWrap.getMinStoreFileAge())
            .addGauge(Interns.info(AVG_STORE_FILE_AGE, AVG_STORE_FILE_AGE_DESC),
                    rsWrap.getAvgStoreFileAge())
            .addGauge(Interns.info(NUM_REFERENCE_FILES, NUM_REFERENCE_FILES_DESC),
                    rsWrap.getNumReferenceFiles())
            .addGauge(Interns.info(RS_START_TIME_NAME, RS_START_TIME_DESC), rsWrap.getStartCode())
            .addGauge(Interns.info(AVERAGE_REGION_SIZE, AVERAGE_REGION_SIZE_DESC),
                    rsWrap.getAverageRegionSize())
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
            .addGauge(Interns.info(TOTAL_BYTES_READ,
                    TOTAL_BYTES_READ_DESC),
                    rsWrap.getTotalBytesRead())
            .addGauge(Interns.info(LOCAL_BYTES_READ,
                    LOCAL_BYTES_READ_DESC),
                    rsWrap.getLocalBytesRead())
            .addGauge(Interns.info(SHORTCIRCUIT_BYTES_READ,
                    SHORTCIRCUIT_BYTES_READ_DESC),
                    rsWrap.getShortCircuitBytesRead())
            .addGauge(Interns.info(ZEROCOPY_BYTES_READ,
                    ZEROCOPY_BYTES_READ_DESC),
                    rsWrap.getZeroCopyBytesRead())
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
                    rsWrap.getWriteRequestsRatePerSecond())
            .addGauge(Interns.info(BYTE_BUFF_ALLOCATOR_HEAP_ALLOCATION_BYTES,
                  BYTE_BUFF_ALLOCATOR_HEAP_ALLOCATION_BYTES_DESC),
                rsWrap.getByteBuffAllocatorHeapAllocationBytes())
            .addGauge(Interns.info(BYTE_BUFF_ALLOCATOR_POOL_ALLOCATION_BYTES,
                  BYTE_BUFF_ALLOCATOR_POOL_ALLOCATION_BYTES_DESC),
                rsWrap.getByteBuffAllocatorPoolAllocationBytes())
            .addGauge(Interns.info(BYTE_BUFF_ALLOCATOR_HEAP_ALLOCATION_RATIO,
                  BYTE_BUFF_ALLOCATOR_HEAP_ALLOCATION_RATIO_DESC),
                rsWrap.getByteBuffAllocatorHeapAllocRatio())
            .addGauge(Interns.info(BYTE_BUFF_ALLOCATOR_TOTAL_BUFFER_COUNT,
                BYTE_BUFF_ALLOCATOR_TOTAL_BUFFER_COUNT_DESC),
                rsWrap.getByteBuffAllocatorTotalBufferCount())
            .addGauge(Interns.info(BYTE_BUFF_ALLOCATOR_USED_BUFFER_COUNT,
                BYTE_BUFF_ALLOCATOR_USED_BUFFER_COUNT_DESC),
                rsWrap.getByteBuffAllocatorUsedBufferCount());
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
  public void updateCheckAndMutate(long t) {
    checkAndMutateHisto.add(t);
  }

  @Override
  public void updatePutBatch(long t) {
    putBatchHisto.add(t);
  }
}
