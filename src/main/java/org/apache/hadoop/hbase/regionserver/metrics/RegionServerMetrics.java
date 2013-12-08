/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.metrics.HBaseInfo;
import org.apache.hadoop.hbase.metrics.MetricsRate;
import org.apache.hadoop.hbase.metrics.PersistentMetricsTimeVaryingRate;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.List;

/**
 * This class is for maintaining the various regionserver statistics
 * and publishing them through the metrics interfaces.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values.
 */
public class RegionServerMetrics implements Updater {
  private final Log LOG = LogFactory.getLog(this.getClass());
  private final MetricsRecord metricsRecord;
  private long lastUpdate = System.currentTimeMillis();
  private long lastExtUpdate = System.currentTimeMillis();
  private long lastHistUpdate = System.currentTimeMillis();
  private long extendedPeriod = 0;
  private static final int MB = 1024*1024;
  private MetricsRegistry registry = new MetricsRegistry();
  private final RegionServerStatistics statistics;

  public final MetricsTimeVaryingRate atomicIncrementTime =
      new MetricsTimeVaryingRate("atomicIncrementTime", registry);

  /**
   * Count of regions carried by this regionserver
   */
  public final MetricsIntValue regions =
    new MetricsIntValue("regions", registry);

  /**
   * Block cache size.
   */
  public final MetricsLongValue blockCacheSize = new MetricsLongValue("blockCacheSize", registry);

  /**
   * Block cache free size.
   */
  public final MetricsLongValue blockCacheFree = new MetricsLongValue("blockCacheFree", registry);

  /**
   * Block cache item count.
   */
  public final MetricsLongValue blockCacheCount = new MetricsLongValue("blockCacheCount", registry);

  /**
   * Block cache hit count.
   */
  public final MetricsLongValue blockCacheHitCount = new MetricsLongValue("blockCacheHitCount", registry);

  /**
   * Block cache miss count.
   */
  public final MetricsLongValue blockCacheMissCount = new MetricsLongValue("blockCacheMissCount", registry);

  /**
   * Block cache evict count.
   */
  public final MetricsLongValue blockCacheEvictedCount = new MetricsLongValue("blockCacheEvictedCount", registry);
  public final MetricsLongValue blockCacheEvictedSingleCount = new MetricsLongValue("blockCacheEvictedSingleCount", registry);
  public final MetricsLongValue blockCacheEvictedMultiCount = new MetricsLongValue("blockCacheEvictedMultiCount", registry);
  public final MetricsLongValue blockCacheEvictedMemoryCount = new MetricsLongValue("blockCacheEvictedMemoryCount", registry);

  /**
   * Block hit ratio.
   */
  public final MetricsIntValue blockCacheHitRatio =
      new MetricsIntValue("blockCacheHitRatio", registry);

  /**
   * L2 cache size
   */
  public final MetricsLongValue l2CacheSize =
      new MetricsLongValue("l2CacheSize", registry);

  /**
   * L2 cache free size
   */
  public final MetricsLongValue l2CacheFree =
      new MetricsLongValue("l2CacheFree", registry);

  /**
   * L2 cache item count
   */
  public final MetricsLongValue l2CacheCount =
      new MetricsLongValue("l2CacheCount", registry);

  /**
   * L2 cache hit count
   */
  public final MetricsLongValue l2CacheHitCount =
      new MetricsLongValue("l2CacheHitCount", registry);

  /**
   * L2 cache miss count
   */
  public final MetricsLongValue l2CacheMissCount =
      new MetricsLongValue("l2CacheMissCount", registry);

  /**
   * L2 evicted item count
   */
  public final MetricsLongValue l2CacheEvictedCount =
      new MetricsLongValue("l2CacheEvictedCount", registry);

  /**
   * L2 cache hit ratio
   */
  public final MetricsIntValue l2CacheHitRatio =
      new MetricsIntValue("l2CacheHitRatio", registry);

  /*
   * Count of rows read or updated to the regionservers since last call to metrics update
   */
  public final MetricsRate requests = new MetricsRate("requests", registry);
  
  /*
   * Count of rows read from the regionservers since last call to metrics update
   */
  public final MetricsRate rowReadCnt = new MetricsRate("rowReadCnt", registry);

  /*
   * Count of row updated to the regionservers since last call to metrics update
   */
  public final MetricsRate rowUpdatedCnt =  new MetricsRate("rowUpdatedCnt", registry);
  
  /**
   * Count of stores open on the regionserver.
   */
  public final MetricsIntValue stores = new MetricsIntValue("stores", registry);

  /**
   * Count of storefiles open on the regionserver.
   */
  public final MetricsIntValue storefiles = new MetricsIntValue("storefiles", registry);

  /**
   * Sum of all the storefile index sizes in this regionserver in MB. This is
   * a legacy metric to be phased out as we fully transition to multi-level
   * block indexes.
   */
  public final MetricsIntValue storefileIndexSizeMB =
    new MetricsIntValue("storefileIndexSizeMB", registry);

  /** The total size of block index root levels in this regionserver in KB. */
  public final MetricsIntValue rootIndexSizeKB =
    new MetricsIntValue("rootIndexSizeKB", registry);

  /** Total size of all block indexes (not necessarily loaded in memory) */
  public final MetricsIntValue totalStaticIndexSizeKB =
    new MetricsIntValue("totalStaticIndexSizeKB", registry);

  /** Total size of all Bloom filters (not necessarily loaded in memory) */
  public final MetricsIntValue totalStaticBloomSizeKB =
    new MetricsIntValue("totalStaticBloomSizeKB", registry);

  /** Total number of the bloom filter block load failure cnt */
  public final MetricsLongValue totalCompoundBloomFilterLoadFailureCnt =
    new MetricsLongValue("totalCompoundBloomFilterLoadFailureCnt", registry);

  /**
   * Sum of all the memstore sizes in this regionserver in MB
   */
  public final MetricsIntValue memstoreSizeMB =
    new MetricsIntValue("memstoreSizeMB", registry);

  /**
   * Size of the compaction queue.
   */
  public final MetricsIntValue compactionQueueSize =
    new MetricsIntValue("compactionQueueSize", registry);

  /**
   * filesystem read latency for positional read operations
   */
  public final MetricsTimeVaryingRate fsReadLatency =
      new MetricsTimeVaryingRate("fsReadLatency", registry);

  /**
   * number of blocks cached during compaction
   */
  public final MetricsIntValue blocksCachedDuringCompaction =
      new MetricsIntValue("blocksCachedDuringCompaction", registry);

  /**
   * filesystem p99 read latency outlier for positional read operations
   */
  public final PercentileMetric fsReadLatencyP99;

  /**
   * filesystem p99 read latency outlier for positional read operations during
   * compactions
   */
  public final PercentileMetric fsCompactionReadLatencyP99;

  /**
   * filesystem read latency for positional read operations during
   * compactions
   */
  public final MetricsTimeVaryingRate fsCompactionReadLatency =
      new MetricsTimeVaryingRate("fsCompactionReadLatency", registry);

  /**
   * filesystem write latency
   */
  public final MetricsTimeVaryingRate fsWriteLatency =
    new MetricsTimeVaryingRate("fsWriteLatency", registry);

  /**
   * size (in bytes) of data in HLog append calls
   */
  public final MetricsTimeVaryingRate fsWriteSize =
    new MetricsTimeVaryingRate("fsWriteSize", registry);

  /**
   * filesystem sync latency
   */
  public final MetricsTimeVaryingRate fsSyncLatency =
    new MetricsTimeVaryingRate("fsSyncLatency", registry);

  /**
   * filesystem group sync latency
   */
  public final MetricsTimeVaryingRate fsGroupSyncLatency =
    new MetricsTimeVaryingRate("fsGroupSyncLatency", registry);

  /**
   * Memstore Insert time (in ms).
   */
  public final MetricsTimeVaryingRate memstoreInsertTime =
    new MetricsTimeVaryingRate("memstoreInsert", registry);

  public final MetricsTimeVaryingRate rowLockTime =
    new MetricsTimeVaryingRate("rowLock", registry);

  public final MetricsTimeVaryingRate mvccWaitTime =
    new MetricsTimeVaryingRate("mvccWait", registry);

  public final MetricsRate numOptimizedSeeks =
    new MetricsRate("numOptimizedSeeks", registry);

  /**
   * time each scheduled compaction takes
   */
  protected final PersistentMetricsTimeVaryingRate compactionTime =
    new PersistentMetricsTimeVaryingRate("compactionTime", registry);

  protected final PersistentMetricsTimeVaryingRate compactionSize =
    new PersistentMetricsTimeVaryingRate("compactionSize", registry);

  /**
   * time each scheduled flush takes
   */
  protected final PersistentMetricsTimeVaryingRate flushTime =
    new PersistentMetricsTimeVaryingRate("flushTime", registry);

  protected final PersistentMetricsTimeVaryingRate flushSize =
    new PersistentMetricsTimeVaryingRate("flushSize", registry);
  
  /**
   * DFSClient metrics
   */
  public final MetricsLongValue bytesRead = 
      new MetricsLongValue("dfsBytesRead", registry);
  public final MetricsLongValue bytesLocalRead = 
      new MetricsLongValue("dfsBytesLocalRead", registry);
  public final MetricsLongValue bytesRackLocalRead = 
      new MetricsLongValue("dfsBytesRackLocalRead", registry);
  public final MetricsLongValue bytesWritten = 
      new MetricsLongValue("dfsBytesWritten", registry);
  public final MetricsLongValue filesCreated = 
      new MetricsLongValue("dfsFilesCreated", registry);
  public final MetricsLongValue filesRead = 
      new MetricsLongValue("dfsFilesRead", registry);
  public final MetricsLongValue cntWriteException = 
      new MetricsLongValue("dfsCntWriteException", registry);
  public final MetricsLongValue cntReadException = 
      new MetricsLongValue("dfsCntReadException", registry);

  // quorum read metrics
  public final MetricsLongValue quorumReadsDone =
      new MetricsLongValue("quorumReadsDone", registry);
  public final MetricsLongValue quorumReadWins =
      new MetricsLongValue("quorumReadWins", registry);
  public final MetricsLongValue quorumReadsExecutedInCurThread =
      new MetricsLongValue("quorumReadsExecutedInCurThread", registry);

  // The histogram metrics are updated every histogramMetricWindow seconds
  private long histogramMetricWindow = 240;

  public final Configuration conf;

  public RegionServerMetrics(Configuration conf) {
    histogramMetricWindow = 1000 * conf.getLong(
        HConstants.HISTOGRAM_BASED_METRICS_WINDOW,
        PercentileMetric.DEFAULT_SAMPLE_WINDOW);
    fsReadLatencyP99 = new PercentileMetric("fsReadLatencyP99", registry,
        HFile.preadHistogram, PercentileMetric.P99, conf.getInt(
            HConstants.PREAD_LATENCY_HISTOGRAM_NUM_BUCKETS,
            PercentileMetric.HISTOGRAM_NUM_BUCKETS_DEFAULT));
    fsCompactionReadLatencyP99 = new PercentileMetric(
        "fsReadCompactionLatencyP99", registry, HFile.preadCompactionHistogram,
        PercentileMetric.P99, conf.getInt(
            HConstants.PREAD_COMPACTION_LATENCY_HISTOGRAM_NUM_BUCKETS,
            PercentileMetric.HISTOGRAM_NUM_BUCKETS_DEFAULT));
    MetricsContext context = MetricsUtil.getContext("hbase");
    metricsRecord = MetricsUtil.createRecord(context, "regionserver");
    String name = Thread.currentThread().getName();
    metricsRecord.setTag("RegionServer", name);
    context.registerUpdater(this);
    // Add jvmmetrics.
    JvmMetrics.init("RegionServer", name);
    // Add Hbase Info metrics
    HBaseInfo.init();

    // export for JMX
    statistics = new RegionServerStatistics(this.registry, name);

    // get custom attributes
    try {
      Object m = ContextFactory.getFactory().getAttribute("hbase.extendedperiod");
      if (m instanceof String) {
        this.extendedPeriod = Long.parseLong((String) m)*1000;
      }
    } catch (IOException ioe) {
      LOG.info("Couldn't load ContextFactory for Metrics config info");
    }

    this.conf = conf;
    // Initializing the HistogramBasedMetrics here since
    // they will be needing conf
    LOG.info("Initialized");
  }

  public void shutdown() {
    if (statistics != null)
      statistics.shutdown();
  }

  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   * @param caller the metrics context that this responsible for calling us
   */
  public void doUpdates(MetricsContext caller) {
    synchronized (this) {
      this.lastUpdate = System.currentTimeMillis();

      // has the extended period for long-living stats elapsed?
      if (this.extendedPeriod > 0 &&
          this.lastUpdate - this.lastExtUpdate >= this.extendedPeriod) {
        this.lastExtUpdate = this.lastUpdate;
        this.resetAllMinMax();
      }

      this.stores.pushMetric(this.metricsRecord);
      this.storefiles.pushMetric(this.metricsRecord);
      this.storefileIndexSizeMB.pushMetric(this.metricsRecord);
      this.rootIndexSizeKB.pushMetric(this.metricsRecord);
      this.totalStaticIndexSizeKB.pushMetric(this.metricsRecord);
      this.totalStaticBloomSizeKB.pushMetric(this.metricsRecord);
      this.memstoreSizeMB.pushMetric(this.metricsRecord);
      this.regions.pushMetric(this.metricsRecord);
      this.requests.pushMetric(this.metricsRecord);
      this.compactionQueueSize.pushMetric(this.metricsRecord);
      this.blockCacheSize.pushMetric(this.metricsRecord);
      this.blockCacheFree.pushMetric(this.metricsRecord);
      this.blockCacheCount.pushMetric(this.metricsRecord);
      this.blockCacheHitCount.pushMetric(this.metricsRecord);
      this.blockCacheMissCount.pushMetric(this.metricsRecord);
      this.blockCacheEvictedCount.pushMetric(this.metricsRecord);
      this.blockCacheEvictedSingleCount.pushMetric(this.metricsRecord);
      this.blockCacheEvictedMultiCount.pushMetric(this.metricsRecord);
      this.blockCacheEvictedMemoryCount.pushMetric(this.metricsRecord);
      this.blockCacheHitRatio.pushMetric(this.metricsRecord);
      this.l2CacheSize.pushMetric(this.metricsRecord);
      this.l2CacheFree.pushMetric(this.metricsRecord);
      this.l2CacheCount.pushMetric(this.metricsRecord);
      this.l2CacheHitCount.pushMetric(this.metricsRecord);
      this.l2CacheMissCount.pushMetric(this.metricsRecord);
      this.l2CacheEvictedCount.pushMetric(this.metricsRecord);
      this.l2CacheHitRatio.pushMetric(this.metricsRecord);
      this.rowReadCnt.pushMetric(this.metricsRecord);
      this.rowUpdatedCnt.pushMetric(this.metricsRecord);
      this.numOptimizedSeeks.pushMetric(this.metricsRecord);

      // Be careful. Here is code for MTVR from up in hadoop:
      // public synchronized void inc(final int numOps, final long time) {
      //   currentData.numOperations += numOps;
      //   currentData.time += time;
      //   long timePerOps = time/numOps;
      //    minMax.update(timePerOps);
      // }
      // Means you can't pass a numOps of zero or get a ArithmeticException / by zero.
      // HLog metrics
      addHLogMetric(HLog.getWriteTime(), this.fsWriteLatency);
      addHLogMetric(HLog.getWriteSize(), this.fsWriteSize);
      addHLogMetric(HLog.getSyncTime(), this.fsSyncLatency);
      addHLogMetric(HLog.getGSyncTime(), this.fsGroupSyncLatency);

      // HFile metrics
      collectHFileMetric(fsReadLatency,
          HFile.getPreadOpsAndReset(), HFile.getPreadTimeMsAndReset());
      collectHFileMetric(fsCompactionReadLatency,
          HFile.getPreadCompactionOpsAndReset(),
          HFile.getPreadCompactionTimeMsAndReset());

      try {
        fsReadLatencyP99.updateMetric();
        fsCompactionReadLatencyP99.updateMetric();
      } catch (UnsupportedOperationException e) {
        LOG.error("Exception in Histogram based metric : " + e.getMessage());
      }

      /* NOTE: removed HFile write latency.  2 reasons:
       * 1) Mixing HLog latencies are far higher priority since they're
       *      on-demand and HFile is used in background (compact/flush)
       * 2) HFile metrics are being handled at a higher level
       *      by compaction & flush metrics.
       */

      int writeOps = (int)HRegion.getWriteOps();
      if (writeOps != 0) {
        this.memstoreInsertTime.inc(writeOps, HRegion.getMemstoreInsertTime());
        this.mvccWaitTime.inc(writeOps, HRegion.getMVCCWaitTime());
        this.rowLockTime.inc(writeOps, HRegion.getRowLockTime());
      }

      this.blocksCachedDuringCompaction.set(
          HFile.getBlocksCachedDuringCompactionAndReset());

      // push the result
      this.fsReadLatencyP99.pushMetric(this.metricsRecord);
      this.fsCompactionReadLatencyP99.pushMetric(this.metricsRecord);
      this.fsReadLatency.pushMetric(this.metricsRecord);
      this.fsCompactionReadLatency.pushMetric(this.metricsRecord);
      this.fsWriteLatency.pushMetric(this.metricsRecord);
      this.fsWriteSize.pushMetric(this.metricsRecord);
      this.fsSyncLatency.pushMetric(this.metricsRecord);
      this.fsGroupSyncLatency.pushMetric(this.metricsRecord);
      this.memstoreInsertTime.pushMetric(this.metricsRecord);
      this.rowLockTime.pushMetric(this.metricsRecord);
      this.mvccWaitTime.pushMetric(this.metricsRecord);
      this.compactionTime.pushMetric(this.metricsRecord);
      this.compactionSize.pushMetric(this.metricsRecord);
      this.flushTime.pushMetric(this.metricsRecord);
      this.flushSize.pushMetric(this.metricsRecord);
      
      this.bytesRead.pushMetric(this.metricsRecord);
      this.bytesLocalRead.pushMetric(this.metricsRecord);
      this.bytesRackLocalRead.pushMetric(this.metricsRecord);
      this.bytesWritten.pushMetric(this.metricsRecord);
      this.filesCreated.pushMetric(this.metricsRecord);
      this.filesRead.pushMetric(this.metricsRecord);
      this.cntWriteException.pushMetric(this.metricsRecord);
      this.cntReadException.pushMetric(this.metricsRecord);

      this.quorumReadsDone.pushMetric(this.metricsRecord);
      this.quorumReadWins.pushMetric(this.metricsRecord);
      this.quorumReadsExecutedInCurThread.pushMetric(this.metricsRecord);

      this.blocksCachedDuringCompaction.pushMetric(this.metricsRecord);

      if (this.histogramMetricWindow > 0 &&
        ((this.lastUpdate - this.lastHistUpdate) >= this.histogramMetricWindow)) {
        this.lastHistUpdate = this.lastUpdate;
        this.resetAllHistogramBasedMetrics();
      }
    }
    this.metricsRecord.update();
  }

  private void resetAllHistogramBasedMetrics() {
    this.fsReadLatencyP99.refresh();
    this.fsCompactionReadLatencyP99.refresh();
  }

  /**
   * Increment the given latency metric using the number of operations and total read time
   * obtained from HFile.
   * @param latencyMetric latency metric to increment
   * @param readOps the number of this type of read operations during the collection period
   * @param readTimeMs the amount of total read time of this type in milliseconds during the period
   */
  private static void collectHFileMetric(MetricsTimeVaryingRate latencyMetric, int readOps,
      long readTimeMs) {
    if (readOps != 0) {
      latencyMetric.inc(readOps, readTimeMs);
    }
  }

  private void addHLogMetric(HLog.Metric logMetric,
      MetricsTimeVaryingRate hadoopMetric) {
    if (logMetric.count > 0)
      hadoopMetric.inc(logMetric.min);
    if (logMetric.count > 1)
      hadoopMetric.inc(logMetric.max);
    if (logMetric.count > 2) {
      int ops = logMetric.count - 2;
      hadoopMetric.inc(ops, logMetric.total - logMetric.max - logMetric.min);
    }
  }

  public void resetAllMinMax() {
    this.compactionTime.resetMinMaxAvg();
    this.compactionSize.resetMinMaxAvg();
    this.flushTime.resetMinMaxAvg();
    this.flushSize.resetMinMaxAvg();
    this.atomicIncrementTime.resetMinMax();
    this.fsReadLatency.resetMinMax();
    this.fsCompactionReadLatency.resetMinMax();
    this.fsWriteLatency.resetMinMax();
    this.fsWriteSize.resetMinMax();
    this.fsSyncLatency.resetMinMax();
    this.fsGroupSyncLatency.resetMinMax();
    this.memstoreInsertTime.resetMinMax();
    this.rowLockTime.resetMinMax();
    this.mvccWaitTime.resetMinMax();
  }

  /**
   * @return Count of requests.
   */
  public float getRequests() {
    return this.requests.getPreviousIntervalValue();
  }

  /**
   * @param time time that compaction took
   * @param size bytesize of storefiles in the compaction
   */
  public synchronized void addCompaction(long time, long size) {
    this.compactionTime.inc(time);
    this.compactionSize.inc(size);
  }

  /**
   * @param flushes history in <time, size>
   */
  public synchronized void addFlush(final List<Pair<Long,Long>> flushes) {
    for (Pair<Long,Long> f : flushes) {
      this.flushTime.inc(f.getFirst());
      this.flushSize.inc(f.getSecond());
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    int seconds = (int)((System.currentTimeMillis() - this.lastUpdate)/1000);
    if (seconds == 0) {
      seconds = 1;
    }
    sb = Strings.appendKeyValue(sb, "request",
      Float.valueOf(this.requests.getPreviousIntervalValue()));
    sb = Strings.appendKeyValue(sb, "regions",
      Integer.valueOf(this.regions.get()));
    sb = Strings.appendKeyValue(sb, "stores",
      Integer.valueOf(this.stores.get()));
    sb = Strings.appendKeyValue(sb, "storefiles",
      Integer.valueOf(this.storefiles.get()));
    sb = Strings.appendKeyValue(sb, "storefileIndexSize",
      Integer.valueOf(this.storefileIndexSizeMB.get()));
    sb = Strings.appendKeyValue(sb, "rootIndexSizeKB",
        Integer.valueOf(this.rootIndexSizeKB.get()));
    sb = Strings.appendKeyValue(sb, "totalStaticIndexSizeKB",
        Integer.valueOf(this.totalStaticIndexSizeKB.get()));
    sb = Strings.appendKeyValue(sb, "totalStaticBloomSizeKB",
        Integer.valueOf(this.totalStaticBloomSizeKB.get()));
    sb = Strings.appendKeyValue(sb, "memstoreSize",
      Integer.valueOf(this.memstoreSizeMB.get()));
    sb = Strings.appendKeyValue(sb, "compactionQueueSize",
      Integer.valueOf(this.compactionQueueSize.get()));
    sb = Strings.appendKeyValue(sb, "numWrites",
      Float.valueOf(this.rowUpdatedCnt.getPreviousIntervalValue()));
    sb = Strings.appendKeyValue(sb, "numReads",
      Float.valueOf(this.rowReadCnt.getPreviousIntervalValue()));
    sb = Strings.appendKeyValue(sb, "numOptimizedSeeks",
      Float.valueOf(this.numOptimizedSeeks.getPreviousIntervalValue()));

    // Duplicate from jvmmetrics because metrics are private there so
    // inaccessible.
    MemoryUsage memory =
      ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    sb = Strings.appendKeyValue(sb, "usedHeap",
      Long.valueOf(memory.getUsed()/MB));
    sb = Strings.appendKeyValue(sb, "maxHeap",
      Long.valueOf(memory.getMax()/MB));
    sb = Strings.appendKeyValue(sb, this.blockCacheSize.getName(),
        Long.valueOf(this.blockCacheSize.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheFree.getName(),
        Long.valueOf(this.blockCacheFree.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheCount.getName(),
        Long.valueOf(this.blockCacheCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheHitCount.getName(),
        Long.valueOf(this.blockCacheHitCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheMissCount.getName(),
        Long.valueOf(this.blockCacheMissCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheEvictedCount.getName(),
        Long.valueOf(this.blockCacheEvictedCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheEvictedSingleCount.getName(),
        Long.valueOf(this.blockCacheEvictedSingleCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheEvictedMultiCount.getName(),
        Long.valueOf(this.blockCacheEvictedMultiCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheEvictedMemoryCount.getName(),
        Long.valueOf(this.blockCacheEvictedMemoryCount.get()));
    sb = Strings.appendKeyValue(sb, this.blockCacheHitRatio.getName(),
        Long.valueOf(this.blockCacheHitRatio.get()));

    sb = Strings.appendKeyValue(sb, this.l2CacheSize.getName(),
        this.l2CacheSize.get());
    sb = Strings.appendKeyValue(sb, this.l2CacheFree.getName(),
        this.l2CacheFree.get());
    sb = Strings.appendKeyValue(sb, this.l2CacheCount.getName(),
        this.l2CacheCount.get());
    sb = Strings.appendKeyValue(sb, this.l2CacheHitCount.getName(),
        this.l2CacheHitCount.get());
    sb = Strings.appendKeyValue(sb, this.l2CacheMissCount.getName(),
        this.l2CacheMissCount.get());
    sb = Strings.appendKeyValue(sb, this.l2CacheEvictedCount.getName(),
        this.l2CacheEvictedCount.get());
    sb = Strings.appendKeyValue(sb, this.l2CacheHitRatio.getName(),
        this.l2CacheHitRatio.get());

    sb = Strings.appendKeyValue(sb, this.bytesRead.getName(),
        Long.valueOf(this.bytesRead.get()));
    sb = Strings.appendKeyValue(sb, this.bytesLocalRead.getName(),
        Long.valueOf(this.bytesLocalRead.get()));
    sb = Strings.appendKeyValue(sb, this.bytesRackLocalRead.getName(),
        Long.valueOf(this.bytesRackLocalRead.get()));
    sb = Strings.appendKeyValue(sb, this.bytesWritten.getName(),
        Long.valueOf(this.bytesWritten.get()));
    sb = Strings.appendKeyValue(sb, this.filesCreated.getName(),
        Long.valueOf(this.filesCreated.get()));
    sb = Strings.appendKeyValue(sb, this.filesRead.getName(),
        Long.valueOf(this.filesRead.get()));
    sb = Strings.appendKeyValue(sb, this.cntWriteException.getName(),
        Long.valueOf(this.cntWriteException.get()));
    sb = Strings.appendKeyValue(sb, this.cntReadException.getName(),
        Long.valueOf(this.cntReadException.get()));
    
    sb = Strings.appendKeyValue(sb, this.quorumReadsDone.getName(),
        Long.valueOf(this.quorumReadsDone.get()));
    sb = Strings.appendKeyValue(sb, this.quorumReadWins.getName(),
        Long.valueOf(this.quorumReadWins.get()));
    sb = Strings.appendKeyValue(sb, this.quorumReadsExecutedInCurThread.getName(),
        Long.valueOf(this.quorumReadsExecutedInCurThread.get()));
    return sb.toString();
  }
}
